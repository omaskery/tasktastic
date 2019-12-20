import datetime
import os
import signal
import tempfile
import uuid

import typing

from aio_pika import ExchangeType
from docker.errors import ImageNotFound
from docker.models.containers import Container
from dataclasses import dataclass
import aio_pika
import asyncio
import docker
from requests import ReadTimeout, ConnectionError

from tasktastic.common.rmq_entities import Exchanges
from tasktastic.common.schemas import ExecutionRequestSchema, ExecutionRequest, ExecutionResponse, \
    ExecutionResponseSchema, \
    NodeHeartbeatSchema, NodeHeartbeat


@dataclass(frozen=True)
class NodeArguments:
    orchestrator_uri: str
    loop: asyncio.AbstractEventLoop
    tags: typing.Dict[str, str]


@dataclass(frozen=True)
class NodeDetails:
    node_id: uuid.UUID
    tags: typing.Dict[str, str]

    @staticmethod
    def generate_with_tags(**tags):
        return NodeDetails(
            node_id=uuid.uuid4(),
            tags=tags,
        )


async def main(args: NodeArguments) -> int:
    node_details = NodeDetails.generate_with_tags(**args.tags)

    if node_details.tags:
        print("tags:")
        for name, value in node_details.tags.items():
            print(f"  - {name} = {value}")

    docker_client = docker.from_env()
    print("checking connection to docker host...")
    docker_client.ping()
    print("confirmed connection to docker host")

    print("connecting to message broker...")
    connection: aio_pika.Connection = await aio_pika.connect_robust(
        args.orchestrator_uri,
        loop=args.loop
    )
    print("connected to message broker")

    shutdown_requested = args.loop.create_future()
    args.loop.add_signal_handler(signal.SIGINT, lambda: shutdown_requested.set_result(True))

    async with connection:
        background_tasks = [
            ('execution request receiver', asyncio.create_task(
                process_execution_requests(args.loop, connection, docker_client, node_details)
            )),
            ('heartbeat transmitter', asyncio.create_task(
                node_heartbeat(connection, node_details)
            ))
        ]

        print("running until interrupted...")
        await shutdown_requested
        print("shutdown requested!")

        print("cancelling background tasks...")
        for task_name, task in background_tasks:
            print(f" - {task_name}")
            task.cancel()
        print("awaiting background task termination...")
        for task_name, task in background_tasks:
            print(f" - {task_name}")
            try:
                await task
            except asyncio.CancelledError:
                pass

    print("exiting")
    return 0


async def node_heartbeat(connection: aio_pika.Connection, node_details: NodeDetails):
    print("establishing heartbeat channel...")
    channel = await connection.channel()

    heartbeat_exchange = await channel.declare_exchange(Exchanges.NodeHeartbeat, ExchangeType.FANOUT)

    print("node heartbeat running...")
    while True:
        heartbeat_json = NodeHeartbeatSchema().dumps(NodeHeartbeat(
            node_id=str(node_details.node_id),
            timestamp=datetime.datetime.utcnow(),
            tags=node_details.tags
        ))
        message = aio_pika.Message(
            heartbeat_json.encode(),
            content_type='application/json'
        )
        await heartbeat_exchange.publish(message, routing_key='')
        await asyncio.sleep(10.0)


async def process_execution_requests(loop: asyncio.AbstractEventLoop, connection: aio_pika.Connection,
                                     docker_client: docker.DockerClient, node_details: NodeDetails):
    print("establishing execution request channel...")
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    request_dlq_exchange = await channel.declare_exchange(Exchanges.ExecutionDLQ, ExchangeType.FANOUT)
    request_exchange = await channel.declare_exchange(Exchanges.ExecutionRequest, ExchangeType.FANOUT)
    response_exchange = await channel.declare_exchange(Exchanges.ExecutionOutcome, ExchangeType.FANOUT)

    request_queue_message_timeout = 60 * 1000
    request_queue_timeout = int(request_queue_message_timeout * 2.5)
    request_queue = await channel.declare_queue(arguments={
        'x-message-ttl': request_queue_message_timeout,
        'x-expires': request_queue_timeout,
        'x-dead-letter-exchange': request_dlq_exchange.name,
    })
    await request_queue.bind(request_exchange.name, str(node_details.node_id))

    print("awaiting execution requests...")
    async with request_queue.iterator() as incoming_messages:
        async for message in incoming_messages:
            message: aio_pika.IncomingMessage = message

            try:
                async with message.process():
                    response = await loop.run_in_executor(None, handle_incoming_request, docker_client, message)
            except Exception as ex:
                print(f"exception processing request: {type(ex).__name__} - {ex}")
                response = ExecutionResponse(
                    None,
                    None,
                    None,
                    f"exception processing request: {type(ex).__name__} - {ex}",
                    None,
                    {}
                )

            await submit_response(response, response_exchange)


def handle_incoming_request(client: docker.DockerClient, message: aio_pika.IncomingMessage) -> ExecutionResponse:
    print("received execution request:")
    request: ExecutionRequest = ExecutionRequestSchema().loads(message.body)
    print(f"  decoded: {request.request_id} - image: {request.image_name}")

    with tempfile.TemporaryDirectory(dir=os.getcwd(), prefix=".", suffix="-io-dir") as io_dir:
        if request.io_bind_path is not None:
            print(f"  io dir: {io_dir}")

            for file_name, data in request.inputs.items():
                print(f"  - preparing input '{file_name}' ({len(data)} bytes)...")
                with open(os.path.join(io_dir, file_name), 'wb') as handle:
                    handle.write(data)

            volumes = {
                io_dir: {
                    'bind': request.io_bind_path,
                    'mode': 'rw',
                },
            }
        else:
            print("  io directory will not be mounted, as requested")
            volumes = None

        try:
            print("  ensuring image is available...")
            client.images.pull(request.image_name, 'latest')
            print("  image retrieved")
        except ImageNotFound:
            return ExecutionResponse(
                request_id=request.request_id,
                exit_status=None,
                exit_error=None,
                error=f"no such image '{request.image_name}'",
                logs=None,
                outputs={}
            )

        print("  creating container...")
        container: Container = client.containers.create(
            request.image_name,
            volumes=volumes,
            detach=True
        )
        try:
            print("  starting container...")
            container.start()
            print(f"  container started, waiting {request.timeout}s for exit...")
            exit_status = container.wait(timeout=request.timeout)
            print(f"  container exited: {exit_status}")

            if request.retrieve_logs:
                print("  processing logs...")
                stdout = _process_logs(container, 'stdout', stdout=True, stderr=False)
                stderr = _process_logs(container, 'stderr', stdout=False, stderr=True)
                all_logs = stdout + stderr
                all_logs.sort(key=lambda entry: entry[1])
                all_logs = [
                    f"{kind} {timestamp} {text}"
                    for kind, timestamp, text in all_logs
                ]
            else:
                all_logs = None
        except (ReadTimeout, ConnectionError):
            print(f"  timed out while waiting")
            return ExecutionResponse(
                request_id=request.request_id,
                exit_status=None,
                exit_error=None,
                error="timed out waiting for container to finish executing",
                logs=None,
                outputs={}
            )
        finally:
            print(f"  container status: {container.status}")
            if container.status in ("running", "created"):
                print("  waiting for container to stop gracefully")
                container.stop(timeout=30)
                print(f"  container stopped ({container.status})")
            print("  removing container...")
            container.remove(force=True)
            print("  container removed")

        outputs = {}
        if request.outputs:
            print("  processing outputs...")
            for file_name in request.outputs:
                print(f"  - retrieving output '{file_name}'...")
                with open(os.path.join(io_dir, file_name), 'rb') as handle:
                    contents = handle.read()
                    print(f"    ({len(contents)} bytes)")
                    outputs[file_name] = contents

        response = ExecutionResponse(
            request_id=request.request_id,
            exit_status=exit_status["StatusCode"],
            exit_error=exit_status["Error"],
            error=None,
            logs=all_logs,
            outputs=outputs
        )

        return response


async def submit_response(response: ExecutionResponse, exchange: aio_pika.Exchange):
    print("  preparing response...")
    response_json = ExecutionResponseSchema().dumps(response)
    message = aio_pika.Message(
        response_json.encode(),
        content_type='application/json'
    )
    await exchange.publish(message, '')
    print(f"  response: {response_json}")


def _process_logs(container: Container, tag: str, stdout: bool, stderr: bool) -> typing.List[
        typing.Tuple[str, datetime.datetime, bytes]]:

    def _process_line(line) -> typing.Tuple[str, datetime.datetime, bytes]:
        line = line.decode()
        timestamp, remainder = line.split(' ', maxsplit=1)
        if timestamp.endswith("Z"):
            timestamp = timestamp[:-1] + "+00:00"
        timestamp_without_nanos = timestamp[:26] + timestamp[29:]
        timestamp = datetime.datetime.fromisoformat(timestamp_without_nanos)
        return tag, timestamp, remainder

    return [
        _process_line(line)
        for line in container.logs(stdout=stdout, stderr=stderr, timestamps=True, stream=True)
    ]
