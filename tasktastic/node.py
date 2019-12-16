import datetime
import os
import tempfile

import typing

from aio_pika import ExchangeType
from docker.models.containers import Container
from dataclasses import dataclass
import aio_pika
import asyncio
import docker

from tasktastic.schemas import ExecutionRequestSchema, ExecutionRequest, ExecutionResponse, ExecutionResponseSchema


@dataclass(frozen=True)
class NodeArguments:
    orchestrator_uri: str
    loop: asyncio.AbstractEventLoop


async def main(args: NodeArguments) -> int:
    print(f"node invoked with args: {args}")

    docker_client = docker.from_env()
    print("checking connection to docker host...")
    docker_client.ping()
    print("confirmed connection to docker host")

    print("connecting to message broker...")
    connection: aio_pika.Connection = await aio_pika.connect_robust(
        args.orchestrator_uri,
        loop=args.loop
    )

    print("connected to message broker, awaiting messages")
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        queue = await channel.declare_queue(exclusive=True)
        response_exchange = await channel.declare_exchange('tasktastic.execution.outcome', ExchangeType.FANOUT)

        async with queue.iterator() as incoming_messages:
            async for message in incoming_messages:
                message: aio_pika.IncomingMessage = message

                try:
                    async with message.process():
                        response = await handle_incoming_request(docker_client, message)
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

    return 0


async def handle_incoming_request(client: docker.DockerClient, message: aio_pika.IncomingMessage) -> ExecutionResponse:
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
        finally:
            print("  removing container...")
            container.remove()
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


def _process_logs(container: Container, tag: str, stdout: bool, stderr: bool) -> typing.List[typing.Tuple[str, datetime.datetime, bytes]]:
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
