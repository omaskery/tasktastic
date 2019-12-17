import datetime
import signal

import typing
from aio_pika import ExchangeType, IncomingMessage
from dataclasses import dataclass
import aio_pika
import asyncio

from tasktastic.schemas import ExecutionResponseSchema, ExecutionResponse, NodeHeartbeatSchema, NodeHeartbeat


@dataclass(frozen=True)
class OrchestratorArguments:
    rabbitmq_uri: str
    loop: asyncio.AbstractEventLoop


async def main(args: OrchestratorArguments) -> int:
    print(f"orchestrator invoked with args: {args}")

    print("connecting to message broker...")
    connection: aio_pika.Connection = await aio_pika.connect_robust(
        args.rabbitmq_uri,
        loop=args.loop
    )

    scheduler = Scheduler(args.loop)

    print("connected to message broker, awaiting messages")
    async with connection:
        channel = await connection.channel()

        await start_receiving_execution_outcomes(channel)
        background_tasks = asyncio.gather(
            await scheduler.start_receiving_node_heartbeats(channel)
        )

        exit_requested = args.loop.create_future()
        args.loop.add_signal_handler(signal.SIGINT, lambda: exit_requested.set_result(True))
        print("waiting for exit to be requested...")
        await exit_requested
        print("exit requested")

        print("cancelling background tasks...")
        try:
            background_tasks.cancel()
        except asyncio.CancelledError:
            pass

    print("exiting")
    return 0


@dataclass
class KnownNode:
    node_id: str
    last_heartbeat: datetime.datetime
    tags: typing.Dict[str, str]

    def time_since_heartbeat(self) -> datetime.timedelta:
        return datetime.datetime.utcnow() - self.last_heartbeat


class Scheduler:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.known_nodes: typing.Dict[str, KnownNode] = {}
        self.loop = loop

    async def start_receiving_node_heartbeats(self, channel) -> asyncio.Future:
        response_exchange = await channel.declare_exchange('tasktastic.node.heartbeat', ExchangeType.FANOUT)
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(response_exchange)
        await queue.consume(self._on_node_heartbeat_received)

        return asyncio.gather(
            self._start_heartbeat_monitor()
        )

    async def _start_heartbeat_monitor(self):
        check_period = 5.0
        timeout_duration = datetime.timedelta(seconds=20)
        while True:
            self._time_out_nodes(timeout_duration)

            await asyncio.sleep(check_period)

    def _time_out_nodes(self, timeout_duration: datetime.timedelta):
        timed_out = [
            node for node in self.known_nodes.values()
            if node.time_since_heartbeat() >= timeout_duration
        ]
        for node in timed_out:
            del self.known_nodes[node.node_id]
            print(f"node {node.node_id} offline (last heartbeat: {node.last_heartbeat})")

    async def _on_node_heartbeat_received(self, message: IncomingMessage):
        async with message.process():
            heartbeat: NodeHeartbeat = NodeHeartbeatSchema().loads(message.body)

            node = KnownNode(
                node_id=heartbeat.node_id,
                last_heartbeat=heartbeat.timestamp,
                tags=heartbeat.tags,
            )

            if node.node_id not in self.known_nodes:
                print(f"node {node.node_id} online (as of {node.last_heartbeat})")

            self.known_nodes[node.node_id] = node


async def start_receiving_execution_outcomes(channel):
    response_exchange = await channel.declare_exchange('tasktastic.execution.outcome', ExchangeType.FANOUT)
    queue = await channel.declare_queue(exclusive=True)
    await queue.bind(response_exchange)
    await queue.consume(on_execution_outcome_received)


async def on_execution_outcome_received(message: IncomingMessage):
    async with message.process():
        response: ExecutionResponse = ExecutionResponseSchema().loads(message.body)

        print(f"execution response: {response.request_id}")
        if response.error:
            print(f"  error: {response.error}")
        else:
            print(f"  status code: {response.exit_status}")
            print(f"  exit error: {response.exit_error}")

            if response.logs:
                print(f"  logs:")
                for log_line in response.logs:
                    print(f"  - {log_line.strip()}")
