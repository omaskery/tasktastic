import signal

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

    print("connected to message broker, awaiting messages")
    async with connection:
        channel = await connection.channel()

        await start_receiving_execution_outcomes(channel)
        await start_receiving_node_heartbeats(channel)

        exit_requested = args.loop.create_future()
        args.loop.add_signal_handler(signal.SIGINT, lambda: exit_requested.set_result(True))
        print("waiting for exit to be requested...")
        await exit_requested
        print("exit requested")

    return 0


async def start_receiving_node_heartbeats(channel):
    response_exchange = await channel.declare_exchange('tasktastic.node.heartbeat', ExchangeType.FANOUT)
    queue = await channel.declare_queue(exclusive=True)
    await queue.bind(response_exchange)
    await queue.consume(on_node_heartbeat_received)


async def start_receiving_execution_outcomes(channel):
    response_exchange = await channel.declare_exchange('tasktastic.execution.outcome', ExchangeType.FANOUT)
    queue = await channel.declare_queue(exclusive=True)
    await queue.bind(response_exchange)
    await queue.consume(on_execution_outcome_received)


async def on_node_heartbeat_received(message: IncomingMessage):
    response: NodeHeartbeat = NodeHeartbeatSchema().loads(message.body)
    print(f"node heartbeat: {response}")


async def on_execution_outcome_received(message: IncomingMessage):
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
