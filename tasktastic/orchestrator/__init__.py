import signal

import marshmallow
from aiohttp import web
import dataclasses
import aio_pika
import asyncio

from tasktastic.common.schemas import ExecutionResponseSchema
from tasktastic.orchestrator import http_schemas
from tasktastic.orchestrator.scheduler import Scheduler, SchedulingException


@dataclasses.dataclass(frozen=True)
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

        await scheduler.start_receiving_execution_outcomes(channel)
        await scheduler.start_receiving_execution_request_dlq(channel)
        background_tasks = asyncio.gather(
            await scheduler.start_receiving_node_heartbeats(channel)
        )
        await scheduler.enable_job_execution(channel)

        print("starting HTTP API...")
        http_api = await create_http_api(scheduler)
        runner = web.AppRunner(http_api)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 5000)
        await site.start()
        print("HTTP API started")

        exit_requested = args.loop.create_future()
        args.loop.add_signal_handler(signal.SIGINT, lambda: exit_requested.set_result(True))
        print("waiting for exit to be requested...")
        await exit_requested
        print("exit requested")

        print("stopping HTTP API...")
        await runner.cleanup()

        print("cancelling background tasks...")
        try:
            background_tasks.cancel()
        except asyncio.CancelledError:
            pass

    print("exiting")
    return 0


async def create_http_api(scheduler: 'Scheduler') -> web.Application:
    routes = web.RouteTableDef()

    @routes.get("/api/nodes")
    async def list_known_nodes(_request):
        return web.json_response({
            'nodes': [
                {
                    'node_id': node.node_id,
                    'last_heartbeat': node.last_heartbeat.isoformat(),
                    'tags': node.tags,
                }
                for node in scheduler.known_nodes.values()
            ]
        })

    @routes.post("/api/execution-requests")
    async def post_new_execution_request(request: web.Request):
        execution_request_json = await request.json()
        try:
            execution_request = http_schemas.ExecutionRequestSchema().load(execution_request_json)
        except marshmallow.ValidationError:
            return web.Response(status=400, text="invalid execution request")

        execution_response_future = await scheduler.submit_execution_request(execution_request)

        try:
            execution_response = await execution_response_future
        except SchedulingException as e:
            return web.Response(status=500, text=f"error performing execution request: {e}")

        return web.json_response(ExecutionResponseSchema().dump(execution_response))

    app = web.Application()
    app.add_routes(routes)

    return app


