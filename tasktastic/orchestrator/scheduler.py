import asyncio
import dataclasses
import datetime
import random
import typing
import uuid

import aio_pika
from aio_pika import Message, IncomingMessage

from tasktastic.common.rmq_entities import Exchanges
from tasktastic.common.schemas import ExecutionResponse, ExecutionRequest, ExecutionRequestSchema, NodeHeartbeat, \
    NodeHeartbeatSchema, ExecutionResponseSchema
from tasktastic.orchestrator import http_schemas


class SchedulingException(Exception):
    pass


@dataclasses.dataclass
class KnownNode:
    node_id: str
    last_heartbeat: datetime.datetime
    tags: typing.Dict[str, str]
    outstanding_jobs: typing.List['OngoingExecutionJob']

    def time_since_heartbeat(self) -> datetime.timedelta:
        return datetime.datetime.utcnow() - self.last_heartbeat

    def matches_tags(self, tags: typing.Dict[str, str]) -> bool:
        for name, value in tags.items():
            if name not in self.tags:
                return False
            if self.tags[name] != value:
                return False
        return True


@dataclasses.dataclass
class OngoingExecutionJob:
    id_of_assigned_node: str
    request_id: str
    request_timestamp: datetime.datetime
    timeout_timestamp: datetime.datetime
    future: asyncio.Future

    def timeout_has_expired(self):
        return datetime.datetime.utcnow() >= self.timeout_timestamp

    @staticmethod
    def create_with_timeout(id_of_assigned_node: str, timeout_duration: datetime.timedelta, future: asyncio.Future):
        now = datetime.datetime.utcnow()
        return OngoingExecutionJob(
            id_of_assigned_node=id_of_assigned_node,
            request_id=str(uuid.uuid4()),
            request_timestamp=now,
            timeout_timestamp=now + timeout_duration,
            future=future
        )


class Scheduler:

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.known_nodes: typing.Dict[str, KnownNode] = {}
        self.ongoing_jobs: typing.Dict[str, OngoingExecutionJob] = {}

        self.loop = loop
        self.job_exchange: typing.Optional[aio_pika.Exchange] = None

    async def enable_job_execution(self, channel: aio_pika.Channel):
        self.job_exchange = await channel.declare_exchange(
            Exchanges.ExecutionRequest.name,
            Exchanges.ExecutionRequest.kind
        )

    async def start_receiving_node_heartbeats(self, channel: aio_pika.Channel) -> asyncio.Future:
        response_exchange = await channel.declare_exchange(Exchanges.NodeHeartbeat.name, Exchanges.NodeHeartbeat.kind)
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(response_exchange)
        await queue.consume(self._on_node_heartbeat_received)

        return asyncio.gather(
            self._start_heartbeat_monitor()
        )

    async def start_receiving_execution_outcomes(self, channel: aio_pika.Channel):
        response_exchange = await channel.declare_exchange(
            Exchanges.ExecutionOutcome.name,
            Exchanges.ExecutionOutcome.kind
        )
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(response_exchange)
        await queue.consume(self._on_execution_outcome_received)

    async def start_receiving_execution_request_dlq(self, channel: aio_pika.Channel):
        request_dlq_exchange = await channel.declare_exchange(Exchanges.ExecutionDLQ.name, Exchanges.ExecutionDLQ.kind)
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(request_dlq_exchange)
        await queue.consume(self._on_execution_request_dlq_received)

    async def submit_execution_request(self, user_execution_request: http_schemas.ExecutionRequest) -> asyncio.Future:
        result = self.loop.create_future()

        if self.job_exchange is None:
            result.set_exception(SchedulingException("execution jobs are not enabled"))
            return result

        if not self.known_nodes:
            result.set_exception(SchedulingException(f"no available nodes"))
            return result

        possible_nodes = [
            node
            for node in self.known_nodes.values()
            if node.matches_tags(user_execution_request.tags)
        ]
        possible_nodes.sort(key=lambda node: len(node.outstanding_jobs))

        if not possible_nodes:
            result.set_exception(SchedulingException(f"no nodes matching request tags"))
            return result

        assigned_node: KnownNode = random.choice(possible_nodes)
        ongoing_job = OngoingExecutionJob.create_with_timeout(
            id_of_assigned_node=assigned_node.node_id,
            timeout_duration=datetime.timedelta(seconds=120),
            future=result
        )
        self.ongoing_jobs[ongoing_job.request_id] = ongoing_job
        assigned_node.outstanding_jobs.append(ongoing_job)

        execution_request = ExecutionRequest(
            request_id=ongoing_job.request_id,
            image_name=user_execution_request.image_name,
            io_bind_path=user_execution_request.io_bind_path,
            timeout=user_execution_request.timeout,
            inputs=user_execution_request.inputs,
            outputs=user_execution_request.outputs,
            retrieve_logs=user_execution_request.retrieve_logs
        )

        print("scheduling job {} on node {} ({} outstanding jobs)".format(
            execution_request.request_id,
            assigned_node.node_id,
            len(assigned_node.outstanding_jobs)
        ))
        execution_request_json = ExecutionRequestSchema().dumps(execution_request)
        await self.job_exchange.publish(
            message=Message(
                execution_request_json.encode(),
                content_type='application/json'
            ),
            routing_key=ongoing_job.id_of_assigned_node
        )

        return result

    async def _start_heartbeat_monitor(self):
        check_period = 5.0
        timeout_duration = datetime.timedelta(seconds=20)
        while True:
            self._time_out_nodes(timeout_duration)
            self._time_out_jobs()

            await asyncio.sleep(check_period)

    def _time_out_nodes(self, timeout_duration: datetime.timedelta):
        timed_out = [
            node for node in self.known_nodes.values()
            if node.time_since_heartbeat() >= timeout_duration
        ]
        for node in timed_out:
            del self.known_nodes[node.node_id]
            print(f"node {node.node_id} offline (last heartbeat: {node.last_heartbeat})")

    def _time_out_jobs(self):
        timed_out = [
            job for job in self.ongoing_jobs.values()
            if job.timeout_has_expired()
        ]
        for job in timed_out:
            self._fail_job(job.request_id, "job timed out (no response)")

    async def _on_node_heartbeat_received(self, message: IncomingMessage):
        async with message.process():
            heartbeat: NodeHeartbeat = NodeHeartbeatSchema().loads(message.body)

            if heartbeat.node_id not in self.known_nodes:
                print(f"node {heartbeat.node_id} online (as of {heartbeat.timestamp})")

            new_node = KnownNode(
                node_id=heartbeat.node_id,
                last_heartbeat=heartbeat.timestamp,
                tags=heartbeat.tags,
                outstanding_jobs=[]
            )

            node = self.known_nodes.setdefault(heartbeat.node_id, new_node)
            node.last_heartbeat = heartbeat.timestamp

            self.known_nodes[node.node_id] = node

    async def _on_execution_outcome_received(self, message: IncomingMessage):
        async with message.process():
            response: ExecutionResponse = ExecutionResponseSchema().loads(message.body)

            if response.error:
                job = self._fail_job(response.request_id, f"execution failed with reason: {response.error}")
                details = f"(error: {response.error})"
            else:
                job = self._complete_job(response.request_id, response)
                details = f"(exit status: {response.exit_status}, exit error: {response.exit_error})"

            if job:
                print(f"execution response: {response.request_id} {details}")
                node = self.known_nodes.get(job.id_of_assigned_node)
                if node:
                    print(f"  node {node.node_id} now has {len(node.outstanding_jobs)} outstanding job")

    async def _on_execution_request_dlq_received(self, message: IncomingMessage):
        async with message.process():
            request: ExecutionRequest = ExecutionRequestSchema().loads(message.body)
            print(f"execution request for {message.routing_key} hit DLQ: {request}")
            self._fail_job(request.request_id, "job rejected (node failed or message timed out in queue)")

    def _complete_job(self, request_id: str, result: ExecutionResponse):
        job = self._remove_job(request_id)
        if not job:
            return job
        job.future.set_result(result)
        return job

    def _fail_job(self, request_id: str, reason: str):
        job = self._remove_job(request_id)
        if not job:
            return job
        job.future.set_exception(SchedulingException(reason))
        return job

    def _remove_job(self, request_id):
        job = self.ongoing_jobs.get(request_id)
        if not job:
            return job
        del self.ongoing_jobs[request_id]

        assigned_node = self.known_nodes.get(job.id_of_assigned_node)
        if assigned_node is not None and job in assigned_node.outstanding_jobs:
            assigned_node.outstanding_jobs.remove(job)

        return job
