import datetime
from dataclasses import dataclass, field
import marshmallow_dataclass
import marshmallow
import binascii
import typing


class Base64Bytes(marshmallow.fields.Field):

    def _serialize(self, value, attr, obj, **kwargs):
        result = binascii.b2a_base64(value).decode()
        return result

    def _deserialize(self, value, attr, data, **kwargs):
        result = binascii.a2b_base64(value)
        return result


@dataclass
class ExecutionRequest:
    request_id: str = field()
    image_name: str = field()
    io_bind_path: typing.Optional[str] = field(default="/task-io", metadata=dict(allow_none=True))
    timeout: int = field(default=30)
    inputs: typing.Dict[str, bytes] = field(default=dict, metadata=dict(
        marshmallow_field=marshmallow.fields.Dict(
            keys=marshmallow.fields.Str(),
            values=Base64Bytes()
        )
    ))
    outputs: typing.List[str] = field(default=list)
    retrieve_logs: bool = field(default=False)


@dataclass
class ExecutionResponse:
    request_id: typing.Optional[str] = field()
    exit_status: typing.Optional[int] = field()
    exit_error: typing.Optional[str] = field(default=None, metadata=dict(allow_none=True))
    error: typing.Optional[str] = field(default=None)
    logs: typing.Optional[typing.List[str]] = field(default=None, metadata=dict(allow_none=True))
    outputs: typing.Dict[str, bytes] = field(default=dict, metadata=dict(
        marshmallow_field=marshmallow.fields.Dict(
            keys=marshmallow.fields.Str(),
            values=Base64Bytes()
        )
    ))


@dataclass
class NodeHeartbeat:
    node_id: str = field()
    timestamp: datetime.datetime = field()
    tags: typing.Dict[str, str] = field()


ExecutionRequestSchema = marshmallow_dataclass.class_schema(ExecutionRequest)
ExecutionResponseSchema = marshmallow_dataclass.class_schema(ExecutionResponse)
NodeHeartbeatSchema = marshmallow_dataclass.class_schema(NodeHeartbeat)
