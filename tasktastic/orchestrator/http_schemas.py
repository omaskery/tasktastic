from dataclasses import dataclass, field
import marshmallow
import typing

import marshmallow_dataclass

from tasktastic.common.schemas import Base64Bytes


@dataclass
class ExecutionRequest:
    image_name: str = field()
    tags: typing.Dict[str, str] = field(default=dict)

    io_bind_path: typing.Optional[str] = field(default="/task-io", metadata=dict(allow_none=True))
    timeout: int = field(default=30)
    inputs: typing.Dict[str, bytes] = field(default=dict, metadata=dict(
        marshmallow_field=marshmallow.fields.Dict(
            keys=marshmallow.fields.Str(),
            values=Base64Bytes(),
            missing=dict
        )
    ))
    outputs: typing.List[str] = field(default=list)
    retrieve_logs: bool = field(default=False)


ExecutionRequestSchema = marshmallow_dataclass.class_schema(ExecutionRequest)
