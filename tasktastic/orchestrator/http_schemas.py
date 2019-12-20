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

    @marshmallow.pre_load(pass_many=False)
    def stringify_incoming_tag_values(self, value, *_args, **_kwargs):
        if not isinstance(value, dict):
            return value
        if 'tags' not in value:
            return value
        if not isinstance(value['tags'], dict):
            return value
        for tag_name, tag_value in value['tags'].items():
            value['tags'][tag_name] = str(tag_value)
        return value


ExecutionRequestSchema = marshmallow_dataclass.class_schema(ExecutionRequest)
