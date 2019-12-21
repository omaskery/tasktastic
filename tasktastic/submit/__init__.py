from dataclasses import dataclass
import urllib.parse
import requests
import sys

from tasktastic.common.schemas import ExecutionResponseSchema, ExecutionResponse
from tasktastic.orchestrator.http_schemas import ExecutionRequest, ExecutionRequestSchema


@dataclass(frozen=True)
class SubmitArguments:
    orchestrator_uri: str
    request: ExecutionRequest


async def main(args: SubmitArguments) -> int:
    uri = urllib.parse.urljoin(args.orchestrator_uri, "/api/execution-requests")

    try:
        request_json = ExecutionRequestSchema().dump(args.request)
        response = requests.post(uri, json=request_json)
        response.raise_for_status()
        response_json = response.json()
        execution_response: ExecutionResponse = ExecutionResponseSchema().load(response_json)

        if execution_response.logs:
            for log_event in execution_response.logs:
                output = sys.stdout if log_event.startswith("stdout") else sys.stderr
                print(log_event[7:], file=output, end='')

        if execution_response.outputs:
            for file, contents in execution_response.outputs.items():
                with open(file, 'wb') as fh:
                    fh.write(contents)

        return execution_response.exit_status
    except requests.HTTPError as err:
        response: requests.Response = err.response
        print(f"{err}\n\t{response.content.decode()}", file=sys.stderr)
        return -1
