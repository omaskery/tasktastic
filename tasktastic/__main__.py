import argparse
import asyncio
import string
import sys

import typing

import tasktastic


async def main(event_loop: asyncio.AbstractEventLoop):
    parser = argparse.ArgumentParser(
        description="tools for executing arbitrary, language-agnostic work loads on remote systems"
    )
    parser.set_defaults(entry_point=unknown_entry_point)

    subparsers = parser.add_subparsers()

    node_parser = subparsers.add_parser('node')
    node_parser.set_defaults(entry_point=node_entry_point)
    node_parser.add_argument("orchestrator", help='address of the orchestrator node')
    node_parser.add_argument(
        '--tag', '-t', action='append', default=[], type=validate_tag_argument, dest='tags',
        help='add tag to the node in form TAG_NAME=TAG_VALUE'
    )

    orchestrator_parser = subparsers.add_parser('orchestrator')
    orchestrator_parser.set_defaults(entry_point=orchestrator_entry_point)
    orchestrator_parser.add_argument("--rabbitmq", default="amqp://localhost", help='address of the rabbitmq host')

    submit_parser = subparsers.add_parser('submit')
    submit_parser.set_defaults(entry_point=submit_entry_point)
    submit_parser.add_argument("orchestrator", help='address of the orchestrator node\'s HTTP API')
    submit_parser.add_argument("image_name", help='name of the docker image to execute')
    submit_parser.add_argument(
        '--tag', '-t', action='append', default=[], type=validate_tag_argument, dest='tags',
        help='specify a tag and value that must be present on the node that executes this job'
    )
    submit_parser.add_argument(
        '--io-bind-path', default=None, help='path to mount I/O volume in container'
    )
    submit_parser.add_argument(
        '--timeout', default=60, type=int, help='timeout before container is forcefully terminated'
    )
    submit_parser.add_argument(
        '-i', '--input', type=argparse.FileType(), default=[], dest='inputs', action='append',
        help='add input file to the job, will be placed in the IO volume'
    )
    submit_parser.add_argument(
        '-o', '--output', default=[], dest='outputs', action='append',
        help='add output file to the job, will be copied from the IO volume'
    )
    submit_parser.add_argument(
        '-L', '--retrieve-logs', default=False, action='store_true',
        help='retrieves console output from job and prints it to terminal'
    )

    args = parser.parse_args()

    sys.exit(await args.entry_point(event_loop, args, parser))


def validate_tag_argument(argument: str) -> typing.Tuple[str, str]:
    parts = argument.split('=', maxsplit=1)
    if len(parts) != 2:
        raise argparse.ArgumentTypeError("argument must be in the format TAG_NAME=TAG_VALUE")

    name, value = parts

    return (
        validate_slug_text_argument('tag name', name),
        validate_slug_text_argument('tag value', value)
    )


def validate_slug_text_argument(name: str, text: str) -> str:
    valid_slug_characters = string.ascii_letters + string.digits + '_-.~'
    valid = all(
        character in valid_slug_characters
        for character in text
    )
    if not valid:
        raise argparse.ArgumentTypeError(
            f"{name} '{text}' can only contain letters, digits, hyphen, period, underscore and tilde"
        )
    return text


async def unknown_entry_point(_event_loop, _args, parser: argparse.ArgumentParser) -> int:
    parser.print_help()
    return 0


async def node_entry_point(event_loop, args, _parser: argparse.ArgumentParser) -> int:
    return await tasktastic.node.main(tasktastic.node.NodeArguments(
        orchestrator_uri=args.orchestrator,
        loop=event_loop,
        tags=dict(args.tags)
    ))


async def orchestrator_entry_point(event_loop, args, _parser: argparse.ArgumentParser) -> int:
    return await tasktastic.orchestrator.main(tasktastic.orchestrator.OrchestratorArguments(
        rabbitmq_uri=args.rabbitmq,
        loop=event_loop,
    ))


async def submit_entry_point(_event_loop, args, _parser: argparse.ArgumentParser) -> int:
    return await tasktastic.submit.main(tasktastic.submit.SubmitArguments(
        orchestrator_uri=args.orchestrator,
        request=tasktastic.orchestrator.http_schemas.ExecutionRequest(
            image_name=args.image_name,
            tags=dict(args.tags),
            io_bind_path=args.io_bind_path if args.inputs or args.outputs else None,
            timeout=args.timeout,
            inputs={
                input_file.name: input_file.read()
                for input_file in args.inputs
            },
            outputs=args.outputs,
            retrieve_logs=args.retrieve_logs
        )
    ))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
