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


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
