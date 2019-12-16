import argparse
import asyncio
import sys

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

    orchestrator_parser = subparsers.add_parser('orchestrator')
    orchestrator_parser.set_defaults(entry_point=orchestrator_entry_point)
    orchestrator_parser.add_argument("--rabbitmq", default="amqp://localhost", help='address of the rabbitmq host')

    args = parser.parse_args()

    sys.exit(await args.entry_point(event_loop, args, parser))


async def unknown_entry_point(_event_loop, _args, parser: argparse.ArgumentParser) -> int:
    parser.print_help()
    return 0


async def node_entry_point(event_loop, args, _parser: argparse.ArgumentParser) -> int:
    return await tasktastic.node.main(tasktastic.node.NodeArguments(
        orchestrator_uri=args.orchestrator,
        loop=event_loop,
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
