from dataclasses import dataclass
from aio_pika import ExchangeType

ENTITY_PREFIX = "tasktastic"


def _dotted(*args: str) -> str:
    return ".".join(_remove_surrounding_dots(a) for a in args)


def _remove_surrounding_dots(text: str) -> str:
    return text.strip('.')


@dataclass
class ExchangeDetails:
    name: str
    kind: ExchangeType


class Exchanges:
    NodeHeartbeat = ExchangeDetails(_dotted(ENTITY_PREFIX, "node", "heartbeat"), ExchangeType.FANOUT)
    ExecutionRequest = ExchangeDetails(_dotted(ENTITY_PREFIX, "execution", "request"), ExchangeType.TOPIC)
    ExecutionDLQ = ExchangeDetails(_dotted(ENTITY_PREFIX, "execution", "dlq"), ExchangeType.FANOUT)
    ExecutionOutcome = ExchangeDetails(_dotted(ENTITY_PREFIX, "execution", "outcome"), ExchangeType.FANOUT)
