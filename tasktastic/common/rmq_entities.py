

ENTITY_PREFIX = "tasktastic"


def _dotted(*args: str) -> str:
    return ".".join(_remove_surrounding_dots(a) for a in args)


def _remove_surrounding_dots(text: str) -> str:
    return text.strip('.')


class Exchanges:
    NodeHeartbeat = _dotted(ENTITY_PREFIX, "node", "heartbeat")
    ExecutionRequest = _dotted(ENTITY_PREFIX, "execution", "request")
    ExecutionDLQ = _dotted(ENTITY_PREFIX, "execution", "dlq")
    ExecutionOutcome = _dotted(ENTITY_PREFIX, "execution", "outcome")
