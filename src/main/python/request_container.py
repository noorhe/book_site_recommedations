from dataclasses import dataclass

@dataclass(frozen=True)
class RequestContainer:
    request: any
    __type__: str
