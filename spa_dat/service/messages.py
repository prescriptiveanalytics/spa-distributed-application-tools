from pydantic.dataclasses import dataclass


@dataclass
class Message:
    topic: str
    payload: bytes
