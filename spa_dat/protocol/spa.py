import time
from typing import Protocol

from pydantic import BaseModel
from pydantic.dataclasses import dataclass


@dataclass
class SpaProtocol(Protocol):
    """
    Defines the interface for SPA applications to communicate with the message bus.
    """

    async def publish():
        raise NotImplementedError()

    async def subscribe():
        raise NotImplementedError()

    async def unsubscribe():
        raise NotImplementedError()

    async def request():
        raise NotImplementedError()


class SpaMessage(BaseModel):
    """
    Defines the message for SPA applications
    """

    client_id: str
    client_name: str
    content_type: str
    payload: bytes
    topic: str
    response_topic: str | None = None
    quality_of_service: int = 0
    timestamp: int = int(time.time())
