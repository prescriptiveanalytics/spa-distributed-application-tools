import asyncio
from typing import Protocol


class SocketProvider(Protocol):
    """
    A service provider is a class which creates a socket from a given configuration and returns it.
    It also allows to add a queue for communication
    """

    def create_socket(self, queue: asyncio.Queue) -> None:
        raise NotImplementedError()
