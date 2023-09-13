from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Protocol, Union

from spa_dat.socket.typedef import SpaMessage, SpaSocket


class ProducerCallback(Protocol):
    async def __call__(self, *, socket: SpaSocket, **kwargs) -> None:
        raise NotImplementedError()


class ConsumerCallback(Protocol):
    async def __call__(self, *, message: SpaMessage, socket: SpaSocket, **kwargs) -> None:
        raise NotImplementedError()


class ApplicationLifeCycle(Protocol):
    def setup(self):
        """
        Perform initialization tasks, such as setting up resources and dependencies
        """
        raise NotImplementedError()

    def start(self):
        """
        Execute your application - should block at this point
        """
        raise NotImplementedError()

    async def start_async(self):
        """
        Asyncronous method which starts the application itself, not blocking
        """
        raise NotImplementedError()

    async def run_async(self):
        """
        Asyncronous method which contains the logic of the application. (e.g. endless loop, etc.)
        """
        raise NotImplementedError()

    def teardown(self):
        """
        Handle cleanup
        """
        raise NotImplementedError()


SupportedContextManagers = Union[AbstractAsyncContextManager, AbstractContextManager]
