import asyncio
import logging
from contextlib import (
    AbstractAsyncContextManager,
    AbstractContextManager,
    AsyncExitStack,
)
from typing import Protocol, Union

from spa_dat.protocol.typedef import SocketProvider, SpaMessage, SpaSocket

logger = logging.getLogger(__name__)

_SupportedContextManagers = Union[AbstractAsyncContextManager, AbstractContextManager]

class ProducerCallback(Protocol):
    async def __call__(self, socket: SpaSocket, **kwargs) -> None:
        raise NotImplementedError()


class ConsumerCallback(Protocol):
    async def __call__(self, message: SpaMessage, socket: SpaSocket, **kwargs) -> None:
        raise NotImplementedError()


class ApplicationLifeCycle(Protocol):
    def setup(self):
        """
        Perform initialization tasks, such as setting up resources and dependencies
        """
        raise NotImplementedError()

    def run(self):
        """
        Execute your application - should block at this point
        """
        raise NotImplementedError()

    def teardown(self):
        """
        Handle cleanup
        """
        raise NotImplementedError()


class ProducerApplication(ApplicationLifeCycle):
    """
    This provides a simple class for implementing a distributed application.
    It connects to a given message bus and provides a services for handling messages.
    It acts only as a producer by default

    Attributes:
        async_callback: A callback which is called upon receiving a message.
        config: The configuration for the message bus. Can be any of the supported config types.
        ressources: A list of context managers which are entered and exited during the livecycle
        _queue_in (asyncio.Queue): A queue for receiving messages from the message bus.
    """

    def __init__(
        self,
        async_callback: ProducerCallback,
        socket_provider: SocketProvider,
        ressources: dict[str, _SupportedContextManagers] = {},
    ) -> None:
        self.exit_stack = AsyncExitStack()
        self.callback = async_callback
        self.socket_provider = socket_provider

        self.ressources: dict[str, _SupportedContextManagers] = ressources

    def setup(self):
        """
        Method for initializing non async components/ressources of the application.
        """
        logger.info(f"Starting Producer '{self.callback.__name__}'")
        self.socket = self.socket_provider.create_socket(None)


    def run(self):
        asyncio.run(self.run_async())

    async def run_async(self):
        """
        Start the Application and initialize the default asyncio loop.
        Initialize the application and its ressources.
        """
        self.setup()
        async with self.exit_stack:
            # enter fixed context
            await self.exit_stack.enter_async_context(self.socket)

            # enter dynamic ressource context
            for ressource in self.ressources:
                if isinstance(ressource, AbstractAsyncContextManager):
                    await self.exit_stack.enter_async_context(ressource)
                else:
                    self.exit_stack.enter_context(ressource)

            # shut down after leaving context
            self.exit_stack.callback(self.teardown)

            await self.callback(socket=self.socket, **self.ressources)

    def teardown(self):
        """
        Method for cleanup non async components/ressources of the application.
        """
        logger.info(f"Shutdown Producer '{self.callback.__name__}'")


class ConsumerApplication(ApplicationLifeCycle):
    """
    This provides a simple class for implementing a distributed application.
    It provides a service for handling messages.
    It calls a callback directly, which can produce and read messages.
    This service runs in an endless loop until stopped by the user.

    Attributes:
        async_callback: A callback which is called upon receiving a message.
        config: The configuration for the message bus. Can be any of the supported config types.
        ressources (list[AbstractAsyncContextManager]): A list of context managers which are entered and exited.
        _queue_in (asyncio.Queue): A queue for receiving messages from the message bus.
    """

    def __init__(
        self,
        async_callback,
        socket_provider: SocketProvider,
        ressources: dict[str, _SupportedContextManagers] = {},
    ) -> None:
        super().__init__()
        self.callback = async_callback
        self._queue_in = asyncio.Queue()
        self.exit_stack = AsyncExitStack()
        self.socket_provider = socket_provider

        # Any async context manager can be added here, these will be entered and exited
        # during the main loop
        self.ressources: dict[str, _SupportedContextManagers] = ressources

    def setup(self):
        """
        Method for initializing non async components/ressources of the application.
        When this method is executed, the asyncio loop is already yet running.
        """
        logger.info(f"Starting Application '{self.callback.__name__}'")
        self.socket = self.socket_provider.create_socket(self._queue_in)

    def run(self):
        """
        Start the Application and initialize the default asyncio loop
        """
        asyncio.run(self.run_async())

    async def run_async(self):
        """
        Start the Application and initialize the default asyncio loop.
        Initialize the application and its ressources.
        """

        self.setup()
        async with self.exit_stack:
            # enter fixed context
            await self.exit_stack.enter_async_context(self.socket)

            # enter dynamic context
            for ressource in self.ressources:
                await self.exit_stack.enter_async_context(ressource)

            # shut down after leaving context
            self.exit_stack.callback(self.teardown)

            # read and handle messages
            while True:
                message = await self._queue_in.get()
                await self.callback(message=message, socket=self.socket, **self.ressources)

    def teardown(self):
        """
        Method for cleanup non async components/ressources of the application.
        """
        logger.info(f"Stopping Application '{self.callback.__name__}'")


class DistributedApplication(ApplicationLifeCycle):
    def __init__(self, default_socket_provider: SocketProvider) -> None:
        self.applications = []
        self.default_socket_provider = default_socket_provider

    def add_application(self, async_consumer_callback: ConsumerCallback, socket_provider: SocketProvider):
        self.applications.append(
            ConsumerApplication(
                async_callback=async_consumer_callback,
                socket_provider=socket_provider,
            )
        )

    def add_producer_application(self, async_producer_callback: ProducerCallback, socket_provider: SocketProvider):
        self.applications.append(
            ProducerApplication(
                async_callback=async_producer_callback,
                socket_provider=socket_provider,
            )
        )

    def application(self, topics: list[str] | str, *, socket_provider: SocketProvider | None = None):
        socket_provider = socket_provider or self.default_socket_provider
        if socket_provider is None:
            raise ValueError("No socket provider found. Either set the default in the constructor or here!")

        socket_provider.overwrite_config(topics=topics)

        def inner(callback: ConsumerCallback):
            self.add_application(callback, socket_provider)

        return inner

    def producer(self, *, socket_provider: SocketProvider | None = None):
        socket_provider = socket_provider or self.default_socket_provider
        if socket_provider is None:
            raise ValueError("No socket provider found. Either set the default in the constructor or here!")

        def inner(callback: ProducerCallback):
            self.add_producer_application(callback, socket_provider)

        return inner

    def run(self):
        asyncio.run(self.run_async())

    async def run_async(self):
        await asyncio.gather(*[app.run_async() for app in self.applications])
