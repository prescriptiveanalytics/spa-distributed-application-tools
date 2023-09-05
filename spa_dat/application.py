import asyncio
import logging
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass
from typing import Callable, Protocol

from spa_dat.protocol.typedef import SocketProvider, SpaMessage, SpaSocket

logger = logging.getLogger(__name__)


# using normal dataclass here because pydantic can not validate subclasses
@dataclass
class DistributedApplicationContext:
    message_service: SpaSocket


ProducerCallback = Callable[[DistributedApplicationContext], None]
ConsumerCallback = Callable[[SpaMessage, DistributedApplicationContext], None]


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
        logger.info("[Shutdown] Application")


class FastDistributedApplication(ApplicationLifeCycle):
    def __init__(self, socket_provider: SocketProvider) -> None:
        self.applications = []
        self.default_socket_provider = socket_provider

    def add_application(self, async_consumer_callback: ConsumerCallback, socket_provider: SocketProvider):
        self.applications.append(
            DistributedApplication(
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

    def application(self, socket_provider: SocketProvider | None = None):
        socket_provider = socket_provider or self.default_socket_provider
        if socket_provider is None:
            raise ValueError("No socket provider found. Either set the default in the constructor or here!")

        def inner(callback: ConsumerCallback):
            self.add_application(callback, socket_provider)

        return inner

    def producer(self, socket_provider: SocketProvider | None = None):
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


class ProducerApplication(ApplicationLifeCycle):
    """
    This provides a simple class for implementing a distributed application.
    It connects to a given message bus and provides a services for handling messages.
    It acts only as a producer by default

    Attributes:
        async_callback: A callback which is called upon receiving a message.
        config: The configuration for the message bus. Can be any of the supported config types.
        async_ressources: A list of context managers which are entered and exited during the livecycle
        _queue_in (asyncio.Queue): A queue for receiving messages from the message bus.
    """

    def __init__(
        self,
        async_callback: ProducerCallback,
        socket_provider: SocketProvider,
        async_ressources: list[AbstractAsyncContextManager] = [],
    ) -> None:
        self.exit_stack = AsyncExitStack()
        self.callback = async_callback
        self.socket_provider = socket_provider

        self.async_ressources: list[AbstractAsyncContextManager] = async_ressources

    def setup(self):
        """
        Method for initializing non async components/ressources of the application.
        """
        logger.info("Startup Application")
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

            # enter dynamic context
            for ressource in self.async_ressources:
                await self.exit_stack.enter_async_context(ressource)

            # shut down after leaving context
            self.exit_stack.callback(self.teardown)

            await self.callback(DistributedApplicationContext(self.socket))

    def teardown(self):
        """
        Method for cleanup non async components/ressources of the application.
        """
        logger.info("Shutdown Application")


class DistributedApplication(ApplicationLifeCycle):
    """
    This provides a simple class for implementing a distributed application.
    It provides a service for handling messages.
    It calls a callback directly, which can produce and read messages.
    This service runs in an endless loop until stopped by the user.

    Attributes:
        async_callback: A callback which is called upon receiving a message.
        config: The configuration for the message bus. Can be any of the supported config types.
        async_ressources (list[AbstractAsyncContextManager]): A list of context managers which are entered and exited.
        _queue_in (asyncio.Queue): A queue for receiving messages from the message bus.
    """

    def __init__(
        self, async_callback, socket_provider: SocketProvider, async_ressources: list[AbstractAsyncContextManager] = []
    ) -> None:
        super().__init__()
        self.callback = async_callback
        self._queue_in = asyncio.Queue()
        self.exit_stack = AsyncExitStack()
        self.socket_provider = socket_provider

        # Any async context manager can be added here, these will be entered and exited
        # during the main loop
        self.async_ressources: list[AbstractAsyncContextManager] = async_ressources

    def setup(self):
        """
        Method for initializing non async components/ressources of the application.
        When this method is executed, the asyncio loop is already yet running.
        """
        logger.info("Startup Application")
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
            for ressource in self.async_ressources:
                await self.exit_stack.enter_async_context(ressource)

            # shut down after leaving context
            self.exit_stack.callback(self.teardown)

            # read and handle messages
            while True:
                message = await self._queue_in.get()
                await self.callback(message, DistributedApplicationContext(self.socket))

    def teardown(self):
        """
        Method for cleanup non async components/ressources of the application.
        """
        logger.info("Shutdown Application")
