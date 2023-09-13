import asyncio
import logging
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from typing import Any, Union

from spa_dat.application.typedef import (
    ApplicationLifeCycle,
    ConsumerCallback,
    ProducerCallback,
    SupportedContextManagers,
)
from spa_dat.socket.typedef import SocketProvider

logger = logging.getLogger(__name__)


class AbstractApplication(ApplicationLifeCycle):
    """
    This provides a simple class for implementing a distributed application.
    It connects to a given message bus and provides a services for handling messages.

    It provides a simple way to initialize ressources and handle sockets. It does not provide a concret implementation
    for how the service behaves. This is left up to subclasses (see `run_async` method).

    Attributes:
        async_callback: A callback which is called upon receiving a message.
        config: The configuration for the message bus. Can be any of the supported config types.
        ressources: A list of context managers which are entered and exited during the livecycle
        _queue_in (asyncio.Queue): A queue for receiving messages from the message bus.
    """

    def __init__(
        self,
        async_callback: Union[ProducerCallback, ConsumerCallback],
        socket_provider: SocketProvider,
        state: Any = None,
        ressources: dict[str, SupportedContextManagers] = {},
    ) -> None:
        self.exit_stack = AsyncExitStack()
        self.callback = async_callback
        self.socket_provider = socket_provider

        self.state = state
        self.ressources: dict[str, SupportedContextManagers] = ressources

    def setup(self):
        """
        Method for initializing non async components/ressources of the application.
        """
        logger.info(f"Starting '{self.callback.__name__}'")
        self.socket = self.socket_provider.create_socket(None)

    def teardown(self):
        """
        Method for cleanup non async components/ressources of the application.
        """
        logger.info(f"Halting '{self.callback.__name__}'")

    def start(self):
        """
        Start the asyncronous application. This method blocks until the application is stopped.
        """
        asyncio.run(self.start_async(), debug=True)

    async def start_async(self):
        """
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

            # run the logic for this application
            await self.run_async()

    async def run_async(self):
        """
        Contains the logic how the applicaiton behaves (e.g. endless loop, etc.)
        """
        raise NotImplementedError()


class ProducerApplication(AbstractApplication):
    async def run_async(self):
        """
        Starts the callback once and after it ends the producer is done.
        """
        await self.callback(socket=self.socket, state=self.state, **self.ressources)


class ConsumerApplication(AbstractApplication):
    def setup(self):
        super().setup()
        self._queue_in = asyncio.Queue()
        self.socket = self.socket_provider.create_socket(self._queue_in)

    async def run_async(self):
        """
        Repeats the callback on each received message
        """
        while True:
            message = await self._queue_in.get()
            await self.callback(message=message, socket=self.socket, state=self.state, **self.ressources)


class DistributedApplication:
    """
    This class provides a simple interface for creating distributed applications. It allows to create multiple
    applications which are connected to the same message bus. It does this by providing decorators for creating
    applications and producers.
    """

    def __init__(self, default_socket_provider: SocketProvider) -> None:
        self.applications = []
        self.default_socket_provider = default_socket_provider

    def add_application(
        self,
        async_consumer_callback: ConsumerCallback,
        socket_provider: SocketProvider,
        state: Any = None,
        ressources: dict[str, SupportedContextManagers] = {},
    ):
        self.applications.append(
            ConsumerApplication(
                async_callback=async_consumer_callback,
                socket_provider=socket_provider,
                state=state,
                ressources=ressources,
            )
        )

    def add_producer_application(
        self,
        async_producer_callback: ProducerCallback,
        socket_provider: SocketProvider,
        state: Any = None,
        ressources: dict[str, SupportedContextManagers] = {},
    ):
        self.applications.append(
            ProducerApplication(
                async_callback=async_producer_callback,
                socket_provider=socket_provider,
                state=state,
                ressources=ressources,
            )
        )

    def application(
        self,
        topics: list[str] | str,
        *,
        socket_provider: SocketProvider | None = None,
        state: Any = None,
        ressources: dict[str, SupportedContextManagers] = {},
    ):
        socket_provider = socket_provider or self.default_socket_provider
        if socket_provider is None:
            raise ValueError("No socket provider found. Either set the default in the constructor or here!")
        socket_provider = socket_provider.rebuild(topics=topics)

        def inner(callback: ConsumerCallback):
            self.add_application(callback, socket_provider, state, ressources)

        return inner

    def producer(
        self,
        *,
        socket_provider: SocketProvider | None = None,
        state: Any = None,
        ressources: dict[str, SupportedContextManagers] = {},
    ):
        socket_provider = socket_provider or self.default_socket_provider
        if socket_provider is None:
            raise ValueError("No socket provider found. Either set the default in the constructor or here!")
        socket_provider = socket_provider.rebuild(topics=None)

        def inner(callback: ProducerCallback):
            self.add_producer_application(callback, socket_provider, state, ressources)

        return inner

    def start(self):
        asyncio.run(self.start_async())

    async def start_async(self):
        await asyncio.gather(*[app.start_async() for app in self.applications])
