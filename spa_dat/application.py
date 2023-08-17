import asyncio
import logging
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from typing import Callable, Protocol

from pydantic.dataclasses import dataclass

from spa_dat.config import MqttConfig
from spa_dat.protocol.spa import SpaMessage, SpaProtocol
from spa_dat.service.mqtt import MqttService

logger = logging.getLogger(__name__)


@dataclass
class DistributedApplicationContext:
    message_service: MqttService


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


class ProducerApplication(ApplicationLifeCycle):
    """
    This provides a simple class for implementing a distributed application.
    It connects to a given message bus and provides a services for handling messages.
    It acts only as a producer by default

    Attributes:
        async_callback: A callback which is called upon receiving a message.
        config: The configuration for the message bus. Can be any of the supported config types.
        ressources (list[AbstractAsyncContextManager]): A list of context managers which are entered and exited.
        This adds support for async ressources.
        _queue_in (asyncio.Queue): A queue for receiving messages from the message bus.
    """

    def __init__(self, async_callback: ProducerCallback, config: MqttConfig) -> None:
        self.config = config
        self.exit_stack = AsyncExitStack()
        self.callback = async_callback

        self.ressources: list[AbstractAsyncContextManager] = []

    def setup(self):
        """
        Method for initializing non async components/ressources of the application.
        """
        logger.info("[Startup] Application")
        self.message_service = MqttService(self.config, None)

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
            await self.exit_stack.enter_async_context(self.message_service)

            # enter dynamic context
            for ressource in self.ressources:
                await self.exit_stack.enter_async_context(ressource)

            # shut down after leaving context
            self.exit_stack.callback(self.teardown)

            await self.callback(DistributedApplicationContext(self.message_service))

    def teardown(self):
        # close async loop for service
        pass


class DistributedApplication(ApplicationLifeCycle):
    """
    This provides a simple class for implementing a distributed application.
    It provides a service for handling messages.
    It calls a callback directly, which can produce arbitary messages. After which the service stops.

    Attributes:
        async_callback: A callback which is called upon receiving a message.
        config: The configuration for the message bus. Can be any of the supported config types.
        ressources (list[AbstractAsyncContextManager]): A list of context managers which are entered and exited.
        This adds support for async ressources.
        _queue_in (asyncio.Queue): A queue for receiving messages from the message bus.
    """

    message_service: SpaProtocol = None

    def __init__(self, async_callback, config: MqttConfig) -> None:
        super().__init__()
        # TODO: Build message service from config
        # ...
        self.callback = async_callback
        self.config = config
        self._queue_in = asyncio.Queue()
        self.exit_stack = AsyncExitStack()

        self.ressources: list[AbstractAsyncContextManager] = []

    def setup(self):
        """
        Method for initializing non async components/ressources of the application.
        """
        logger.info("[Startup] Application")
        self.message_service = MqttService(self.config, self._queue_in)

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
            await self.exit_stack.enter_async_context(self.message_service)

            # enter dynamic context
            for ressource in self.ressources:
                await self.exit_stack.enter_async_context(ressource)

            # shut down after leaving context
            self.exit_stack.callback(self.teardown)

            # subscribe to default topic
            if self.config.default_subscription_topic is not None:
                await self.message_service.subscribe(self.config.default_subscription_topic)

            # read and handle messages
            while True:
                message = await self._queue_in.get()
                await self.callback(message, DistributedApplicationContext(self.message_service))

    def teardown(self):
        """
        Method for cleanup non async components/ressources of the application.
        """
        logger.info("[Shutdown] Application")
