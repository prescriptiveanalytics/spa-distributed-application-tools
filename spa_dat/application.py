import asyncio
import logging
from typing import Protocol

from pydantic.dataclasses import dataclass

from spa_dat.config import MqttConfig
from spa_dat.protocol.spa import SpaProtocol
from spa_dat.service.mqtt import MqttService


logger = logging.getLogger(__name__)


@dataclass
class DistributedApplicationContext:
    message_service: MqttService


class ApplicationLifeCycle(Protocol):
    def startup(self):
        """
        Perform initialization tasks, such as setting up resources and dependencies
        """
        raise NotImplementedError()

    def run(self):
        """
        Execute your application - should block at this point
        """
        raise NotImplementedError()

    def shutdown(self):
        """
        Handle cleanup
        """
        raise NotImplementedError()


class DistributedApplication(ApplicationLifeCycle):
    """
    This provides a simple class for implementing a distributed application.
    It connects to a given message bus and provides a services for handling messages.
    It depends on a callback which is called upon receiving a message.
    It can act both as a consumer and a producer after receiving a message
    """

    message_service: SpaProtocol = None

    def startup(self):
        # TODO: INIT service
        pass

    def run(self):
        # start async loop
        pass

    def shutdown(self):
        # close async loop for service
        pass


class ProducerApplication(ApplicationLifeCycle):
    """
    This provides a simple class for implementing a distributed application.
    It provides a service for handling messages.
    It calls a callback directly, which can produce arbitary messages. After which the service stops.
    """

    message_service: SpaProtocol = None

    def __init__(self, callback, config: MqttConfig) -> None:
        super().__init__()
        # TODO: Build message service from config
        # ...
        self.callback = callback
        self.config = config
        self.queue_in = asyncio.Queue()

    def startup(self):
        logger.info("[Startup] Application")
        self.message_service = MqttService(self.config, self.queue_in)

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        try:
            self.startup()
            # subscribe to topics
            self.message_service.subscribe(self.message_service.config.topic)
            
            # read and handle messages
            while True:
                message = await self.queue_in.get()
                await self.callback(message, DistributedApplicationContext(self.message_service))
        finally:
            self.shutdown()

    def shutdown(self):
        logger.info("[Shutdown] Application")
