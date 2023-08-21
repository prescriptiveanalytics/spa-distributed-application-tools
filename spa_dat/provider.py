from typing import Protocol

from spa_dat.config import Configuration
from spa_dat.protocol.mqtt import MqttConfig, MqttSocketProvider
from spa_dat.protocol.typedef import SocketProvider


class SocketProviderFactory(Protocol):
    """
    A service provider factory is a function which returns a service provider.
    """

    @staticmethod
    def from_config(config: Configuration) -> SocketProvider:
        # load necessary providers
        match config:
            case MqttConfig():
                return MqttSocketProvider(config)
            case _:
                raise NotImplementedError(f"Config type {type(config)} not supported")

    @staticmethod
    def from_url(url: str) -> SocketProvider:
        # 1. Parse url contents and contents to valid config
        # 2. Return socket via from_config
        raise NotImplementedError()
