from spa_dat.config import PayloadFormat, SocketConfig
from spa_dat.socket.kafka import KafkaConfig, KafkaSocketProvider
from spa_dat.socket.mqtt import MqttConfig, MqttSocketProvider
from spa_dat.socket.typedef import SocketProvider
from spa_dat.serialization import Serializer, JsonSerializer


class SocketProviderFactory:
    """
    The factory builds the socket provider based upon the SpaConfiguration.
    """

    @staticmethod
    def _get_payload_serializer(config: SocketConfig) -> Serializer:
        match config.payload_format:
            case PayloadFormat.JSON:
                return JsonSerializer()
            case _:
                raise NotImplementedError(f"Payload type {config.payload_format} not supported")

    @staticmethod
    def _get_socket_provider(config: SocketConfig, serializer: Serializer) -> SocketProvider:
        match config.socket_config:
            case MqttConfig():
                return MqttSocketProvider(config.socket_config, serializer)
            case KafkaConfig():
                return KafkaSocketProvider(config.socket_config, serializer)
            case _:
                raise NotImplementedError(f"Config type {type(config.socket_config)} not supported")

    @staticmethod
    def from_config(config: SocketConfig) -> SocketProvider:
        return SocketProviderFactory._get_socket_provider(config, SocketProviderFactory._get_payload_serializer(config))

    @staticmethod
    def from_url(url: str) -> SocketProvider:
        # 1. Parse url contents and contents to valid config
        # 2. Return socket via from_config
        raise NotImplementedError()
