from enum import Enum
from typing import Union

from pydantic import BaseModel

from spa_dat.protocol.kafka import KafkaConfig
from spa_dat.protocol.mqtt import MqttConfig


class PayloadFormat(Enum):
    JSON = "json"
    TOML = "toml"
    YAML = "yaml"
    PROTOBUF = "protobuf"
    SIDL = "sidl"


SupportedSockets = Union[MqttConfig, KafkaConfig]


class SocketConfig(BaseModel):
    payload_format: PayloadFormat
    socket_config: SupportedSockets
