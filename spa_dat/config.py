from enum import Enum
from typing import Any

from pydantic import BaseModel


class PayloadFormat(Enum):
    JSON = "json"
    TOML = "toml"
    YAML = "yaml"
    PROTOBUF = "protobuf"
    SIDL = "sidl"


class SocketType(Enum):
    MQTT = "mqtt"
    KAFKA = "kafka"


class Configuration(BaseModel):
    payload_format: PayloadFormat
    socket_type: SocketType
    socket_options: dict[str, Any]
