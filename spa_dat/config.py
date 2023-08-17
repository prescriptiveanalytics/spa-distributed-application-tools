import uuid

from pydantic.dataclasses import dataclass


@dataclass
class MqttConfig:
    host: str
    port: int
    default_subscription_topic: str | None = None  # if set to none no subscription will be made
    keepalive: int = 60
    qos: int = 0
    retain = False
    username = None
    password = None
    client_id: str = str(uuid.uuid4())
