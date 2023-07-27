import uuid

from pydantic.dataclasses import dataclass

@dataclass
class MqttConfig:
    host: str
    port: int
    topic: str
    keepalive: int = 60
    qos: int = 0
    retain = False
    username = None
    password = None
    client_id: str = str(uuid.uuid4())
