import json
import logging
from typing import Protocol

from spa_dat.socket.typedef import SpaMessage

logger = logging.getLogger(__name__)


class Serializer(Protocol):
    """
    A serializer is a class which is able to serialize and deserialize a given object.
    """

    def serialize(self, obj: SpaMessage) -> bytes:
        raise NotImplementedError()

    def deserialize(self, data: bytes) -> SpaMessage:
        raise NotImplementedError()


class JsonSerializer(Serializer):
    def serialize(self, message: SpaMessage) -> bytes:
        return message.model_dump_json(by_alias=True).encode("utf-8")

    def deserialize(self, message: bytes | None) -> SpaMessage:
        if message is None:
            return None

        try:
            return SpaMessage(**json.loads(message))
        except json.JSONDecodeError as e:
            logger.error(f"Could not parse message: {message} {e}")
            raise e
