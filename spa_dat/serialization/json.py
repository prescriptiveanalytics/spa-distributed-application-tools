import json
import logging

from spa_dat.protocol.typedef import SpaMessage
from spa_dat.serialization.typedef import Serializer

logger = logging.getLogger(__name__)


class JsonSerializer(Serializer):
    def serialize(self, message: SpaMessage) -> bytes:
        return message.model_dump_json().encode("utf-8")

    def deserialize(self, message: bytes | None) -> SpaMessage:
        if message is None:
            return None

        try:
            return SpaMessage(**json.loads(message))
        except json.JSONDecodeError as e:
            logger.error(f"Could not parse message: {message} {e}")
            raise e
