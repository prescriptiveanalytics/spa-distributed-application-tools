from typing import Protocol

from spa_dat.protocol.typedef import SpaMessage


class Serializer(Protocol):
    """
    A serializer is a class which is able to serialize and deserialize a given object.
    """

    def serialize(self, obj: SpaMessage) -> bytes:
        raise NotImplementedError()

    def deserialize(self, data: bytes) -> SpaMessage:
        raise NotImplementedError()
