from typing import Protocol

class MessageBusProtocol(Protocol):
    def publish():
        raise NotImplementedError()
    def publish_async():
        raise NotImplementedError()

    def subscribe():
        raise NotImplementedError()
    def subscribe_async():
        raise NotImplementedError()

    def unsubscribe():
        raise NotImplementedError()
    def unsubscribe_aysnc():
        raise NotImplementedError()

    def request():
        raise NotImplementedError()
    def request_async():
        raise NotImplementedError()
    