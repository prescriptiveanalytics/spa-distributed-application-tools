# Producer

A producer does not receive any message but it can publishe messages and also take advantage of the request pattern.

!!! note

    The producer callback will be executed once. If you want to publish messages periodically you have to implement this logic inside the callback.

```
--8<-- "examples/mqtt/02_simple_producer.py"
```