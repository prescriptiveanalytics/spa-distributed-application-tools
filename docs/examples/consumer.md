# Consumer

A consumer is triggered by a message and can then do arbitary work. It can also publish messages using the `DistributedApplicationContext` provided inside the method call.

The following shows a minimal example of such a consumer.

```
--8<-- "examples/mqtt/01_simple_consumer.py"
```
