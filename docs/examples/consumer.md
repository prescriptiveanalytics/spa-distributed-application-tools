# Consumer

A consumer is triggered by a message and can then do arbitary work. It can also publish messages using the `DistributedApplicationContext` provided inside the method call.

The following shows a minimal example of such a consumer.

=== "MQTT"

    ```
    --8<-- "examples/mqtt/01_consumer.py"
    ```

=== "KAFKA"

    ```
    --8<-- "examples/kafka/01_consumer.py"
    ```
