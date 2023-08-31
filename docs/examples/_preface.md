# Preface

The following examples use the freely available mqtt-dasbhoard broker. You should **not** use it for **production purposes**. 


## Local Development

The simplest way to test an `MQTT` implementation is to use an freely available broker like mqtt-dashboard.com. You do not need any credentials to connect to the broker. The examples use the `mqtt-dashboard.com` broker. If you want to host a broker for development purposes, you can use one of the following docker stacks:

=== "MQTT"
    Save this file as `docker-compose.yml` and start it with `docker-compose up`
    ```
    --8<-- "examples/mqtt/docker-compose.yml"
    ```

    Use this `mosquitto.conf`
    ```
    --8<-- "examples/mqtt/mosquitto.conf"
    ```

    Use tools like mqttui and mqtt-explorer to connect to the broker. The broker is available at `localhost:1883`.

=== "KAFKA"

    ```
    --8<-- "examples/kafka/docker-compose.yml"
    ```
    
    You can then connect to the ui at `http://localhost:8080` and create a new kafka cluster. The bootstrap server is available at `kafka1:29092`.