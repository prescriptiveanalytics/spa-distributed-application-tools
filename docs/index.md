# In a Nutshell

!!! info Secure Prescriptive Analytics Project

    This project is part of the Secure Prescriptive Analytics Project. For more information visit the [project website](https://www.prescriptiveanalytics.at/).

This package provides a wrapper for the message based communication of distributed applications. It can be used to implement a request-response pattern or a publish-subscribe pattern. It currently supports the following brokers:

  - MQTT protocol. 
  
It accomplishes this by providing a common interface for the message bus systems and abstracting the protocol for each broker. The package is designed for take advantage of the `asnycio` library and the `async/await` syntax.

### Configuration

Configs can be provided programmatically or loaded from an URL. To programmatically provide a config use the `SocketProviderFactory.from_config` method. To load a config from an URL use the `SocketProviderFactory.from_url` method. The following example shows available configurations for each broker:

::: spa_dat.protocol.mqtt.MqttConfig
    :showcode:
    :language: python
    :caption: MQTT Config
    :name: mqtt_config

For more detailed examples see the [examples](#examples) section.

### Interaction Patterns

The library abstracts from the specific broker and provides a common interface for the following interaction patterns:

- Subscribe
- Publish
- Request-Response

#### Distributed Application Context

The `DistributedApplicationContext` is a wrapper for the `SocketProvider` and provides a common interface for the interaction patterns. It is used to publish messages and to register consumers. It is currently defined as follows:


::: spa_dat.application.DistributedApplicationContext

and provides the following interface:

::: spa_dat.protocol.typedef.SpaSocket

!!! info Note

    For more information see the examples in the [examples](examples/consumer/) section.
    