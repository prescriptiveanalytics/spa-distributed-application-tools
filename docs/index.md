# In a Nutshell

!!! info Secure Prescriptive Analytics Project

    This project is part of the Secure Prescriptive Analytics Project. For more information visit the [project website](https://www.prescriptiveanalytics.at/).

This package provides a wrapper for the message based communication of distributed applications. It can be used to implement a request-response pattern or a publish-subscribe pattern. It currently supports the following brokers:

  - MQTT 
  - Kafka 
  
It accomplishes this by providing a common interface for the message bus systems and abstracting the protocol for each broker. The package is designed for take advantage of the `asnycio` library and the `async/await` syntax.

## Building an Application

!!! info Note

    For more information see the examples in the [examples](examples/consumer.md) section.

### Configuration

Configs can be provided programmatically or loaded from an URL. To programmatically provide a config use the `SocketProviderFactory.from_config` method. To load a config from an URL use the `SocketProviderFactory.from_url` method. The following example shows available configurations for each broker:

::: spa_dat.socket.mqtt.MqttConfig
    options:
        show_root_toc_entry: false

::: spa_dat.socket.kafka.KafkaConfig
    options:
        show_root_toc_entry: false

### Defining a Producer or Consumer

Can be accomplished via `decorators`. Currently two decorators are supported: `producer` and `application`. The are implemented in the `DistributionApplication` class.

::: spa_dat.application.application.DistributedApplication
    options:
        show_root_toc_entry: false
        show_source: false
        heading_level: 6
        members:
            - producer
            - application

### Application Context and State

The application context and state are injected into the callback functions. The state is used to store the state of the application and can be user defined. An example of the state can be viewed [in the examples](examples/state.md). The `SocketProvider` provides a common interface for the interaction patterns. It is used to to ineract with other applications.  

### Ressource Handling

All ressources (e.g. database connections, ...) are handled via `ContextManager`. The application supports the following types:

::: spa_dat.application.typedef
    options:
        show_root_toc_entry: false
        heading_level: 7
        members:
            - SupportedContextManagers
    

To use a Contextmanager, it is added the ressource dict in the decorator function. Context managers are automatically started and cleaned up after the application shuts down.

### Supported Interaction Patterns

::: spa_dat.socket.typedef.SpaSocket
    options:
        show_root_toc_entry: false

It is implemented for each broker. 
