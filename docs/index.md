# Documentation

## In a Nutshell

!!! info Secure Prescriptive Analytics Project

    This project is part of the Secure Prescriptive Analytics Project. For more information visit the [project website](https://www.prescriptiveanalytics.at/).

This package provides a wrapper for the message based communication of distributed applications. It can be used to implement a request-response pattern or a publish-subscribe pattern. It currently supports the following brokers:

  - MQTT protocol. 
  
It accomplishes this by providing a common interface for the message bus systems and abstracting the protocol for each broker. The package is designed for take advantage of the `asnycio` library and the `async/await` syntax.

## Examples

> The following examples use the freely available mqtt-dasbhoard broker. You should not use it for production purposes.

### Consumer

```
--8<-- "examples/mqtt/01_simple_consumer.py"
```

### Producer

```
--8<-- "examples/mqtt/02_simple_producer.py"
```

### Request

```
--8<-- "examples/mqtt/03_request_example.py"
```

Fill your docs!