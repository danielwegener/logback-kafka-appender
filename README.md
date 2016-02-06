#logback-kafka-appender

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.danielwegener/logback-kafka-appender/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.danielwegener/logback-kafka-appender)
[![Build Status](https://travis-ci.org/danielwegener/logback-kafka-appender.svg?branch=master)](https://travis-ci.org/danielwegener/logback-kafka-appender)
[![Coverage Status](https://img.shields.io/coveralls/danielwegener/logback-kafka-appender.svg)](https://coveralls.io/r/danielwegener/logback-kafka-appender)

This appender provides a way for applications to publish their application logs to Apache Kafka.
This is ideal for applications within immutable containers without a writable filesystem.

## Full configuration example

Add `logback-kafka-appender` and `logback-classic` as library dependencies to your project (maven example).

```xml
[pom.xml]
<dependency>
    <groupId>com.github.danielwegener</groupId>
    <artifactId>logback-kafka-appender</artifactId>
    <version>0.0.5</version>
</dependency>
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.1.2</version>
</dependency>
```
This is an example `logback.xml` that uses a common `PatternLayout` to encode a log message as a string.

```xml
[src/main/resources/logback.xml]
<configuration>

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- This is the kafkaAppender -->
    <appender name="kafkaAppender" class="com.github.danielwegener.logback.kafka.KafkaAppender">
            <!-- This is the default encoder that encodes every log message to an utf8-encoded string  -->
            <encoder class="com.github.danielwegener.logback.kafka.encoding.PatternLayoutKafkaMessageEncoder">
                <layout class="ch.qos.logback.classic.PatternLayout">
                    <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
                </layout>
            </encoder>
            <topic>logs</topic>
            <keyingStrategy class="com.github.danielwegener.logback.kafka.keying.RoundRobinKeyingStrategy" />
            <deliveryStrategy class="com.github.danielwegener.logback.kafka.delivery.AsynchronousDeliveryStrategy" />

            <!-- each <producerConfig> translates to regular kafka-client config (format: key=value) -->
            <!-- producer configs are documented here: https://kafka.apache.org/documentation.html#newproducerconfigs -->
            <!-- bootstrap.servers is the only mandatory producerConfig -->
            <producerConfig>bootstrap.servers=localhost:9092</producerConfig>

            <!-- this is the fallback appender if kafka is not available. -->
            <appender-ref ref="STDOUT">
        </appender>

    <root level="info">
        <appender-ref ref="kafkaAppender" />
    </root>
</configuration>

```

You may also look at the [complete configuration examples](src/example/resources/logback.xml)

### Delivery strategies

Direct logging over the network is not a trivial thing because it might be much less reliable than the local file system and has a much bigger impact on the application performance if the transport has hiccups.

You need make a essential decision: Is it more important to deliver all logs to the remote Kafka or is it more important to keep the application running smoothly? Either of this decisions allows you to tune this appender for throughput.

| Strategy   | Description  |
|---|---|
| `AsynchronousDeliveryStrategy` | Dispatches each log message to the `Kafka Producer`. If the delivery fails for some reasons, the message is dispatched to the fallback appenders. This DeliveryStrategy does block if the producers send buffer is full. To avoid even this blocking, enable the producerConfig `block.on.buffer.full=false`. All log messages that cannot be delivered fast enough will then go to the fallback appenders. |
| `BlockingDeliveryStrategy` | Blocks each calling thread until the log message is actually delivered. Normally this strategy is discouraged because it has a huge negative impact on throughput. __Warning: This strategy should not be used together with the producerConfig `linger.ms`__  |

#### Note on Broker outages

The `AsynchronousDeliveryStrategy` does not prevent you from being blocked by the Kafka metadata exchange. That means: If all brokers are not reachable when the logging context starts, or all brokers become unreachable for a longer time period (> `metadata.max.age.ms`), your appender will eventually block. This behavior is undesirable in general and may change with kafka-clients 0.9. Until then, you can wrap the KafkaAppender with logback's own AsyncAppender.

An example configuration could look like this:

```xml
<configuration>

    <!-- This is the kafkaAppender -->
    <appender name="kafkaAppender" class="com.github.danielwegener.logback.kafka.KafkaAppender">
    <!-- Kafka Appender configuration -->
    </appender>

    <appender name="ASYNC" class="ch.qos.logback.classic.AsyncAppender">
        <appender-ref ref="kafkaAppender" />
    </appender>

    <root level="info">
        <appender-ref ref="ASYNC" />
    </root>
</configuration>

```

#### Custom delivery strategies

You may also roll your own delivery strategy. Just extend `com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy`.

#### Fallback-Appender

If, for whatever reason, the kafka-producer decides that it cannot publish a log message, the message could still be logged to a fallback appender (a `ConsoleAppender` on STDOUT or STDERR would be a reasonable choice for that).

Just add your fallback appender(s) as logback `appender-ref` to the `KafkaAppender` section in your `logback.xml`. Every message that cannot be delivered to kafka will be written to _all_ defined `appender-ref`'s.

Example: `<appender-ref ref="STDOUT">` while `STDOUT` is an defined appender.

Note that the `AsynchronousDeliveryStrategy` will reuse the kafka producers io thread to write the message to the fallback appenders. Thus all fallback appenders should be reasonable fast so they does not slow down or break the kafka producer.


### Producer tuning

This appender uses the [kafka producer](https://kafka.apache.org/documentation.html#producerconfigs) introduced in kafka-0.8.2.
It uses the producer default configuration.

You may override any known kafka producer config with an `<producerConfig>Name=Value</producerConfig>` block (note that the `boostrap.servers` config is mandatory).
This allows a lot of fine tuning potential (eg. with `batch.size`, `compression.type` and `linger.ms`).

## Serialization

This module provides a `PatternLayoutKafkaEncoder` that works like a common `PatternLayoutEncoder`
(with the distinction that it creates byte-arrays instead of appending them to a synchronous `OutputStream`).

The `PatternLayoutKafkaEncoder` uses a regular `ch.qos.logback.core.Layout` as layout-parameter.

This allows you to use any layout that is capable of laying out an `ILoggingEvent` like a well-known `PatternLayout` or for example the
[logstash-logback-encoder's `LogstashLayout`](https://github.com/logstash/logstash-logback-encoder#usage).

### Custom Serialization

If you want to write something different than string on your kafka logging topic, you may roll your encoding mechanism. A use case would be to
to smaller message sizes and/or better serialization/deserialization performance on the producing or consuming side. Useful formats could be BSON, Avro or others.

Just roll your own `KafkaMessageEncoder`. The interface is quite simple:

```java
package com.github.danielwegener.logback.kafka.encoding;
public interface KafkaMessageEncoder<E> {
    byte[] doEncode(E loggingEvent);
}

```
Your encoder should be type-parameterized for any subtype of ILoggingEvent like in
```java
public class MyEncoder extends KafkaMessageEncoderBase<ILoggingEvent> { //...
```

You may also extend The `KafkaMessageEncoderBase` class that already implements the `ContextAware` and `Lifecycle` interfaces
and thus allows accessing the appender configuration and work with its lifecycle.


## Keying strategies / Partitioning

Kafka's scalability and ordering guarantees heavily rely on the concepts of partitions ([more details here](https://kafka.apache.org/082/documentation.html#introduction)).
For application logging this means that we need to decide how we want to distribute our log messages over multiple kafka
topic partitions. One implication of this decision is how messages are ordered when they are consumed from a
arbitrary multi-partition consumer since kafka only provides a guaranteed read order only on each single partition.
Another implication is how evenly our log messages are distributed across all available partitions and therefore balanced
between multiple brokers.

The order may or may not be important, depending on the intended consumer-audience (e.g. a logstash indexer will reorder all message by its timestamp anyway).
The kafka producer client uses a messages key as partitioner. Thus `logback-kafka-appender` supports the following partitioning strategies:

| Strategy   | Description  |
|---|---|
| `RoundRobinPartitioningStrategy` (default)   | Evenly distributes all written log messages over all available kafka partitions. This strategy may lead to unexpected read orders on clients.   |
| `HostNamePartitioningStrategy` | This strategy uses the HOSTNAME to partition the log messages to kafka. This is useful because it ensures that all log messages issued by this host will remain in the correct order for any consumer. But this strategy can lead to uneven log distribution for a small number of hosts (compared to the number of partitions). |
| `ContextNamePartitioningStrategy` |  This strategy uses logbacks CONTEXT_NAME to partition the log messages to kafka. This is ensures that all log messages logged by the same logging context will remain in the correct order for any consumer. But this strategy can lead to uneven log distribution for a small number of hosts (compared to the number of partitions).  |
| `ThreadNamePartitioningStrategy` |  This strategy uses the calling threads name as partitioning key. This ensures that all messages logged by the same thread will remain in the correct order for any consumer. But this strategy can lead to uneven log distribution for a small number of thread(-names) (compared to the number of partitions). |
| `LoggerNamePartitioningStrategy` | * This strategy uses the logger name as partitioning key. This ensures that all messages logged by the same logger will remain in the correct order for any consumer. But this strategy can lead to uneven log distribution for a small number of distinct loggers (compared to the number of partitions). |



### Custom keying strategies

If none of the above partitioners satisfies your requirements, you can easily implement your own partitioner by implementing a custom `KeyingStrategy`:

```java
package foo;
com.github.danielwegener.logback.kafka.keying.KeyingStrategy;

public class LevelKeyingStrategy implements KeyingStrategy {
    @Override
    public byte[] createKey(ILoggingEvent e) {
        return ByteBuffer.allocate(4).putInt(e.getLevel()).array();
    }
}
```

As most custom logback component, your custom partitioning strategy may implement the
`ch.qos.logback.core.spi.ContextAware` and `ch.qos.logback.core.spi.LifeCycle` interfaces.

A custom keying strategy may especially become handy when you want to use kafka's log compactation facility.

## FAQ

- __Q: I want to log to different/multiple topics!<br>__
  A: No problem, create an appender for each topic.

- __Q: I want my logs in the logstash json format!<br>__
  A: Use the [`LogstashLayout` from logstash-logback-encoder](https://github.com/logstash/logstash-logback-encoder#encoder)<br>
  Example:
```xml
<encoder class="com.github.danielwegener.logback.kafka.encoding.PatternLayoutKafkaMessageEncoder">
  <layout class="net.logstash.logback.layout.LogstashLayout" />
</encoder>
```


## License

This project is licensed under the [Apache License Version 2.0](LICENSE).

