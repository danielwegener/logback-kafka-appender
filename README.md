# logback-kafka-appender

[![Join the chat at https://gitter.im/danielwegener/logback-kafka-appender](https://badges.gitter.im/danielwegener/logback-kafka-appender.svg)](https://gitter.im/danielwegener/logback-kafka-appender?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.danielwegener/logback-kafka-appender/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.danielwegener/logback-kafka-appender)
[![Build Status](https://travis-ci.org/danielwegener/logback-kafka-appender.svg?branch=master)](https://travis-ci.org/danielwegener/logback-kafka-appender)
[![Coverage Status](https://img.shields.io/coveralls/danielwegener/logback-kafka-appender.svg)](https://coveralls.io/r/danielwegener/logback-kafka-appender)

This appender provides a way for applications to publish their application logs to Apache Kafka.
This is ideal for applications within immutable containers without a writable filesystem.

## Logback incompatibility Warning 

__Due to a breaking change in the Logback Encoder API you need to use at least logback version 1.2.__

## Full configuration example

Add `logback-kafka-appender` and `logback-classic` as library dependencies to your project.

```xml
[maven pom.xml]
<dependency>
    <groupId>com.github.danielwegener</groupId>
    <artifactId>logback-kafka-appender</artifactId>
    <version>0.2.0</version>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.2.3</version>
    <scope>runtime</scope>
</dependency>
```

```scala
// [build.sbt]
libraryDependencies += "com.github.danielwegener" % "logback-kafka-appender" % "0.1.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
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
            <encoder>
                <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
            </encoder>
            <topic>logs</topic>
            <keyingStrategy class="com.github.danielwegener.logback.kafka.keying.RoundRobinKeyingStrategy" />
            <deliveryStrategy class="com.github.danielwegener.logback.kafka.delivery.AsynchronousDeliveryStrategy" />

            <!-- each <producerConfig> translates to regular kafka-client config (format: key=value) -->
            <!-- producer configs are documented here: https://kafka.apache.org/documentation.html#newproducerconfigs -->
            <!-- bootstrap.servers is the only mandatory producerConfig -->
            <producerConfig>bootstrap.servers=localhost:9092</producerConfig>

            <!-- this is the fallback appender if kafka is not available. -->
            <appender-ref ref="STDOUT" />
        </appender>

    <root level="info">
        <appender-ref ref="kafkaAppender" />
    </root>
</configuration>

```

You may also look at the [complete configuration examples](src/example/resources/logback.xml)

### Compatibility

logback-kafka-appender depends on `org.apache.kafka:kafka-clients:0.9.0.0:jar`. It can append logs to a kafka broker with version 0.9.0.0 or higher.

The dependency to kafka-clients is not shadowed and may be upgraded to a higher, binary compatible, version through dependency resolution.

### Delivery strategies

Direct logging over the network is not a trivial thing because it might be much less reliable than the local file system and has a much bigger impact on the application performance if the transport has hiccups.

You need make a essential decision: Is it more important to deliver all logs to the remote Kafka or is it more important to keep the application running smoothly? Either of this decisions allows you to tune this appender for throughput.

| Strategy   | Description  |
|---|---|
| `AsynchronousDeliveryStrategy` | Dispatches each log message to the `Kafka Producer`. If the delivery fails for some reasons, the message is dispatched to the fallback appenders. However, this DeliveryStrategy _does_ block if the producers send buffer is full (this can happen if the connection to the broker gets lost). To avoid even this blocking, enable the producerConfig `block.on.buffer.full=false`. All log messages that cannot be delivered fast enough will then immediately go to the fallback appenders. |
| `BlockingDeliveryStrategy` | Blocks each calling thread until the log message is actually delivered. Normally this strategy is discouraged because it has a huge negative impact on throughput. __Warning: This strategy should not be used together with the producerConfig `linger.ms`__  |

#### Note on Broker outages

The `AsynchronousDeliveryStrategy` does not prevent you from being blocked by the Kafka metadata exchange. That means: If all brokers are not reachable when the logging context starts, or all brokers become unreachable for a longer time period (> `metadata.max.age.ms`), your appender will eventually block. This behavior is undesirable in general and can be migitated with kafka-clients 0.9 (see #16). Until then, you can wrap the KafkaAppender with logback's own AsyncAppender.

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

Note that the `AsynchronousDeliveryStrategy` will reuse the kafka producers io thread to write the message to the fallback appenders. Thus all fallback appenders should be reasonable fast so they do not slow down or break the kafka producer.


### Producer tuning

This appender uses the [kafka producer](https://kafka.apache.org/documentation.html#producerconfigs) introduced in kafka-0.8.2.
It uses the producer default configuration.

You may override any known kafka producer config with an `<producerConfig>Name=Value</producerConfig>` block (note that the `boostrap.servers` config is mandatory).
This allows a lot of fine tuning potential (eg. with `batch.size`, `compression.type` and `linger.ms`).

## Serialization

This module supports any `ch.qos.logback.core.encoder.Encoder`. This allows you to use any encoder  that is capable of encoding an `ILoggingEvent` or `IAccessEvent` like the well-known 
[logback `PatternLayoutEncoder`](https://logback.qos.ch/manual/encoders.html#PatternLayoutEncoder) or for example the 
[logstash-logback-encoder's `LogstashEncoxer`](https://github.com/logstash/logstash-logback-encoder#usage).

### Custom Serialization

If you want to write something different than string on your kafka logging topic, you may roll your encoding mechanism. A use case would be to
to smaller message sizes and/or better serialization/deserialization performance on the producing or consuming side. Useful formats could be BSON, Avro or others.

To roll your own implementation please refer to the [logback documentation](https://logback.qos.ch/xref/ch/qos/logback/core/encoder/Encoder.html).
Note that logback-kafka-appender will _never_ call the `headerBytes()` or `footerBytes()` method.

Your encoder should be type-parameterized for any subtype of the type of event you want to support (typically `ILoggingEvent`) like in

```java
public class MyEncoder extends ch.qos.logback.core.encoder.Encoder<ILoggingEvent> {/*..*/}
```

## Keying strategies / Partitioning

Kafka's scalability and ordering guarantees heavily rely on the concepts of partitions ([more details here](https://kafka.apache.org/082/documentation.html#introduction)).
For application logging this means that we need to decide how we want to distribute our log messages over multiple kafka
topic partitions. One implication of this decision is how messages are ordered when they are consumed from a
arbitrary multi-partition consumer since kafka only provides a guaranteed read order only on each single partition.
Another implication is how evenly our log messages are distributed across all available partitions and therefore balanced
between multiple brokers.

The order of log messages may or may not be important, depending on the intended consumer-audience (e.g. a logstash indexer will reorder all message by its timestamp anyway).
The kafka producer client uses a message key as partitioner. Thus `logback-kafka-appender` supports the following partitioning strategies:

| Strategy   | Description  |
|---|---|
| `RoundRobinKeyingStrategy` (default)   | Evenly distributes all written log messages over all available kafka partitions. This strategy may lead to unexpected read orders on clients.   |
| `HostNameKeyingStrategy` | This strategy uses the HOSTNAME to partition the log messages to kafka. This is useful because it ensures that all log messages issued by this host will remain in the correct order for any consumer. But this strategy can lead to uneven log distribution for a small number of hosts (compared to the number of partitions). |
| `ContextNameKeyingStrategy` |  This strategy uses logbacks CONTEXT_NAME to partition the log messages to kafka. This is ensures that all log messages logged by the same logging context will remain in the correct order for any consumer. But this strategy can lead to uneven log distribution for a small number of hosts (compared to the number of partitions). This strategy only works for `ILoggingEvents`. |
| `ThreadNameKeyingStrategy` |  This strategy uses the calling threads name as partitioning key. This ensures that all messages logged by the same thread will remain in the correct order for any consumer. But this strategy can lead to uneven log distribution for a small number of thread(-names) (compared to the number of partitions). This strategy only works for `ILoggingEvents`. |
| `LoggerNameKeyingStrategy` | * This strategy uses the logger name as partitioning key. This ensures that all messages logged by the same logger will remain in the correct order for any consumer. But this strategy can lead to uneven log distribution for a small number of distinct loggers (compared to the number of partitions). This strategy only works for `ILoggingEvents`. |



### Custom keying strategies

If none of the above partitioners satisfies your requirements, you can easily implement your own partitioner by implementing a custom `KeyingStrategy`:

```java
package foo;
import com.github.danielwegener.logback.kafka.keying.KeyingStrategy;

public class LevelKeyingStrategy implements KeyingStrategy<ILoggingEvent> {
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

## License

This project is licensed under the [Apache License Version 2.0](LICENSE).

