#logback-kafka-appender

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.danielwegener/logback-kafka-appender/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.logback-kafka-appender/logback-kafka-appender)
[![Build Status](https://api.travis-ci.org/logback-kafka-appender/logback-kafka-appender.svg)](https://travis-ci.org/logback-kafka-appender/logback-kafka-appender)
[![Coverage Status](https://img.shields.io/coveralls/logback-kafka-appender/logback-kafka-appender.svg)](https://coveralls.io/r/logback-kafka-appender/logback-kafka-appender)

This logback appender supports direct logging to apache kafka.


## Example
This is an example `logback.xml` that uses a common `PatternLayout` to encode a log message as a string.

Add `logback-kafka-appender` and `logback-classic` as libraray dependencies to your project (maven example).

```xml
[pom.xml]
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.1.2</version>
    <scope>provided</scope>
</dependency>
```


```xml
[src/main/resources/logback.xml]
<configuration>
   <appender name="kafkaAppender" class="com.github.danielwegener.logback.kafka.KafkaAppender">
       <encoder class="com.github.danielwegener.logback.kafka.encoding.PatternLayoutKafkaEncoder">
           <layout class="ch.qos.logback.classic.PatternLayout">
               <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
           </layout>
       </encoder>
       <topic>logs</topic>
       <producerConfig>bootstrap.servers=localhost:9092</producerConfig>
    </appender>
    <root level="info">
        <appender-ref ref="kafkaAppender" />
    </root>
</configuration>

```


## Full configuration example


## Delivery guarantee



## Keying strategy / Partitioning

Kafka's scalability and ordering guarantees heavily rely on the concepts of partitions ([more details here](TBD)).
For logging this means that we need to decide how we want to distribute our log messages over multiple kafka
topic partitions. One implication of this decision is how messages are ordered when they are consumed from a
arbitrary multi-partition consumer. Another implication is how evenly our log messages are distributed across all
 available partitions.

The order may or may not be important, depending on the inteded consumer-audience (e.g. a logstash indexer will reorder all message by its timestamp anyway).
The kafka producer client uses a messages key as partitioner. Thus `logback-kafka-appender` supports
the following partitioning strategies:


_TODO TABLE_


### Custom keying strategies

If none of the above partitioners satisfies your requirements, you mal also implement your own partitioner by implementing a custom `PartitioningStrategy`:

```java
package foo;
com.github.danielwegener.logback.kafka.partitioning.PartitioningStrategy;

public class MyPartitioningStrategy implements PartitioningStrategy {
    @Override
    public byte[] createKey(ILoggingEvent e) {
        return ByteBuffer.allocate(4).putInt(e.getLevel()).array();
    }
}
```

As most custom logback component, your custom partitioning strategy may implement the
`ch.qos.logback.core.spi.ContextAware` and `ch.qos.logback.core.spi.LifeCycle` interfaces.





## FAQ

- Q: I want to log to different/multiple topics!
- A: No problem, create an appender for each topic.


## License

This project is licensed under the [Apache License Version 2.0](LICENSE).

