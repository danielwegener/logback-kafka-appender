package com.github.danielwegener.logback.kafka.producer;

import ch.qos.logback.core.spi.LifeCycle;
import org.apache.kafka.clients.producer.Producer;

import java.util.Map;

public interface ProducerLifeCycleStrategy<K, V> extends LifeCycle {

    void setProducerConfig(Map<String, Object> producerConfig);

    Producer<K, V> getProducer();

}
