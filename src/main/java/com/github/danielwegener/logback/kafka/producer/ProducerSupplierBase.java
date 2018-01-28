package com.github.danielwegener.logback.kafka.producer;

import ch.qos.logback.core.spi.ContextAware;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.Map;

/**
 * Lazy initializer for producer, patterned after commons-lang.
 *
 * @see <a href="https://commons.apache.org/proper/commons-lang/javadocs/api-3.4/org/apache/commons/lang3/concurrent/LazyInitializer.html">LazyInitializer</a>
 */
abstract class ProducerSupplierBase<K, V> implements ProducerSupplier<K, V> {
    protected ContextAware contextAware;
    protected HashMap<String, Object> producerConfig;

    @Override
    public void start(ContextAware contextAware, Map<String, Object> producerConfig) {
        this.contextAware = contextAware;
        this.producerConfig = new HashMap<>(producerConfig);
    }

    protected Producer<K, V> createProducer() {
        return new KafkaProducer<>(new HashMap<>(producerConfig));
    }

}