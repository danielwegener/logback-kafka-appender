package com.github.danielwegener.logback.kafka.producer;

import ch.qos.logback.core.spi.ContextAware;
import org.apache.kafka.clients.producer.Producer;

import java.util.Map;

/**
 * Strategy to initialize the {@link org.apache.kafka.clients.producer.KafkaProducer} instance
 */
public interface ProducerSupplier<K, V> {
    /**
     * Initialize and start this supplier
     * @param contextAware Context wrapper
     * @param producerConfig Kafka Producer config
     */
    void start(ContextAware contextAware, Map<String, Object> producerConfig) ;

    /**
     * Get Kafka Producer
     * @return Kafka Producer
     */
    Producer<K, V> get();

    /**
     * Stop this supplier and free resources (close the Kafka Producer)
     */
    void stop();
}
