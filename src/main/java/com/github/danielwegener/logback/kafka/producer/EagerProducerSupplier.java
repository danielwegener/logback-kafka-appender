package com.github.danielwegener.logback.kafka.producer;

import ch.qos.logback.core.spi.ContextAware;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;

import java.util.Map;

/**
 * Initializes Kafka Producer on Appender startup
 */
public class EagerProducerSupplier<K, V> extends ProducerSupplierBase<K, V> {
    private Producer<K, V> producer;

    @Override
    public void start(ContextAware contextAware, Map<String, Object> producerConfig) {
        super.start(contextAware, producerConfig);
        try {
            producer = createProducer();
        } catch (RuntimeException e) {
            contextAware.addError("error creating producer", e);
            throw e;
        }
    }

    @Override
    public Producer<K, V> get() {
        return  this.producer;
    }

    @Override
    public void stop() {
        if (producer != null) {
            try {
                producer.close();
            } catch (KafkaException e) {
                contextAware.addWarn("Failed to shut down kafka producer: " + e.getMessage(), e);
            }
            producer = null;
        }

    }

}