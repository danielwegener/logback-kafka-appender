package com.github.danielwegener.logback.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;

/**
 * Lazy initializer for producer, patterned after commons-lang.
 *
 * @see <a href="https://commons.apache.org/proper/commons-lang/javadocs/api-3.4/org/apache/commons/lang3/concurrent/LazyInitializer.html">LazyInitializer</a>
 */
public class LazyProducerSupplier<K, V> extends ProducerSupplierBase<K, V> {
    private volatile Producer<K, V> producer;

    private Producer<K, V> initialize() {
        Producer<K, V> producer = null;
        try {
            producer = createProducer();
        } catch (Exception e) {
            contextAware.addError("error creating producer", e);
        }
        return producer;
    }

    @Override
    public Producer<K, V> get() {
        Producer<K, V> result = this.producer;
        if (result == null) {
            synchronized (this) {
                result = this.producer;
                if (result == null) {
                    this.producer = result = this.initialize();
                }
            }
        }

        return result;
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