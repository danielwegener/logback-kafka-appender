package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

/**
 * {@link DeliveryStrategy} which initializes the Kafka producer on first message
 * and sends messages asynchronously
 */
public class LateAsyncDeliveryStrategy<E,K,V> extends BaseDeliveryStrategy<E,K,V> {
    private volatile Producer<K, V> producer;

    public Producer<K, V> getProducer() {
        Producer<K, V> result = this.producer;
        if (result == null) {
            synchronized (this) {
                result = this.producer;
                if (result == null) {
                    try {
                        this.producer = result = this.createProducer();
                    } catch (KafkaException e) {
                        addWarn("Failed to create kafka producer: " + e.getMessage(), e);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void send(ProducerRecord<K, V> record, E event) {
        Producer<K, V> producer = getProducer();
        if (producer == null) {
            failedDeliveryCallback.onFailedDelivery(event, null);
            return;
        }
        doSend(record, event);
    }

    @Override
    public void stop() {
        stopProducer();
        producer = null;
    }

}
