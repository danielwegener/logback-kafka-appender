package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

/**
 * {@link DeliveryStrategy} which initializes the Kafka producer on startup
 * and sends messages asynchronously
 */
public class EarlyAsyncDeliveryStrategy<E,K,V> extends BaseDeliveryStrategy<E,K,V> {
    private Producer<K, V> producer;

    @Override
    public void start(Map<String, Object> producerConfig, FailedDeliveryCallback<E> failedDeliveryCallback) {
        super.start(producerConfig, failedDeliveryCallback);
        producer = createProducer();
    }

    @Override
    public Producer<K, V> getProducer() {
        return producer;
    }

    @Override
    public void send(ProducerRecord<K, V> record, E event) {
        doSend(record, event);
    }

    @Override
    public void stop() {
        stopProducer();
        producer = null;
    }
}
