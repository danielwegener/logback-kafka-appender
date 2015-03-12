package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Interface for DeliveryStrategies.
 */
public interface DeliveryStrategy {

    /**
     * Sends a message to a kafka producer and somehow deals with failures.
     *
     * @param producer the backing kafka producer
     * @param record the prepared kafka message (ready to ship)
     * @param event the originating logging event
     * @param failedDeliveryCallback a callback that handles messages that could not be delivered with best-effort.
     * @return {@code true} if the message could be sent successfully, {@code false} otherwise.
     */
    <K,V,E> boolean send(Producer<K,V> producer, ProducerRecord<K, V> record, E event, FailedDeliveryCallback<E> failedDeliveryCallback);

}
