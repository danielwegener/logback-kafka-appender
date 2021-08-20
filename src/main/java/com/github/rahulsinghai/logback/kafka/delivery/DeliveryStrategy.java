package com.github.rahulsinghai.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Interface for DeliveryStrategies.
 *
 * @since 0.0.1
 */
public interface DeliveryStrategy {

    /**
     * Sends a message to a kafka producer and somehow deals with failures.
     *
     * @param producer               the backing kafka producer
     * @param record                 the prepared kafka message (ready to ship)
     * @param event                  the originating logging event
     * @param failedDeliveryCallback a callback that handles messages that could not be delivered
     *                               with best-effort.
     * @param <K>                    the key type of a persisted log message.
     * @param <V>                    the value type of a persisted log message.
     * @param <E>                    the type of the logging event.
     * @return {@code true} if the message could be sent successfully, {@code false} otherwise.
     */
    <K, V, E> boolean send(Producer<K, V> producer, ProducerRecord<K, V> record, E event,
        FailedDeliveryCallback<E> failedDeliveryCallback);

}
