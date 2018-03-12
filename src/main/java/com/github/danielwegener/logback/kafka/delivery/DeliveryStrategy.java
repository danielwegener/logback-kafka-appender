package com.github.danielwegener.logback.kafka.delivery;

import ch.qos.logback.core.spi.ContextAware;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

/**
 * Interface for DeliveryStrategies.
 *
 * @param <E> the type of the logging event.
 * @param <K> the key type of a persisted log message.
 * @param <V> the value type of a persisted log message.
 * @since 0.0.1
 */
public interface DeliveryStrategy<E, K, V> {
    /**
     * Initialize the strategy and create Kafka producer if needed
     * @param producerConfig Kafka producer config map
     * @param failedDeliveryCallback a callback that handles messages that could not be delivered with best-effort.
     */
    void start(Map<String, Object> producerConfig, FailedDeliveryCallback<E> failedDeliveryCallback);

    /**
     * Sends a message to a kafka producer and somehow deals with failures.
     *
     * @param record   the prepared kafka message (ready to ship)
     * @param event    the originating logging event
     */
    void send(ProducerRecord<K, V> record, E event);

    /**
     * Stop this strategy and close the Kafka producer if needed, freeing resources
     */
    void stop();
}
