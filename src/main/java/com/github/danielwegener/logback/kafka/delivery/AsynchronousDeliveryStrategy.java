package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;

/**
 * @since 0.0.1
 */
public class AsynchronousDeliveryStrategy implements DeliveryStrategy {

    @Override
    public <K, V, E> boolean send(Producer<K, V> producer, ProducerRecord<K, V> record, final E event,
                                  final FailedDeliveryCallback<E> failedDeliveryCallback) {
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    failedDeliveryCallback.onFailedDelivery(event, exception);
                }
            });
            return true;
        } catch (Exception e) {
          if (e instanceof org.apache.kafka.common.errors.InterruptException) {
            Thread.currentThread().interrupt();
          }
            failedDeliveryCallback.onFailedDelivery(event, e);
            return false;
        }
    }

}
