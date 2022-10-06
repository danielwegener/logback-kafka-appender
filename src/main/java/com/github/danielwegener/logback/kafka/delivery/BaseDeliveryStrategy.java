package com.github.danielwegener.logback.kafka.delivery;

import ch.qos.logback.core.spi.ContextAwareBase;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Base implementation of {@link DeliveryStrategy}
 * @since 0.0.1
 */
public abstract class BaseDeliveryStrategy<E, K, V> extends ContextAwareBase implements DeliveryStrategy<E, K, V> {
    private HashMap<String, Object> producerConfig;
    protected FailedDeliveryCallback<E> failedDeliveryCallback;

    @Override
    public void start(Map<String, Object> producerConfig, FailedDeliveryCallback<E> failedDeliveryCallback) {
        this.producerConfig = new HashMap<>(producerConfig);
        this.failedDeliveryCallback = failedDeliveryCallback;
    }

    /**
     * Instantiates the producer with its config
     * @return Created producer
     * @throws KafkaException Producer initialization failed
     */
    protected Producer<K, V> createProducer() throws KafkaException{
        return new KafkaProducer<>(new HashMap<>(producerConfig));
    }

    /**
     * Get producer
     * @return Producer or null if producer is not initiliazed
     */
    protected abstract Producer<K, V> getProducer();
    /**
     * Stop the producer
     */
    public void stopProducer() {
        if (getProducer() != null) {
            try {
                getProducer().close();
            } catch (KafkaException e) {
                addWarn("Failed to shut down kafka producer: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Send a record using the provider.
     * In case of failure, the event is sent to the {@link #failedDeliveryCallback}
     * @param record Record to send
     * @param event Event to send to failedDeliveryCallback in case of failure
     * @return Future returned by the Kafka producer
     * @throws KafkaException Record sending failed
     */
    protected Future<RecordMetadata> doSend(ProducerRecord<K, V> record, final E event) throws KafkaException {
        try {
            return getProducer().send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        failedDeliveryCallback.onFailedDelivery(event, exception);
                    }
                }
            });
        } catch (BufferExhaustedException | TimeoutException e) {
            failedDeliveryCallback.onFailedDelivery(event, e);
            return null;
        }
    }

}
