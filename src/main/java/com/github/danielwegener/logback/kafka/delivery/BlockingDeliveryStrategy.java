package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DeliveryStrategy that waits on the producer if the output buffer is full.
 * The wait timeout is configurable with {@link BlockingDeliveryStrategy#setTimeout(long)}
 * @since 0.0.1
 * @deprecated Use {@link BaseDeliveryStrategy} instead.
 */
@Deprecated
public class BlockingDeliveryStrategy<E, K, V> extends BaseDeliveryStrategy<E, K, V> {

    private long timeout = 0L;
    private Producer<K,V> producer;

    @Override
    public void start(Map<String, Object> producerConfig, FailedDeliveryCallback<E> failedDeliveryCallback) {
        super.start(producerConfig, failedDeliveryCallback);
        producer = createProducer();
    }

    @Override
    public void send(ProducerRecord<K, V> record, E event) {
        try {
            final Future<RecordMetadata> future = doSend(record, event);
            if (future == null) {
                return;
            }
                if (timeout > 0L) future.get(timeout, TimeUnit.MILLISECONDS);
                else if (timeout == 0) future.get();
        }
        catch (InterruptedException e) { }
        catch (ExecutionException | CancellationException | TimeoutException e) {
            failedDeliveryCallback.onFailedDelivery(event, e);
        }
    }

    @Override
    public Producer<K, V> getProducer() {
        return producer;
    }

    @Override
    public void stop() {
        stopProducer();
        producer = null;
    }

    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets the timeout for waits on full consumers.
     * <ul>
     *     <li>{@code timeout > 0}: Wait for {@code timeout} milliseconds</li>
     *     <li>{@code timeout == 0}: Wait infinitely
     * </ul>
     * @param timeout a timeout in {@link TimeUnit#MILLISECONDS}.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
