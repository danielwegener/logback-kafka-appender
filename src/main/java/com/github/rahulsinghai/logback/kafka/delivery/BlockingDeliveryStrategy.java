package com.github.rahulsinghai.logback.kafka.delivery;

import ch.qos.logback.core.spi.ContextAwareBase;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * DeliveryStrategy that waits on the producer if the output buffer is full. The wait timeout is
 * configurable with {@link BlockingDeliveryStrategy#setTimeout(long)}
 *
 * @since 0.0.1
 * @deprecated Use {@link AsynchronousDeliveryStrategy} instead.
 */
@Deprecated
public class BlockingDeliveryStrategy extends ContextAwareBase implements DeliveryStrategy {

    private long timeout = 0L;

    @Override
    public <K, V, E> boolean send(Producer<K, V> producer, ProducerRecord<K, V> record, E event,
        FailedDeliveryCallback<E> failureCallback) {
        try {

            final Future<RecordMetadata> future = producer.send(record);
            if (timeout > 0L) {
                future.get(timeout, TimeUnit.MILLISECONDS);
            } else if (timeout == 0) {
                future.get();
            }
            return true;
        } catch (InterruptedException e) {
            return false;
        } catch (BufferExhaustedException | ExecutionException | CancellationException | TimeoutException e) {
            failureCallback.onFailedDelivery(event, e);
        }
        return false;
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
     *
     * @param timeout a timeout in {@link TimeUnit#MILLISECONDS}.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
