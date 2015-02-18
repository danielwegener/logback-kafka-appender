package com.github.danielwegener.logback.kafka.delivery;

import ch.qos.logback.core.spi.ContextAwareBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BlockingDeliveryStrategy extends ContextAwareBase implements DeliveryStrategy {

    private long timeout = 0L;

    @Override
    public <K, V, E> boolean send(KafkaProducer<K, V> producer, ProducerRecord<K, V> record, E event, FailedDeliveryCallback<E> failureCallback) {
        try {
            producer.send(record).get(timeout, TimeUnit.MILLISECONDS);
            return true;
        }
        catch (InterruptedException e) { return false; }
        catch (ExecutionException e)  { failureCallback.onFailedDelivery(event, e); }
        catch (TimeoutException e) { failureCallback.onFailedDelivery(event, e); }
        return false;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
