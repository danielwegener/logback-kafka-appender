package com.github.danielwegener.logback.kafka.delivery;

import ch.qos.logback.core.spi.ContextAwareBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BlockingDeliveryStrategy<E> extends ContextAwareBase implements DeliveryStrategy<E> {

    private long timeout;

    @Override
    public <K, V> boolean send(KafkaProducer<K, V> producer, ProducerRecord<K, V> record, E event) {
        try {
            producer.send(record).get(timeout, TimeUnit.MILLISECONDS);
            return true;
        }
        catch (InterruptedException e) { return false; }
        catch (ExecutionException e)  { addError("Exception during send.", e); }
        catch (TimeoutException e) { addError("Timeout during send."); }
        return false;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
