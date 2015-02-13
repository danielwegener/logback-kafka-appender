package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

public class BlockingSendStrategy<E> implements SendStrategy<E> {

    @Override
    public <K, V> boolean send(KafkaProducer<K, V> producer, ProducerRecord<K, V> record, E event) {
        try {
            producer.send(record).get();
            return true;
        }
        catch (InterruptedException e) { return false; }
        catch (ExecutionException e) { return false; }
    }
}
