package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Daniel Wegener (Holisticon AG)
 */
public class FireAndForgetStrategy<E> implements DeliveryStrategy<E> {

    @Override
    public <K, V> boolean send(KafkaProducer<K, V> producer, ProducerRecord<K, V> record, E event) {
        producer.send(record);
        return true;
    }
}
