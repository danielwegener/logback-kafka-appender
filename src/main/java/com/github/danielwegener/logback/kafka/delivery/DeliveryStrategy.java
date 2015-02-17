package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface DeliveryStrategy<E> {

    <K,V> boolean send(KafkaProducer<K,V> producer, ProducerRecord<K, V> record, E event);

}
