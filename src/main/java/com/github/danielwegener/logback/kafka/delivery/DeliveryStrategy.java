package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface DeliveryStrategy {

    <K,V,E> boolean send(Producer<K,V> producer, ProducerRecord<K, V> record, E event, FailedDeliveryCallback<E> failedDeliveryCallback);

}
