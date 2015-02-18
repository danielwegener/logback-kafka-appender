package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsynchronousDeliveryStrategy implements DeliveryStrategy {

    @Override
    public <K, V, E> boolean send(KafkaProducer<K, V> producer, ProducerRecord<K, V> record, final E event,
                               final FailedDeliveryCallback<E> failedDeliveryCallback) {
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    failedDeliveryCallback.onFailedDelivery(event, exception);
                }
            }
        });
        return true;
    }

}
