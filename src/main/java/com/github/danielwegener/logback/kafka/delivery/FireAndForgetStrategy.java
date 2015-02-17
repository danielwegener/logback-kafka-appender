package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Daniel Wegener (Holisticon AG)
 */
public class FireAndForgetStrategy<E> implements DeliveryStrategy<E> {


    @Override
    public <K, V> boolean send(KafkaProducer<K, V> producer, ProducerRecord<K, V> record, E event) {
        producer.send(record);
        return true;
    }

    private final Callback producerCallback = new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {

        }
    };

}
