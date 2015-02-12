package com.github.danielwegener.logback.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaProducerTest {

    @Test
    public void testI() throws ExecutionException, InterruptedException {
        final Map<String,Object> props = new HashMap<String,Object>();

        props.put("bootstrap.servers", "localhost:9092");

        KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer());

        kafkaProducer.send(new ProducerRecord<byte[], byte[]>("logs", null, "HI FROM TEST".getBytes())).get();


    }

}
