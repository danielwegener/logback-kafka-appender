package com.github.danielwegener.logback.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.Map;

public class StrictProducerLifeCycleStrategy implements ProducerLifeCycleStrategy<byte[], byte[]> {

    private Map<String, Object> producerConfig;
    private Producer<byte[], byte[]> producer;
    private volatile boolean started;

    @Override
    public void setProducerConfig(Map<String, Object> producerConfig) {
        this.producerConfig = producerConfig;
    }

    @Override
    public Producer<byte[], byte[]> getProducer() {
        return producer;
    }

    @Override
    public void start() {
        if (!isStarted()) {
            synchronized(this) {
                if (!isStarted()) {
                    producer = new KafkaProducer<byte[], byte[]>(new HashMap<String, Object>(producerConfig));
                    started = true;
                }
            }
        }
    }

    @Override
    public void stop() {
        if (isStarted()) {
            synchronized (this) {
                if (isStarted()) {
                    producer.close();
                    producer = null;
                    started = false;
                }
            }
        }
    }

    @Override
    public boolean isStarted() {
        return started;
    }
}
