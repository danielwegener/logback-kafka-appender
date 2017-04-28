package com.github.danielwegener.logback.kafka.producer;

import ch.qos.logback.core.spi.ContextAware;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;

import java.util.HashMap;
import java.util.Map;

public class LazyProducerLifeCycleStrategy implements ProducerLifeCycleStrategy<byte[], byte[]> {

    private Map<String, Object> producerConfig;
    private LazyProducer lazyProducer;
    private boolean started;
    private ContextAware contextAware;

    public LazyProducerLifeCycleStrategy(ContextAware contextAware) {
        this.contextAware = contextAware;
    }

    public void setProducerConfig(Map<String, Object> config) {
        this.producerConfig = config;
    }

    public Map<String, Object> getProducerConfig() {
        return this.producerConfig;
    }

    public void setContextAware(ContextAware contextAware) {
        this.contextAware = contextAware;
    }

    @Override
    public Producer<byte[], byte[]> getProducer() {
        return lazyProducer.get();
    }

    @Override
    public void start() {
        if (contextAware == null) {
            return;
        }

        if (producerConfig == null) {
            contextAware.addError("producerConfig null");
            return;
        }

        lazyProducer = new LazyProducer();
        started = true;
    }

    @Override
    public void stop() {
        if (lazyProducer != null && lazyProducer.isInitialized()) {
            try {
                lazyProducer.get().close();
            } catch (KafkaException e) {
                contextAware.addWarn("Failed to shut down kafka producer: " + e.getMessage(), e);
            }
            lazyProducer = null;
        }
        started = false;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    /**
     * Lazy initializer for producer, patterned after commons-lang.
     *
     * @see <a href="https://commons.apache.org/proper/commons-lang/javadocs/api-3.4/org/apache/commons/lang3/concurrent/LazyInitializer.html">LazyInitializer</a>
     */
    private class LazyProducer {

        private volatile Producer<byte[], byte[]> producer;

        Producer<byte[], byte[]> get() {
            Producer<byte[], byte[]> result = this.producer;
            if (result == null) {
                synchronized(this) {
                    result = this.producer;
                    if(result == null) {
                        this.producer = result = this.initialize();
                    }
                }
            }

            return result;
        }

        Producer<byte[], byte[]> initialize() {
            Producer<byte[], byte[]> producer = null;
            try {
                producer = new KafkaProducer<byte[], byte[]>(new HashMap<String, Object>(producerConfig));
            } catch (Exception e) {
                contextAware.addError("error creating producer", e);
            }
            return producer;
        }

        boolean isInitialized() { return producer != null; }
    }
}
