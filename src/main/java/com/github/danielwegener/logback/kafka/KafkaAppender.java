package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaAppender<E extends ILoggingEvent> extends KafkaAppenderConfig<E, byte[]> {

    public KafkaAppender() {
        addFilter(new Filter<E>() {
            @Override
            public FilterReply decide(E event) {
                if (event.getLoggerName().startsWith(KAFKA_LOGGER_PREFIX)) {
                    return FilterReply.DENY;
                }
                return FilterReply.NEUTRAL;
            }
        });
    }

    /**
     * Kafka clients uses this prefix for its slf4j logging.
     * This appender should never ever log any of its logs since it could cause harmful infinite recursion.
     */
    private static final String KAFKA_LOGGER_PREFIX = "org.apache.kafka.clients";


    private KafkaProducer<byte[], byte[]> producer = null;


    @Override
    protected void append(E e) {
        final String message = layout.doLayout(e);
        final byte[] payload;
        if (!message.isEmpty()) {
            payload = message.getBytes(charset);
        } else {
            payload = new byte[0];
        }
        final byte[] key = partitioningStrategy.createKey(e);
        producer.partitionsFor(topic);
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[],byte[]>(topic, key, payload);
        sendStrategy.send(producer,record, e);
    }




    @Override
    public void start() {
        // only error free appenders should be activated
        if (!checkPrerequisites()) return;

        // Copy any valid kafka producer config from the context properties if not explicitly configured yet
        for (Map.Entry<String, Object> entry : producerConfig.entrySet()) {
            final String key = entry.getKey();
            if (entry.getValue() == null) {
                entry.setValue(getContext().getProperty(key));
            }
        }

        final ByteArraySerializer serializer = new ByteArraySerializer();
        producer = new  KafkaProducer<byte[], byte[]>(new HashMap<String, Object>(producerConfig), serializer, serializer);

        super.start();
    }

    @Override
    public void stop() {
        if (producer != null) {
            try {
                producer.close();
            } catch (KafkaException e) {
                this.addWarn("Failed to shut down kafka producer: " + e.getMessage(), e);
            }
            producer = null;
        }
        super.stop();
    }

    @Override
    public boolean isStarted() {
        return super.isStarted();
    }


}
