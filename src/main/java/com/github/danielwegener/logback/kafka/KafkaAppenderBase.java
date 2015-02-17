package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.helpers.SubstituteLogger;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class KafkaAppenderBase<E extends ILoggingEvent> extends KafkaAppenderConfig<E> {

    public KafkaAppenderBase() {
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
     * This appender should never ever log any of its logs since it could cause harmful infinite recursion/self feeding effects.
     */
    private static final String KAFKA_LOGGER_PREFIX = "org.apache.kafka.clients";


    private KafkaProducer<byte[], byte[]> producer = null;


    @Override
    protected void append(E e) {
        final byte[] payload = encoder.doEncode(e);
        final byte[] key = partitioningStrategy.createKey(e);
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[],byte[]>(topic, key, payload);
        deliveryStrategy.send(producer,record, e);
    }


    @Override
    public void start() {
        // only error free appenders should be activated
        if (!checkPrerequisites()) return;

        final ByteArraySerializer serializer = new ByteArraySerializer();
        final BlindLogger blindLogger = new BlindLogger("blind",getStatusManager());

        replaceSubstituteLoggers(KafkaProducer.class, "log", blindLogger);
        replaceSubstituteLoggers(Selector.class, "log", blindLogger);

        producer = new  KafkaProducer<byte[], byte[]>(new HashMap<String, Object>(producerConfig), serializer, serializer);

        super.start();
    }

    private static void replaceSubstituteLoggers(Class<?> clazz, String field, Logger temporaryDelegate) {
        try {
            final Field log = clazz.getDeclaredField(field);
            log.setAccessible(true);
            final Object maybeSubstituteLogger = log.get(null);

            if (maybeSubstituteLogger instanceof SubstituteLogger) {
                final SubstituteLogger substituteLogger = (SubstituteLogger) maybeSubstituteLogger;
                substituteLogger.setDelegate(temporaryDelegate);
            }
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
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
