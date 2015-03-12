package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import ch.qos.logback.core.spi.FilterReply;
import com.github.danielwegener.logback.kafka.delivery.FailedDeliveryCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.helpers.SubstituteLogger;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;

public class KafkaAppenderBase<E extends ILoggingEvent> extends KafkaAppenderConfig<E> {

    protected Producer<byte[], byte[]> createProducer() {
        final ByteArraySerializer serializer = new ByteArraySerializer();
        return new KafkaProducer<byte[], byte[]>(new HashMap<String, Object>(producerConfig), serializer, serializer);
    }

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


    private final AppenderAttachableImpl<E> aai = new AppenderAttachableImpl<E>();
    private Producer<byte[], byte[]> producer = null;


    @Override
    protected void append(E e) {
        final byte[] payload = encoder.doEncode(e);
        final byte[] key = keyingStrategy.createKey(e);
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[],byte[]>(topic, key, payload);
        deliveryStrategy.send(producer,record, e, failedDeliveryCallback);
    }

    private final FailedDeliveryCallback<E> failedDeliveryCallback = new FailedDeliveryCallback<E>() {
        @Override
        public void onFailedDelivery(E evt, Throwable throwable) {
            aai.appendLoopOnAppenders(evt);
        }
    };


    @Override
    public void start() {
        // only error free appenders should be activated
        if (!checkPrerequisites()) return;


        producer = createProducer();

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

    @Override
    public void addAppender(Appender<E> newAppender) {
        aai.addAppender(newAppender);
    }

    @Override
    public Iterator<Appender<E>> iteratorForAppenders() {
        return aai.iteratorForAppenders();
    }

    @Override
    public Appender<E> getAppender(String name) {
        return aai.getAppender(name);
    }

    @Override
    public boolean isAttached(Appender<E> appender) {
        return aai.isAttached(appender);
    }

    @Override
    public void detachAndStopAllAppenders() {
        aai.detachAndStopAllAppenders();
    }

    @Override
    public boolean detachAppender(Appender<E> appender) {
        return aai.detachAppender(appender);
    }

    @Override
    public boolean detachAppender(String name) {
        return aai.detachAppender(name);
    }
}
