package com.github.rahulsinghai.logback.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import com.github.rahulsinghai.logback.kafka.delivery.FailedDeliveryCallback;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * @since 0.0.1
 */
public class KafkaAppender<E> extends KafkaAppenderConfig<E> {

    /**
     * Kafka clients uses this prefix for its slf4j logging. This appender defers appends of any
     * Kafka logs since it could cause harmful infinite recursion/self feeding effects.
     */
    private static final String KAFKA_LOGGER_PREFIX = KafkaProducer.class.getPackage().getName()
        .replaceFirst("\\.producer$", "");
    private final AppenderAttachableImpl<E> aai = new AppenderAttachableImpl<E>();
    private final ConcurrentLinkedQueue<E> queue = new ConcurrentLinkedQueue<E>();
    private final FailedDeliveryCallback<E> failedDeliveryCallback = new FailedDeliveryCallback<E>() {
        @Override
        public void onFailedDelivery(E evt, Throwable throwable) {
            aai.appendLoopOnAppenders(evt);
        }
    };
    private LazyProducer lazyProducer = null;

    public KafkaAppender() {
        // setting these as config values sidesteps an unnecessary warning (minor bug in KafkaProducer)
        addProducerConfigValue(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName());
        addProducerConfigValue(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName());
    }

    @Override
    public void doAppend(E e) {
        ensureDeferredAppends();
        if (e instanceof ILoggingEvent && ((ILoggingEvent) e).getLoggerName()
            .startsWith(KAFKA_LOGGER_PREFIX)) {
            deferAppend(e);
        } else {
            super.doAppend(e);
        }
    }

    @Override
    public void start() {
        // only error free appenders should be activated
        if (!checkPrerequisites()) {
            return;
        }

        if (partition != null && partition < 0) {
            partition = null;
        }

        lazyProducer = new LazyProducer();

        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        if (lazyProducer != null && lazyProducer.isInitialized()) {
            try {
                lazyProducer.get().close();
            } catch (KafkaException e) {
                this.addWarn("Failed to shut down kafka producer: " + e.getMessage(), e);
            }
            lazyProducer = null;
        }
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

    @Override
    protected void append(E e) {
        final byte[] payload = encoder.encode(e);
        final byte[] key = keyingStrategy.createKey(e);

        final Long timestamp = isAppendTimestamp() ? getTimestamp(e) : null;

        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, partition,
            timestamp, key, payload);

        final Producer<byte[], byte[]> producer = lazyProducer.get();
        if (producer != null) {
            deliveryStrategy.send(lazyProducer.get(), record, e, failedDeliveryCallback);
        } else {
            failedDeliveryCallback.onFailedDelivery(e, null);
        }
    }

    protected Long getTimestamp(E e) {
        if (e instanceof ILoggingEvent) {
            return ((ILoggingEvent) e).getTimeStamp();
        } else {
            return System.currentTimeMillis();
        }
    }

    protected Producer<byte[], byte[]> createProducer() {
        return new KafkaProducer<>(new HashMap<>(producerConfig));
    }

    private void deferAppend(E event) {
        queue.add(event);
    }

    // drains queue events to super
    private void ensureDeferredAppends() {
        E event;

        while ((event = queue.poll()) != null) {
            super.doAppend(event);
        }
    }

    /**
     * Lazy initializer for producer, patterned after commons-lang.
     *
     * @see <a href="https://commons.apache.org/proper/commons-lang/javadocs/api-3.4/org/apache/commons/lang3/concurrent/LazyInitializer.html">LazyInitializer</a>
     */
    private class LazyProducer {

        private volatile Producer<byte[], byte[]> producer;

        public Producer<byte[], byte[]> get() {
            Producer<byte[], byte[]> result = this.producer;
            if (result == null) {
                synchronized (this) {
                    result = this.producer;
                    if (result == null) {
                        this.producer = result = this.initialize();
                    }
                }
            }

            return result;
        }

        protected Producer<byte[], byte[]> initialize() {
            Producer<byte[], byte[]> producer = null;
            try {
                producer = createProducer();
            } catch (Exception e) {
                addError("error creating producer", e);
            }
            return producer;
        }

        public boolean isInitialized() {
            return producer != null;
        }
    }

}
