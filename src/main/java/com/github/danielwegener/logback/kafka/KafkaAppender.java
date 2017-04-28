package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.github.danielwegener.logback.kafka.producer.LazyProducerLifeCycleStrategy;
import com.github.danielwegener.logback.kafka.producer.ProducerLifeCycleStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @since 0.0.1
 */
public class KafkaAppender extends KafkaAppenderBase<ILoggingEvent> {

    /**
     * Kafka clients use these prefixes for slf4j logging.
     * This appender defers appends of any Kafka logs since it could cause harmful infinite recursion/self feeding
     * effects.
     */
    private static final String[] KAFKA_LOGGER_PREFIXES = new String[]{
            "org.apache.kafka.clients",
            "org.apache.kafka.common.metrics",
            "org.apache.kafka.common.network"
    };

    /**
     * The default queue size for deferred events.
     */
    public static final int DEFAULT_QUEUE_SIZE = 256;

    // Queue size for deferred events.
    private int queueSize = DEFAULT_QUEUE_SIZE;
    private static final int UNDEFINED = -1;

    // When the queue capacity falls beneath the discarding threshold, events are either disgarded or sent to the
    // fallback appender.
    private int discardingThreshold = UNDEFINED;
    private boolean includeCallerData = false;

    private ProducerLifeCycleStrategy<byte[], byte[]> producerLifeCycleStrategy = new LazyProducerLifeCycleStrategy(this);
    private BlockingQueue<ILoggingEvent> blockingQueue;
    private final ReentrantLock metadataAvailableLock = new ReentrantLock();
    private volatile boolean metadataAvailable = false;

    public KafkaAppender() {
        // setting these as config values sidesteps an unnecessary warning (minor bug in KafkaProducer)
        addProducerConfigValue(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        addProducerConfigValue(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    }

    @Override
    public void start() {
        // only error free appenders should be activated
        if (!checkPrerequisites()) return;

        if (queueSize < 1) {
            addError("Invalid queue size [" + queueSize + "]");
            return;
        }

        if (discardingThreshold == UNDEFINED) {
            discardingThreshold = queueSize / 5;
            addInfo("Setting discardingThreshold to " + discardingThreshold);
        }


        blockingQueue = new ArrayBlockingQueue<ILoggingEvent>(queueSize);

        producerLifeCycleStrategy.setProducerConfig(producerConfig);
        producerLifeCycleStrategy.start();

        super.start();
    }

    @Override
    public void stop() {
        if (producerLifeCycleStrategy != null && producerLifeCycleStrategy.isStarted()) {
            ensureDeferredAppends();
            producerLifeCycleStrategy.stop();
        }
        super.stop();
    }

    public void setDiscardingThreshold(int discardingThreshold) {
        this.discardingThreshold = discardingThreshold;
    }

    public boolean isIncludeCallerData() {
        return includeCallerData;
    }

    public void setIncludeCallerData(boolean includeCallerData) {
        this.includeCallerData = includeCallerData;
    }

    public void setProducerLifeCycleStrategy(ProducerLifeCycleStrategy<byte[], byte[]> producerLifeCycleStrategy) {
        this.producerLifeCycleStrategy = producerLifeCycleStrategy;
    }

    @Override
    protected void append(ILoggingEvent e) {
        // events which are logged by Kafka client must be deferred to avoid recursion
        if (isLoggedByKafkaClient(e) || !isMetadataAvailable()) {
            deferAppend(e);
        } else {
            // ensure the delivery of deferred events prior to that of subsequent events
            ensureDeferredAppends();

            encodeAndDeliver(e);
        }
    }

    @Override
    protected void preprocess(ILoggingEvent e) {
        e.prepareForDeferredProcessing();
        if (includeCallerData) {
            e.getCallerData();
        }
    }

    @Override
    protected boolean isDiscardable(ILoggingEvent e) {
        return Level.DEBUG.isGreaterOrEqual(e.getLevel());
    }

    private void encodeAndDeliver(ILoggingEvent e) {
        final byte[] payload = encoder.doEncode(e);
        final byte[] key = keyingStrategy.createKey(e);
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[],byte[]>(topic, key, payload);
        deliveryStrategy.send(producerLifeCycleStrategy.getProducer(), record, e, failedDeliveryCallback);
    }

    private void deferAppend(ILoggingEvent event) {
        if (isQueueBelowDiscardingThreshold()) {
            if (isDiscardable(event)) {
                return;
            } else {
                aai.appendLoopOnAppenders(event);
                return;
            }
        }
        preprocess(event);
        put(event);
    }

    private boolean isQueueBelowDiscardingThreshold() {
        return (blockingQueue.remainingCapacity() < discardingThreshold);
    }

    private void put(ILoggingEvent eventObject) {
        try {
            blockingQueue.put(eventObject);
        } catch (InterruptedException ignored) {
        }
    }

    // drains and delivers queued events
    private void ensureDeferredAppends() {
        ILoggingEvent event;

        while ((event = blockingQueue.poll()) != null) {
            encodeAndDeliver(event);
        }
    }

    private boolean isLoggedByKafkaClient(ILoggingEvent e) {
        final String loggerName = e.getLoggerName();
        for (String prefix : KAFKA_LOGGER_PREFIXES) {
            if (loggerName.startsWith(prefix)) return true;
        }
        return false;
    }

    private boolean isMetadataAvailable() {
        if (!metadataAvailableLock.tryLock()) {
            return metadataAvailable;
        }

        try {
            metadataAvailable = !producerLifeCycleStrategy.getProducer().partitionsFor(topic).isEmpty();
        } catch (Exception ignored) {

        } finally {
            metadataAvailableLock.unlock();
        }

        return metadataAvailable;
    }

}
