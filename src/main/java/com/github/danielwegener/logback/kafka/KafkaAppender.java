package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.ErrorStatus;
import ch.qos.logback.core.status.Status;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaAppender<E extends ILoggingEvent> extends UnsynchronizedAppenderBase<E> {

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

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private KafkaProducer<byte[], byte[]> producer = null;

    private String topic = null;
    private byte[] hostnameHash = null;


    private Charset charset = null;


    private PartitioningStrategy partitioningStrategy = null;

    protected Layout<E> layout;

    @Override
    protected void append(E e) {
        final String message = layout.doLayout(e);
        final byte[] payload = message.getBytes(charset);
        final byte[] key = createKeyByStrategy(partitioningStrategy, e, hostnameHash);
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[],byte[]>(topic, key, payload);
        producer.send(record);
    }

    private static byte[] createKeyByStrategy(PartitioningStrategy strategy, ILoggingEvent e, byte[] hostnameHash) {
        switch (strategy) {
            case THREAD:
                if (hostnameHash != null) {
                    return ByteBuffer.allocate(8).put(hostnameHash).putInt(e.getThreadName().hashCode()).array();
                } else {
                    return ByteBuffer.allocate(4).putInt(e.getThreadName().hashCode()).array();
                }
            case HOSTNAME:
                return hostnameHash;
            case ROUND_ROBIN:
            default:
                return null;
        }
    }

    private boolean checkPrerequisites() {
        int errors = 0;

        if (this.topic == null) {
            addStatus(new ErrorStatus("No topic set for the appender named \""
                    + name + "\".", this));
            errors++;
        }

        if (this.layout == null) {
            addStatus(new ErrorStatus("No layout set for the appender named \""
                    + name + "\".", this));
            errors++;
        }

        // only error free appenders should be activated
        return errors == 0;
    }

    @Override
    public void start() {
        if (this.topic == null)
            this.topic = context.getProperty("topic");

        if (!checkPrerequisites()) return;

        final String hostname = context.getProperty("HOSTNAME");
        if (hostname != null) {
            hostnameHash = ByteBuffer.allocate(4).putInt(hostname.hashCode()).array();
        } else {
            this.addWarn("Could not determine hostname. PartitionStrategy HOSTNAME will not work.");
        }

        if (charset == null) {
            addInfo("No charset specified. Using default UTF8 encoding.");
            charset = UTF8;
        }

        if (partitioningStrategy == null) {
            addInfo("No partitionStrategy defined. Using default ROUND_ROBIN strategy.");
            partitioningStrategy = PartitioningStrategy.ROUND_ROBIN;
        }

        final ByteArraySerializer serializer = new ByteArraySerializer();
        producer = new  KafkaProducer<byte[], byte[]>(new HashMap<String, Object>(context.getCopyOfPropertyMap()), serializer, serializer);

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

    public void setCharset(String charset) {
        this.charset = Charset.forName(charset);
    }

    public Layout<E> getLayout() {
        return layout;
    }

    public void setLayout(Layout<E> layout) {
        this.layout = layout;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }


    public void setPartitioningStrategy(String partitioningStrategy) {
        this.partitioningStrategy = PartitioningStrategy.valueOf(partitioningStrategy);
    }


}
