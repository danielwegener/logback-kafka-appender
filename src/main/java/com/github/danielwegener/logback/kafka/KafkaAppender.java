package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;


public class KafkaAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

    private KafkaProducer producer = null;

    @Override
    protected void append(ILoggingEvent e) {

    }

    @Override
    public void start() {
        producer = new  KafkaProducer(context.getCopyOfPropertyMap());

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
