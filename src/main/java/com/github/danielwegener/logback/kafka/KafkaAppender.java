package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * See <a href="https://github.com/danielwegener/logback-kafka-appender">logback-kafka-appender at github</a>
 */
public class KafkaAppender extends KafkaAppenderBase<ILoggingEvent> {

    private static final String KAFKA_LOGGER_PREFIX = "org.apache.kafka";

    @Override
    public void doAppend(ILoggingEvent eventObject) {
        if (eventObject.getLoggerName().startsWith(KAFKA_LOGGER_PREFIX)) return;
        super.doAppend(eventObject);
    }

}
