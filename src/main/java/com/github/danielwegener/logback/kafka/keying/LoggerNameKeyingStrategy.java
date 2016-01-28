package com.github.danielwegener.logback.kafka.keying;

import ch.qos.logback.classic.spi.ILoggingEvent;

import java.nio.ByteBuffer;

/**
 * This strategy uses the logger name as partitioning key. This ensures that all messages logged by the
 * same logger will remain in the correct order for any consumer.
 * But this strategy can lead to uneven log distribution for a small number of distinct loggers (compared to the number of partitions).
 * @since 0.0.1
 */
public class LoggerNameKeyingStrategy implements KeyingStrategy<ILoggingEvent> {

    @Override
    public byte[] createKey(ILoggingEvent e) {
        final String loggerName;
        if (e.getLoggerName() == null) {
            loggerName = "";
        } else {
            loggerName = e.getLoggerName();
        }
        return ByteBuffer.allocate(4).putInt(loggerName.hashCode()).array();
    }

}
