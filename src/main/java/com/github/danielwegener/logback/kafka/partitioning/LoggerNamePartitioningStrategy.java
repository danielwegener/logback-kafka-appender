package com.github.danielwegener.logback.kafka.partitioning;

import ch.qos.logback.classic.spi.ILoggingEvent;

import java.nio.ByteBuffer;

/**
 * This strategy uses the hostname and the logger name as partitioning key. This ensures that all messages logged by the
 * same logger will remain in the correct order for any consumer.
 * But this strategy can lead to uneven log distribution for a small number of threads (compared to the number of partitions).
 */
public class LoggerNamePartitioningStrategy implements PartitioningStrategy<byte[]> {

    @Override
    public byte[] createKey(ILoggingEvent e) {
        return ByteBuffer.allocate(4).putInt(e.getThreadName().hashCode()).array();
    }

}
