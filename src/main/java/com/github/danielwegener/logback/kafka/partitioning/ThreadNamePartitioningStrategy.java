package com.github.danielwegener.logback.kafka.partitioning;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.spi.ContextAwareBase;
import ch.qos.logback.core.spi.LifeCycle;

import java.nio.ByteBuffer;

/**
 * This strategy uses the calling threads name as partitioning key. This ensures that all messages logged by the
 * same thread will remain in the correct order for any consumer.
 * But this strategy can lead to uneven log distribution for a small number of thread(-names) (compared to the number of partitions).
 */
public class ThreadNamePartitioningStrategy implements PartitioningStrategy {

    @Override
    public byte[] createKey(ILoggingEvent e) {
        return ByteBuffer.allocate(4).putInt(e.getThreadName().hashCode()).array();
    }
}
