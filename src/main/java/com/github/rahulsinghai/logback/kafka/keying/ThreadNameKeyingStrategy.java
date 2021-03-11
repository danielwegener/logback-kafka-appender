package com.github.rahulsinghai.logback.kafka.keying;

import ch.qos.logback.classic.spi.ILoggingEvent;

import java.nio.ByteBuffer;

/**
 * This strategy uses the calling threads name as partitioning key. This ensures that all messages logged by the
 * same thread will remain in the correct order for any consumer.
 * But this strategy can lead to uneven log distribution for a small number of thread(-names) (compared to the number of partitions).
 * @since 0.0.1
 */
public class ThreadNameKeyingStrategy implements KeyingStrategy<ILoggingEvent> {

    @Override
    public byte[] createKey(ILoggingEvent e) {
        return ByteBuffer.allocate(4).putInt(e.getThreadName().hashCode()).array();
    }
}
