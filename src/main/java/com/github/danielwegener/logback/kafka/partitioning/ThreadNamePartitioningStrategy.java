package com.github.danielwegener.logback.kafka.partitioning;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.spi.ContextAwareBase;

import java.nio.ByteBuffer;

/**
 * This strategy uses the hostname and the calling thread as partitioning key. This ensures that all messages logged by the
 * same thread will remain in the correct order for any consumer.
 * But this strategy can lead to uneven log distribution for a small number of threads and hosts (compared to the number of partitions).
 */
public class ThreadNamePartitioningStrategy extends ContextAwareBase implements PartitioningStrategy<byte[]> {

    private byte[] hostnameHash = null;

    @Override
    public void setContext(Context context) {
        super.setContext(context);
        final String hostname = context.getProperty(CoreConstants.HOSTNAME_KEY);
        if (hostname == null) {
            addWarn("Hostname could not be found in context. ThreadNamePartitioningStrategy may not properly distribute across all partitions.");
        } else {
            hostnameHash = ByteBuffer.allocate(4).putInt(hostname.hashCode()).array();
        }
    }

    @Override
    public byte[] createKey(ILoggingEvent e) {
        if (hostnameHash != null) {
            return ByteBuffer.allocate(8).put(hostnameHash).putInt(e.getThreadName().hashCode()).array();
        } else {
            return ByteBuffer.allocate(4).putInt(e.getThreadName().hashCode()).array();
        }
    }
}
