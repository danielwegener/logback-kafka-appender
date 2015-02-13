package com.github.danielwegener.logback.kafka.partitioning;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.spi.ContextAwareBase;

import java.nio.ByteBuffer;

/**
 * This strategy uses the HOSTNAME to partition the log messages to kafka.
 * This is useful because it ensures that all log messages issued by this host will remain in the correct order for any consumer.
 * But this strategy can lead to uneven log distribution for a small number of hosts (compared to the number of partitions).
 */
public class HostNamePartitioningStrategy extends ContextAwareBase implements PartitioningStrategy<byte[]> {

    private byte[] hostnameHash = null;

    @Override
    public void setContext(Context context) {
        super.setContext(context);
        final String hostname = context.getProperty(CoreConstants.HOSTNAME_KEY);
        if (hostname == null) {
            addError("Hostname could not be found in context. HostNamePartitioningStrategy will not work.");
        } else {
            hostnameHash = ByteBuffer.allocate(4).putInt(hostname.hashCode()).array();
        }
    }

    @Override
    public byte[] createKey(ILoggingEvent e) {
        return hostnameHash;
    }
}
