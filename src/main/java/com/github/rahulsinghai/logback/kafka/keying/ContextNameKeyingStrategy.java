package com.github.rahulsinghai.logback.kafka.keying;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.spi.ContextAwareBase;
import java.nio.ByteBuffer;

/**
 * This strategy uses logbacks CONTEXT_NAME as kafka message key. This is ensures that all log
 * messages logged by the same logging context will remain in the correct order for any consumer.
 * But this strategy can lead to uneven log distribution for a small number of hosts (compared to
 * the number of partitions).
 *
 * @since 0.0.1
 */
public class ContextNameKeyingStrategy extends ContextAwareBase implements
    KeyingStrategy<ILoggingEvent> {

    private byte[] contextNameHash = null;

    @Override
    public void setContext(Context context) {
        super.setContext(context);
        final String hostname = context.getProperty(CoreConstants.CONTEXT_NAME_KEY);
        if (hostname == null) {
            addError(
                "Hostname could not be found in context. HostNamePartitioningStrategy will not work.");
        } else {
            contextNameHash = ByteBuffer.allocate(4).putInt(hostname.hashCode()).array();
        }
    }

    @Override
    public byte[] createKey(ILoggingEvent e) {
        return contextNameHash;
    }
}
