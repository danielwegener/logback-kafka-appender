package com.github.rahulsinghai.logback.kafka.keying;

import static org.hamcrest.MatcherAssert.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.CoreConstants;
import java.nio.ByteBuffer;
import org.hamcrest.Matchers;
import org.junit.Test;

public class HostNameKeyingStrategyTest {

    private final HostNameKeyingStrategy unit = new HostNameKeyingStrategy();

    private final LoggerContext ctx = new LoggerContext();

    @Test
    public void shouldPartitionByHostName() {
        ctx.putProperty(CoreConstants.HOSTNAME_KEY, "localhost");
        unit.setContext(ctx);
        final ILoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL,
            "msg", null, new Object[0]);
        assertThat(unit.createKey(evt),
            Matchers.equalTo(ByteBuffer.allocate(4).putInt("localhost".hashCode()).array()));
    }
}
