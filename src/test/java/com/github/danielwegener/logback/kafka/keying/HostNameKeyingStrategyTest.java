package com.github.danielwegener.logback.kafka.keying;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.CoreConstants;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;


public class HostNameKeyingStrategyTest {

    private final HostNameKeyingStrategy unit = new HostNameKeyingStrategy();

    private final LoggerContext ctx = new LoggerContext();


    @Test
    public void shouldPartitionByHostName() {
        ctx.putProperty(CoreConstants.HOSTNAME_KEY, "localhost");
        unit.setContext(ctx);
        final ILoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "msg", null, new Object[0]);
        Assert.assertThat(unit.createKey(evt), Matchers.equalTo(ByteBuffer.allocate(4).putInt("localhost".hashCode()).array()));
    }


}