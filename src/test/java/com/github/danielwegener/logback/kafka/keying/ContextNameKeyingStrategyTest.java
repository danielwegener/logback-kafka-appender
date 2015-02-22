package com.github.danielwegener.logback.kafka.keying;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;


public class ContextNameKeyingStrategyTest {

    private final ContextNameKeyingStrategy unit = new ContextNameKeyingStrategy();

    private final LoggerContext ctx = new LoggerContext();
    private static final String LOGGER_CONTEXT_NAME = "loggerContextName";


    @Test
    public void shouldPartitionByEventThreadName() {
        ctx.setName(LOGGER_CONTEXT_NAME);
        unit.setContext(ctx);
        final ILoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "msg", null, new Object[0]);
        Assert.assertThat(unit.createKey(evt), Matchers.equalTo(ByteBuffer.allocate(4).putInt(LOGGER_CONTEXT_NAME.hashCode()).array()));
    }


}