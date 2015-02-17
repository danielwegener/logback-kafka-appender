package com.github.danielwegener.logback.kafka.partitioning;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;


public class ContextNamePartitioningStrategyTest {

    private final ContextNamePartitioningStrategy unit = new ContextNamePartitioningStrategy();

    private final LoggerContext ctx = new LoggerContext();
    private static final String LOGGER_CONTEXT_NAME = "loggerContextName";

    @Before
    public void before() {
        ctx.setName(LOGGER_CONTEXT_NAME);
        unit.setContext(ctx);
    }

    @Test
    public void shouldPartitionByEventThreadName() {
        final ILoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "msg", null, new Object[0]);
        Assert.assertThat(unit.createKey(evt), Matchers.equalTo(ByteBuffer.allocate(4).putInt(LOGGER_CONTEXT_NAME.hashCode()).array()));
    }


}