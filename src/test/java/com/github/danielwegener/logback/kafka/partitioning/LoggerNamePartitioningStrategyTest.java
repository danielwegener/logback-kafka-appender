package com.github.danielwegener.logback.kafka.partitioning;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.CoreConstants;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;


public class LoggerNamePartitioningStrategyTest {

    private final LoggerNamePartitioningStrategy unit = new LoggerNamePartitioningStrategy();

    private final LoggerContext ctx = new LoggerContext();


    @Test
    public void shouldPartitionByLoggerName() {
        final ILoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "msg", null, new Object[0]);
        Assert.assertThat(unit.createKey(evt), Matchers.equalTo(ByteBuffer.allocate(4).putInt("logger".hashCode()).array()));
    }


}