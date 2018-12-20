package com.github.danielwegener.logback.kafka.keying;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;


public class LoggerNameKeyingStrategyTest {

    private final LoggerNameKeyingStrategy unit = new LoggerNameKeyingStrategy();

    private final LoggerContext ctx = new LoggerContext();


    @Test
    public void shouldPartitionByLoggerName() {
        final ILoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "msg", null, new Object[0]);
        Assert.assertThat(unit.createKey(evt), Matchers.equalTo(ByteBuffer.allocate(4).putInt("logger".hashCode()).array()));
    }


}
