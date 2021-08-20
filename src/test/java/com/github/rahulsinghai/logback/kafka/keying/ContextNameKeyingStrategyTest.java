package com.github.rahulsinghai.logback.kafka.keying;

import static org.hamcrest.MatcherAssert.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import java.nio.ByteBuffer;
import org.hamcrest.Matchers;
import org.junit.Test;

public class ContextNameKeyingStrategyTest {

    private static final String LOGGER_CONTEXT_NAME = "loggerContextName";
    private final ContextNameKeyingStrategy unit = new ContextNameKeyingStrategy();
    private final LoggerContext ctx = new LoggerContext();

    @Test
    public void shouldPartitionByEventThreadName() {
        ctx.setName(LOGGER_CONTEXT_NAME);
        unit.setContext(ctx);
        final ILoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL,
            "msg", null, new Object[0]);
        assertThat(unit.createKey(evt), Matchers
            .equalTo(ByteBuffer.allocate(4).putInt(LOGGER_CONTEXT_NAME.hashCode()).array()));
    }
}
