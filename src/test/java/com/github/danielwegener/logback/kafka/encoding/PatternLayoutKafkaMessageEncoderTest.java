package com.github.danielwegener.logback.kafka.encoding;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Layout;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PatternLayoutKafkaMessageEncoderTest {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private final PatternLayout layout = new PatternLayout();
    private final PatternLayoutKafkaMessageEncoder unit = new PatternLayoutKafkaMessageEncoder(layout, UTF8);

    @Test
    public void testStart() {
        unit.start();
        assertTrue("isStarted",unit.isStarted());
    }


    @Test
    public void testDoEncode() {
        final LoggerContext ctx = new LoggerContext();
        layout.setContext(ctx);
        layout.setPattern("prefix %msg");
        layout.start();
        final Logger logger = ctx.getLogger("logger");
        final LoggingEvent evt = new LoggingEvent("fqcn", logger, Level.ALL,"message", null, new Object[0]);
        assertThat(unit.doEncode(evt), equalTo("prefix message".getBytes()));
    }

    @Test
    public void testGetLayout() {
        assertThat(unit.getLayout(), Matchers.<Layout<?>>equalTo(layout));
    }

    @Test
    public void testGetCharset() {
        assertThat(unit.getCharset(), equalTo(UTF8));
    }


}