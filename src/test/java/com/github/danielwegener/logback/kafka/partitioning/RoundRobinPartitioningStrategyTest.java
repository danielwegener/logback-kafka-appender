package com.github.danielwegener.logback.kafka.partitioning;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;


public class RoundRobinPartitioningStrategyTest {

    private final RoundRobinPartitioningStrategy unit = new RoundRobinPartitioningStrategy();

    @Test
    public void shouldAlwaysReturnNull() {
        assertThat(unit.createKey(Mockito.mock(ILoggingEvent.class)), is(nullValue()));
    }

}