package com.github.danielwegener.logback.kafka.encoding;

import ch.qos.logback.core.spi.ContextAwareBase;
import ch.qos.logback.core.spi.LifeCycle;

/**
 * A base class for {@link KafkaMessageEncoder}'s that are {@link ContextAwareBase} and have a {@link LifeCycle}
 * @since 0.0.1
 */
public abstract class KafkaMessageEncoderBase<E> extends ContextAwareBase implements KafkaMessageEncoder<E>, LifeCycle {

    private boolean started = false;

    @Override
    public void start() {
        started = true;
    }

    @Override
    public void stop() {
        started = false;
    }

    @Override
    public boolean isStarted() {
        return started;
    }
}
