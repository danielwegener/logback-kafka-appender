package com.github.danielwegener.logback.kafka;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import ch.qos.logback.core.spi.FilterReply;
import com.github.danielwegener.logback.kafka.delivery.FailedDeliveryCallback;

import java.util.Iterator;

public abstract class KafkaAppenderBase<E> extends KafkaAppenderConfig<E> {

    // Reentrancy guard preempts guard in UnsynchronizedAppenderBase to handle reentrant appends (if needed)
    private ThreadLocal<Boolean> guard = new ThreadLocal<Boolean>();

    AppenderAttachableImpl<E> aai = new AppenderAttachableImpl<E>();

    FailedDeliveryCallback<E> failedDeliveryCallback = new FailedDeliveryCallback<E>() {
        @Override
        public void onFailedDelivery(E evt, Throwable throwable) {
            aai.appendLoopOnAppenders(evt);
        }
    };

    @Override
    public void doAppend(E e) {
        if (Boolean.TRUE.equals(guard.get()) && getFilterChainDecision(e) != FilterReply.DENY) {
            doReentrantAppend(e);
            return;
        }

        try {
            guard.set(Boolean.TRUE);
            super.doAppend(e);
        } finally {
            guard.set(Boolean.FALSE);
        }
    }

    @Override
    public void addAppender(Appender<E> newAppender) {
        aai.addAppender(newAppender);
    }

    @Override
    public Iterator<Appender<E>> iteratorForAppenders() {
        return aai.iteratorForAppenders();
    }

    @Override
    public Appender<E> getAppender(String name) {
        return aai.getAppender(name);
    }

    @Override
    public boolean isAttached(Appender<E> appender) {
        return aai.isAttached(appender);
    }

    @Override
    public void detachAndStopAllAppenders() {
        aai.detachAndStopAllAppenders();
    }

    @Override
    public boolean detachAppender(Appender<E> appender) {
        return aai.detachAppender(appender);
    }

    @Override
    public boolean detachAppender(String name) {
        return aai.detachAppender(name);
    }

    /**
     * Process reentrant logging events (ie logging events produced from doAppend).
     *
     * Great care must be taken implementing this to avoid a runaway snowball in the number of log messages, or
     * other recursive gotchas.
     *
     * Presumably it is for this reason that the default behavior in logback is to discard reentrant messages.
     * @param e
     */
    protected void doReentrantAppend(E e) {
        // discard reentrant events by default (as is done with UnsynchronizedAppenderBase)
    }

    /**
     * Is the event discardable?
     *
     * @param eventObject
     * @return
     */
    protected boolean isDiscardable(E eventObject) {
        return false;
    }

    /**
     * Preprocess event in case of deferral.
     * @param eventObject
     */
    protected void preprocess(E eventObject) {
    }

}
