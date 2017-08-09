package com.github.danielwegener.logback.kafka;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import ch.qos.logback.core.spi.FilterReply;
import com.github.danielwegener.logback.kafka.delivery.FailedDeliveryCallback;

import java.util.Iterator;

public abstract class KafkaAppenderBase<E> extends KafkaAppenderConfig<E> {

    AppenderAttachableImpl<E> aai = new AppenderAttachableImpl<E>();

    FailedDeliveryCallback<E> failedDeliveryCallback = new FailedDeliveryCallback<E>() {
        @Override
        public void onFailedDelivery(E evt, Throwable throwable) {
            aai.appendLoopOnAppenders(evt);
        }
    };

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
