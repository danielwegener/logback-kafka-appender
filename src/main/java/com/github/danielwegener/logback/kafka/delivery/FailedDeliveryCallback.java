package com.github.danielwegener.logback.kafka.delivery;

public interface FailedDeliveryCallback<E> {
    void onFailedDelivery(E evt, Throwable throwable);
}
