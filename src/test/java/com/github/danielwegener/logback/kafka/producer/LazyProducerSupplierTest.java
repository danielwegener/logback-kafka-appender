package com.github.danielwegener.logback.kafka.producer;

import ch.qos.logback.core.spi.ContextAware;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class LazyProducerSupplierTest {


    @Test
    public void testStartGetStop() {
        ContextAware contextAware = mock(ContextAware.class);
        Map<String, Object> producerConfig = mock(Map.class);
        LazyProducerSupplier producerSupplier = mock(LazyProducerSupplier.class);
        //doCallRealMethod().when(producerSupplier).start(same(contextAware), same(producerConfig));
        when(producerSupplier.get()).thenCallRealMethod();
        doCallRealMethod().when(producerSupplier).stop();
        Producer producerMock = mock(Producer.class);
        when(producerSupplier.createProducer()).thenReturn(producerMock);

        // Start does nothing
        producerSupplier.start(contextAware, producerConfig);

        // Get initializes and returns producer
        Producer producer = producerSupplier.get();
        verify(producerSupplier).createProducer();
        assertThat(producer, sameInstance(producerMock));

        // Stop closes producer
        producerSupplier.stop();
        verify(producerMock).close();
    }

    @Test
    public void testGetFailure() {
        ContextAware contextAware = mock(ContextAware.class);
        Map<String, Object> producerConfig = mock(Map.class);
        LazyProducerSupplier producerSupplier = mock(LazyProducerSupplier.class);
        //doCallRealMethod().when(producerSupplier).start(same(contextAware), same(producerConfig));
        when(producerSupplier.get()).thenCallRealMethod();
        when(producerSupplier.createProducer()).thenThrow(IllegalArgumentException.class);

        // Start does nothing
        producerSupplier.start(contextAware, producerConfig);

        // Get initializes and returns producer
        Producer producer = producerSupplier.get();
        assertThat(producer, nullValue());
        verify(producerSupplier).createProducer();
        verify(contextAware).addError(anyString(), any(IllegalArgumentException.class));
    }
}