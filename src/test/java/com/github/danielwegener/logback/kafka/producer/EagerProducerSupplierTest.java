package com.github.danielwegener.logback.kafka.producer;

import ch.qos.logback.core.spi.ContextAware;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.*;

public class EagerProducerSupplierTest {


    @Test
    public void testStartGetStop() {
        ContextAware contextAware = mock(ContextAware.class);
        Map<String, Object> producerConfig = mock(Map.class);
        EagerProducerSupplier producerSupplier = mock(EagerProducerSupplier.class);
        doCallRealMethod().when(producerSupplier).start(same(contextAware), same(producerConfig));
        when(producerSupplier.get()).thenCallRealMethod();
        doCallRealMethod().when(producerSupplier).stop();
        Producer producerMock = mock(Producer.class);
        when(producerSupplier.createProducer()).thenReturn(producerMock);

        // Start creates producer
        producerSupplier.start(contextAware, producerConfig);
        verify(producerSupplier).createProducer();

        // Get returns producer
        Producer producer = producerSupplier.get();
        assertThat(producer, sameInstance(producerMock));

        // Stop closes producer
        producerSupplier.stop();
        verify(producerMock).close();
    }

    @Test
    public void testStartFailure() {
        ContextAware contextAware = mock(ContextAware.class);
        Map<String, Object> producerConfig = mock(Map.class);
        EagerProducerSupplier producerSupplier = mock(EagerProducerSupplier.class);
        doCallRealMethod().when(producerSupplier).start(same(contextAware), same(producerConfig));
        when(producerSupplier.get()).thenCallRealMethod();
        when(producerSupplier.createProducer()).thenThrow(IllegalArgumentException.class);

        // Start fails to create producer
        try {
            producerSupplier.start(contextAware, producerConfig);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
        }
        verify(producerSupplier).createProducer();
        verify(contextAware).addError(anyString(), any(IllegalArgumentException.class));

        // Get returns null
        Producer producer = producerSupplier.get();
        assertThat(producer, nullValue());

    }
}