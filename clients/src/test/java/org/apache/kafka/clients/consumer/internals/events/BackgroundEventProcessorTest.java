/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackgroundEventProcessorTest {

    private final LogContext logContext = new LogContext();
    private BlockingQueue<BackgroundEvent> queue;
    private BackgroundEventHandler handler;
    private BackgroundEventProcessor processor;

    @BeforeEach
    public void setup() {
        queue = new LinkedBlockingQueue<>();
        handler = new BackgroundEventHandler(logContext, queue);
        processor = new BackgroundEventProcessor(logContext, queue);
    }

    @Test
    public void testProcessWithNoEventsDoesNothing() {
        assertDoesNotThrow(() -> processor.process());
    }

    @Test
    public void testErrorEventIsRethrownOnTheCallingThread() {
        KafkaException error = new KafkaException("Failure in the network thread");
        handler.add(new ErrorBackgroundEvent(error));

        KafkaException thrown = assertThrows(KafkaException.class, processor::process);

        assertSame(error, thrown);
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testRuntimeExceptionTypeIsPreserved() {
        handler.add(new ErrorBackgroundEvent(new TimeoutException("Timed out")));

        assertThrows(TimeoutException.class, processor::process);
    }

    @Test
    public void testCheckedErrorIsWrappedInKafkaException() {
        IOException cause = new IOException("Disk is gone");
        handler.add(new ErrorBackgroundEvent(cause));

        KafkaException thrown = assertThrows(KafkaException.class, processor::process);

        assertSame(cause, thrown.getCause());
    }

    @Test
    public void testAllEventsAreProcessedAndFirstErrorIsThrown() {
        KafkaException first = new KafkaException("First");
        KafkaException second = new KafkaException("Second");
        handler.add(new ErrorBackgroundEvent(first));
        handler.add(new ErrorBackgroundEvent(second));

        KafkaException thrown = assertThrows(KafkaException.class, processor::process);

        assertSame(first, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(second, thrown.getSuppressed()[0]);
        // Nothing is left behind to be thrown on the next call
        assertTrue(queue.isEmpty());
        assertDoesNotThrow(() -> processor.process());
    }

    @Test
    public void testProcessSingleErrorEventThrowsItsError() {
        KafkaException error = new KafkaException("Direct");

        assertSame(error, assertThrows(KafkaException.class, () -> processor.process(new ErrorBackgroundEvent(error))));
    }

    @Test
    public void testEventClass() {
        assertEquals(BackgroundEvent.class, processor.getEventClass());
    }

    @Test
    public void testCloseIsIdempotent() {
        processor.close();
        assertDoesNotThrow(processor::close);
    }

    @Test
    public void testProcessAfterCloseFails() {
        processor.close();

        assertThrows(IllegalStateException.class, processor::process);
    }

    @Test
    public void testCloseDiscardsPendingEvents() {
        handler.add(new ErrorBackgroundEvent(new KafkaException("Never delivered")));

        processor.close();

        assertTrue(queue.isEmpty());
    }

    @Test
    public void testErrorBackgroundEventWrapsCheckedThrowable() {
        ErrorBackgroundEvent event = new ErrorBackgroundEvent(new IOException("Checked"));

        assertInstanceOf(KafkaException.class, event.error());
        assertInstanceOf(IOException.class, event.error().getCause());
    }

    @Test
    public void testErrorBackgroundEventKeepsRuntimeException() {
        IllegalStateException error = new IllegalStateException("Runtime");

        assertSame(error, new ErrorBackgroundEvent(error).error());
    }
}
