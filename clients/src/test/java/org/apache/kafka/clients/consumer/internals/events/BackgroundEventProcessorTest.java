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
import org.apache.kafka.common.errors.GroupAuthorizationException;
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
    public void testNoEvents() {
        assertDoesNotThrow(() -> processor.process());
    }

    @Test
    public void testErrorEventIsRethrownOnTheCallingThread() {
        GroupAuthorizationException error = new GroupAuthorizationException("not allowed");
        handler.add(new ErrorBackgroundEvent(error));

        GroupAuthorizationException thrown = assertThrows(GroupAuthorizationException.class, () -> processor.process());

        assertSame(error, thrown);
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testCheckedErrorIsWrappedInKafkaException() {
        IOException cause = new IOException("disk on fire");
        ErrorBackgroundEvent event = new ErrorBackgroundEvent(cause);

        assertInstanceOf(KafkaException.class, event.error());
        assertSame(cause, event.error().getCause());

        handler.add(event);
        KafkaException thrown = assertThrows(KafkaException.class, () -> processor.process());
        assertSame(cause, thrown.getCause());
    }

    @Test
    public void testRuntimeErrorIsNotWrapped() {
        IllegalStateException error = new IllegalStateException("bad state");

        assertSame(error, new ErrorBackgroundEvent(error).error());
    }

    @Test
    public void testFirstErrorIsThrownAfterAllEventsWereProcessed() {
        IllegalStateException first = new IllegalStateException("first");
        IllegalArgumentException second = new IllegalArgumentException("second");
        handler.add(new ErrorBackgroundEvent(first));
        handler.add(new ErrorBackgroundEvent(second));

        assertSame(first, assertThrows(IllegalStateException.class, () -> processor.process()));

        // Both events were consumed, even if only one error could be thrown
        assertTrue(queue.isEmpty());
        assertDoesNotThrow(() -> processor.process());
    }

    @Test
    public void testErrorsAreNotLostBetweenCalls() {
        handler.add(new ErrorBackgroundEvent(new IllegalStateException("one")));
        assertThrows(IllegalStateException.class, () -> processor.process());

        handler.add(new ErrorBackgroundEvent(new IllegalArgumentException("two")));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> processor.process());
        assertEquals("two", e.getMessage());
    }

    @Test
    public void testProcessAfterCloseFails() {
        processor.close();

        assertThrows(IllegalStateException.class, () -> processor.process());
    }

    @Test
    public void testCloseIsIdempotent() {
        assertDoesNotThrow(() -> {
            processor.close();
            processor.close();
        });
    }
}
