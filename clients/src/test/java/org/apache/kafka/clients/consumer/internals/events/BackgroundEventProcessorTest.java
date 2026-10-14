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
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackgroundEventProcessorTest {

    private final BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
    private final BackgroundEventHandler backgroundEventHandler =
            new BackgroundEventHandler(new LogContext(), backgroundEventQueue);
    private final BackgroundEventProcessor processor =
            new BackgroundEventProcessor(new LogContext(), backgroundEventQueue);

    @Test
    public void testNoEvents() {
        assertTrue(backgroundEventQueue.isEmpty());
        assertDoesNotThrow(() -> processor.process());
    }

    @Test
    public void testErrorEventIsRethrownOnProcess() {
        KafkaException error = new InvalidGroupIdException("error");
        backgroundEventHandler.add(new ErrorBackgroundEvent(error));

        KafkaException thrown = assertThrows(KafkaException.class, processor::process);

        assertSame(error, thrown);
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testNonKafkaErrorIsWrapped() {
        IllegalStateException error = new IllegalStateException("error");
        backgroundEventHandler.add(new ErrorBackgroundEvent(error));

        KafkaException thrown = assertThrows(KafkaException.class, processor::process);

        assertSame(error, thrown.getCause());
    }

    @Test
    public void testFirstErrorIsThrownAfterAllEventsWereProcessed() {
        KafkaException first = new KafkaException("first");
        KafkaException second = new KafkaException("second");
        backgroundEventHandler.add(new ErrorBackgroundEvent(first));
        backgroundEventHandler.add(new ErrorBackgroundEvent(second));

        assertSame(first, assertThrows(KafkaException.class, processor::process));
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testProcessSingleErrorEventRethrowsItsError() {
        RuntimeException error = new IllegalArgumentException("error");
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> processor.process(new ErrorBackgroundEvent(error)));
        assertSame(error, thrown);
    }

    @Test
    public void testCheckedThrowableIsWrappedInKafkaExceptionByTheEvent() {
        Exception checked = new Exception("checked");
        ErrorBackgroundEvent event = new ErrorBackgroundEvent(checked);

        assertInstanceOf(KafkaException.class, event.error());
        assertSame(checked, event.error().getCause());
        assertEquals(BackgroundEvent.Type.ERROR, event.type());
    }

    @Test
    public void testRuntimeThrowableIsKeptAsIs() {
        RuntimeException runtime = new IllegalStateException("runtime");
        assertSame(runtime, new ErrorBackgroundEvent(runtime).error());
    }

    @Test
    public void testCloseIsIdempotent() {
        backgroundEventHandler.add(new ErrorBackgroundEvent(new KafkaException("error")));

        processor.close();
        processor.close();

        // The events that were pending were discarded on close.
        assertTrue(backgroundEventQueue.isEmpty());
        assertDoesNotThrow(() -> processor.process());
    }

    @Test
    public void testHandlerRejectsNullEvent() {
        assertThrows(NullPointerException.class, () -> backgroundEventHandler.add(null));
    }
}
