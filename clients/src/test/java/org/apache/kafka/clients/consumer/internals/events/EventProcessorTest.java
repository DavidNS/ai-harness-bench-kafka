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
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EventProcessorTest {

    private BlockingQueue<TestEvent> queue;
    private TestEventProcessor processor;

    @BeforeEach
    public void setup() {
        queue = new LinkedBlockingQueue<>();
        processor = new TestEventProcessor(queue);
    }

    @Test
    public void testProcessEventsInOrder() {
        queue.add(new TestEvent("a"));
        queue.add(new TestEvent("b"));
        queue.add(new TestEvent("c"));

        processor.process();

        assertEquals(3, processor.processed.size());
        assertEquals("a", processor.processed.get(0).name);
        assertEquals("b", processor.processed.get(1).name);
        assertEquals("c", processor.processed.get(2).name);
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testProcessWithNoEvents() {
        processor.process();

        assertTrue(processor.processed.isEmpty());
        assertTrue(processor.errors.isEmpty());
    }

    @Test
    public void testErrorsArePassedToTheHandlerAndDoNotStopProcessing() {
        queue.add(new TestEvent("a"));
        queue.add(new TestEvent("fail"));
        queue.add(new TestEvent("c"));

        // The error must not escape the call to process()
        processor.process();

        assertEquals(2, processor.processed.size());
        assertEquals("a", processor.processed.get(0).name);
        assertEquals("c", processor.processed.get(1).name);
        assertEquals(1, processor.errors.size());
        assertEquals("fail", processor.errors.get(0).event.name);
        assertEquals("failed to process fail", processor.errors.get(0).error.getMessage());
    }

    @Test
    public void testCheckedErrorsAreWrappedInKafkaException() {
        queue.add(new TestEvent("checked"));

        processor.process();

        assertEquals(1, processor.errors.size());
        RuntimeException error = processor.errors.get(0).error;
        assertInstanceOf(KafkaException.class, error);
        assertInstanceOf(IOException.class, error.getCause());
    }

    @Test
    public void testUnexpectedRuntimeFailureIsReportedAsIs() {
        queue.add(new TestEvent(null));

        processor.process();

        assertEquals(1, processor.errors.size());
        assertInstanceOf(NullPointerException.class, processor.errors.get(0).error);
    }

    @Test
    public void testProcessAfterCloseFails() {
        processor.close();

        assertThrows(IllegalStateException.class, () -> processor.process());
    }

    @Test
    public void testCloseIsIdempotent() {
        processor.close();

        // Anything queued after the first close is left alone by subsequent closes, since the closing logic
        // only runs once.
        TestEvent late = new TestEvent("late");
        queue.add(late);
        processor.close();
        processor.close();

        assertFalse(late.future.isDone());
        assertEquals(1, queue.size());
    }

    @Test
    public void testCloseCompletesFuturesOfUnprocessedEvents() {
        TestEvent pending = new TestEvent("pending");
        TestEvent alreadyDone = new TestEvent("done");
        alreadyDone.future.complete("result");
        queue.add(pending);
        queue.add(alreadyDone);

        processor.close();

        assertTrue(pending.future.isCompletedExceptionally());
        ExecutionException e = assertThrows(ExecutionException.class, () -> pending.future.get());
        assertInstanceOf(KafkaException.class, e.getCause());
        assertEquals("result", alreadyDone.future.getNow(null));
        assertTrue(queue.isEmpty());
    }

    private static class TestEvent implements CompletableEvent<String> {
        private final String name;
        private final CompletableFuture<String> future = new CompletableFuture<>();

        private TestEvent(String name) {
            this.name = name;
        }

        @Override
        public CompletableFuture<String> future() {
            return future;
        }

        @Override
        public String toString() {
            return "TestEvent{" + name + "}";
        }
    }

    private static class ProcessingError {
        private final TestEvent event;
        private final RuntimeException error;

        private ProcessingError(TestEvent event, RuntimeException error) {
            this.event = event;
            this.error = error;
        }
    }

    private static class TestEventProcessor extends EventProcessor<TestEvent> {
        private final List<TestEvent> processed = new ArrayList<>();
        private final List<ProcessingError> errors = new ArrayList<>();

        private TestEventProcessor(BlockingQueue<TestEvent> queue) {
            super(new LogContext(), queue);
        }

        @Override
        public void process() {
            process((event, error) -> errors.add(new ProcessingError(event, error)));
        }

        @Override
        public void process(TestEvent event) {
            if ("fail".equals(event.name))
                throw new IllegalStateException("failed to process " + event.name);

            if ("checked".equals(event.name))
                sneakyThrow(new IOException("checked"));

            if (event.name == null)
                throw new NullPointerException("no name");

            processed.add(event);
        }

        @Override
        protected Class<TestEvent> getEventClass() {
            return TestEvent.class;
        }

        @SuppressWarnings("unchecked")
        private static <E extends Throwable> void sneakyThrow(Throwable t) throws E {
            throw (E) t;
        }
    }
}
