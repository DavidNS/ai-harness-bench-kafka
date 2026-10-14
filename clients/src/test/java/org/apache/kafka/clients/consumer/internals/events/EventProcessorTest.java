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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
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

    private final LogContext logContext = new LogContext();
    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

    private static class CompletableString implements CompletableEvent<String> {

        private final CompletableFuture<String> future = new CompletableFuture<>();

        @Override
        public CompletableFuture<String> future() {
            return future;
        }
    }

    private static class RecordingProcessor extends EventProcessor<Object> {

        private final List<Object> processed = new ArrayList<>();
        private final List<KafkaException> errors = new ArrayList<>();

        RecordingProcessor(LogContext logContext, BlockingQueue<Object> queue) {
            super(logContext, queue);
        }

        @Override
        public void process() {
            process((event, error) -> errors.add(error));
        }

        @Override
        public void process(Object event) {
            if (event instanceof RuntimeException)
                throw (RuntimeException) event;

            processed.add(event);
        }

        @Override
        protected Class<Object> getEventClass() {
            return Object.class;
        }
    }

    @Test
    public void testProcessesAllEventsInOrder() {
        RecordingProcessor processor = new RecordingProcessor(logContext, queue);
        queue.add("a");
        queue.add("b");
        queue.add("c");

        processor.process();

        assertEquals(Arrays.asList("a", "b", "c"), processor.processed);
        assertTrue(processor.errors.isEmpty());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testErrorsArePassedToHandlerAndRemainingEventsAreStillProcessed() {
        RecordingProcessor processor = new RecordingProcessor(logContext, queue);
        IllegalStateException illegalState = new IllegalStateException("bad event");
        KafkaException kafkaException = new KafkaException("kafka bad event");
        queue.add("first");
        queue.add(illegalState);
        queue.add(kafkaException);
        queue.add("last");

        // The errors are handed to the handler rather than thrown.
        processor.process();

        assertEquals(Arrays.asList("first", "last"), processor.processed);
        assertEquals(2, processor.errors.size());

        // A non-Kafka exception is wrapped; a Kafka exception is passed along as is.
        assertInstanceOf(KafkaException.class, processor.errors.get(0));
        assertEquals(illegalState, processor.errors.get(0).getCause());
        assertEquals(kafkaException, processor.errors.get(1));
    }

    @Test
    public void testNoEventsIsANoOp() {
        RecordingProcessor processor = new RecordingProcessor(logContext, queue);
        processor.process();
        assertTrue(processor.processed.isEmpty());
        assertTrue(processor.errors.isEmpty());
    }

    @Test
    public void testCloseCompletesIncompleteEventsExceptionally() {
        RecordingProcessor processor = new RecordingProcessor(logContext, queue);
        CompletableString incomplete = new CompletableString();
        CompletableString alreadyDone = new CompletableString();
        alreadyDone.future().complete("done");
        queue.add(incomplete);
        queue.add(alreadyDone);
        queue.add("not completable");

        processor.close();

        assertTrue(queue.isEmpty());
        assertTrue(incomplete.future().isCompletedExceptionally());
        ExecutionException e = assertThrows(ExecutionException.class, () -> incomplete.future().get());
        assertInstanceOf(KafkaException.class, e.getCause());
        assertEquals("done", alreadyDone.future().getNow(null));
        assertTrue(processor.processed.isEmpty());
    }

    @Test
    public void testCloseIsIdempotent() {
        RecordingProcessor processor = new RecordingProcessor(logContext, queue);
        CompletableString first = new CompletableString();
        queue.add(first);

        processor.close();
        assertTrue(first.future().isCompletedExceptionally());

        // Events added after the first close are not touched by a repeated close.
        CompletableString second = new CompletableString();
        queue.add(second);
        processor.close();

        assertEquals(1, queue.size());
        assertFalse(second.future().isDone());
    }
}
