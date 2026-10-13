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
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * An {@link EventProcessor} is the means by which events <em>produced</em> by thread <em>A</em> are
 * <em>processed</em> by thread <em>B</em>. By definition, threads <em>A</em> and <em>B</em> run in parallel to
 * each other, so a mechanism is needed with which to receive and process the events from the other thread. That
 * communication channel is provided by the {@link BlockingQueue}.
 *
 * <p/>
 *
 * Event processing happens in a single thread; it is not safe to call {@link #process()} from multiple threads
 * concurrently.
 *
 * @param <T> Type of event
 */
public abstract class EventProcessor<T> implements Closeable {

    private final Logger log;
    private final BlockingQueue<T> eventQueue;
    private final IdempotentCloser closer;

    protected EventProcessor(final LogContext logContext, final BlockingQueue<T> eventQueue) {
        this.log = logContext.logger(EventProcessor.class);
        this.eventQueue = eventQueue;
        this.closer = new IdempotentCloser();
    }

    public abstract void process();

    public abstract void process(T event);

    protected abstract Class<T> getEventClass();

    protected interface ProcessErrorHandler<T> {

        /**
         * Invoked when processing of an event fails.
         *
         * @param event Event that was being processed
         * @param error Error that was raised while processing it; always a {@link RuntimeException}
         */
        void onError(T event, RuntimeException error);
    }

    /**
     * Drains all available events from the queue, and then processes them in order. If processing of an event
     * fails, the error is handed to the given {@link ProcessErrorHandler} rather than being propagated, so a
     * failure does not prevent the remaining events from being processed (unless the handler itself throws).
     */
    protected void process(ProcessErrorHandler<T> processErrorHandler) {
        Objects.requireNonNull(processErrorHandler, "Process error handler must be non-null");
        closer.assertOpen("The processor was previously closed, so no further processing can occur");

        List<T> events = drain();

        if (events.isEmpty()) {
            log.trace("No events to process");
            return;
        }

        try {
            log.trace("Starting processing of {} event{}", events.size(), events.size() == 1 ? "" : "s");

            for (T event : events) {
                try {
                    Objects.requireNonNull(event, "Attempted to process a null event");
                    log.trace("Processing event: {}", event);
                    process(event);
                } catch (Throwable t) {
                    RuntimeException error = t instanceof RuntimeException ? (RuntimeException) t : new KafkaException(t);
                    log.debug("Error processing event {}", event, t);
                    processErrorHandler.onError(event, error);
                }
            }
        } finally {
            log.trace("Completed processing");
        }
    }

    /**
     * Moves all the events from the queue to the returned list.
     */
    protected List<T> drain() {
        List<T> events = new ArrayList<>();
        eventQueue.drainTo(events);
        return events;
    }

    @Override
    public void close() {
        closer.close(this::closeInternal, () -> log.warn("The event processor was already closed"));
    }

    private void closeInternal() {
        log.trace("Closing event processor");
        List<T> incompleteEvents = drain();

        if (incompleteEvents.isEmpty())
            return;

        KafkaException exception = new KafkaException("The consumer is closed");

        // Check each of the events and if it has a Future that is incomplete, complete it exceptionally so
        // whoever is waiting on it is released.
        incompleteEvents.stream()
                .filter(e -> e instanceof CompletableEvent)
                .map(e -> ((CompletableEvent<?>) e).future())
                .filter(f -> !f.isDone())
                .forEach(f -> {
                    log.debug("Completing event future {} exceptionally as the consumer is closing", f);
                    f.completeExceptionally(exception);
                });

        log.debug("Discarding {} events because the consumer is closing", incompleteEvents.size());
    }
}
