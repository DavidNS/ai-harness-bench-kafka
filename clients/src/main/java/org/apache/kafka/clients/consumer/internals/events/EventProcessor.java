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
 * communication channel is a {@link BlockingQueue} that the producing thread adds to and that this class drains.
 *
 * <p/>
 *
 * When the processor is closed, any {@link CompletableEvent} still in the queue has its future completed
 * exceptionally so that nobody waits on it forever.
 *
 * @param <T> Type of event produced by the other thread and consumed by this processor
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
         * Invoked when the processing of an event resulted in an error.
         *
         * @param event Event whose processing failed
         * @param error Error that was thrown while processing the event
         */
        void onError(T event, RuntimeException error);
    }

    /**
     * Drains all available events from the queue, and then processes them in order. If any errors are thrown while
     * processing an individual event, the error is passed to the {@link ProcessErrorHandler} and processing
     * continues with the next event; the error is never propagated to the caller.
     *
     * @throws IllegalStateException if the processor was previously closed
     */
    protected void process(ProcessErrorHandler<T> processErrorHandler) {
        Objects.requireNonNull(processErrorHandler, "ProcessErrorHandler must be non-null");
        closer.assertOpen("The processor was previously closed, so no further processing can occur");

        List<T> events = drain();

        if (events.isEmpty()) {
            log.trace("No events to process");
            return;
        }

        log.debug("Starting processing of {} {}", events.size(), getEventClass().getSimpleName());

        for (T event : events) {
            try {
                Objects.requireNonNull(event, "Attempted to process a null event");
                log.trace("Processing event: {}", event);
                process(event);
            } catch (Throwable t) {
                RuntimeException error = t instanceof RuntimeException ? (RuntimeException) t : new KafkaException(t);
                processErrorHandler.onError(event, error);
            }
        }
    }

    /**
     * Moves all the events from the queue to the returned list.
     */
    private List<T> drain() {
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

        // Complete the future of any event that was never processed so that nobody waits on it forever.
        incompleteEvents.stream()
                .filter(e -> e instanceof CompletableEvent)
                .map(e -> ((CompletableEvent<?>) e).future())
                .filter(f -> !f.isDone())
                .forEach(f -> f.completeExceptionally(exception));

        log.debug("Discarded {} unprocessed {} because the processor was closed",
                incompleteEvents.size(), getEventClass().getSimpleName());
    }
}
