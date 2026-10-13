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

import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * An {@link EventProcessor} is the means by which events <em>produced</em> by thread <em>A</em> are
 * <em>processed</em> by thread <em>B</em>. By definition, threads <em>A</em> and <em>B</em> run in parallel to
 * each other, so a thread-safe queue is used to pass the events between them.
 *
 * <p/>
 *
 * Because the events are produced and processed by different threads, the processing is not done on the
 * producing thread: it is up to the processing thread to call {@link #process()} (or one of its variants) to
 * drain the queue and handle each event.
 *
 * <p/>
 *
 * Closing the processor is idempotent. When it is closed, any events remaining in the queue that have a
 * {@link CompletableEvent#future() future} are completed exceptionally so nobody waits on them forever.
 *
 * @param <T> Event type that is processed
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

    /**
     * Drains the queue and processes each of the events that were in it. How errors are handled is up to the
     * implementation.
     */
    public abstract void process();

    /**
     * Processes a single event.
     *
     * @param event Event to process
     */
    public abstract void process(T event);

    /**
     * Returns the type of event processed, used for error messages.
     */
    protected abstract Class<T> getEventClass();

    protected interface ProcessErrorHandler<T> {

        /**
         * Invoked if processing of the event failed.
         *
         * @param event Event whose processing failed
         * @param error Error that was thrown while processing it, wrapped as a {@link RuntimeException}
         */
        void onError(T event, RuntimeException error);
    }

    /**
     * Drains all available events from the queue and processes them one at a time. If the processing of an event
     * throws, the error is given to the {@link ProcessErrorHandler} and the remaining events are still processed.
     * Only if the handler itself throws is the exception propagated to the caller (and the events following the
     * failed one in this batch are not processed).
     *
     * @param processErrorHandler Invoked for each error thrown while processing an event
     */
    protected void process(ProcessErrorHandler<T> processErrorHandler) {
        String eventClassName = getEventClass().getSimpleName();
        closer.assertOpen(() -> String.format("The processor was previously closed, so no further %s processing can occur", eventClassName));

        List<T> events = drain();

        if (events.isEmpty()) {
            log.trace("No events to process");
            return;
        }

        try {
            log.trace("Starting processing of {} {}(s)", events.size(), eventClassName);

            for (T event : events) {
                try {
                    Objects.requireNonNull(event, () -> String.format("Attempted to process a null %s", eventClassName));
                    log.trace("Processing event: {}", event);
                    process(event);
                } catch (Throwable t) {
                    RuntimeException error = ConsumerUtils.maybeWrapAsKafkaException(t);
                    log.debug("An error occurred when processing the event: {}", error.getMessage(), error);
                    processErrorHandler.onError(event, error);
                }
            }
        } finally {
            log.trace("Completed processing");
        }
    }

    /**
     * It is possible for the consumer to close before complete processing all the events in the queue. In
     * this case, we need to throw an exception to notify the user the consumer is closed.
     */
    private void closeInternal() {
        log.trace("Closing event processor");
        List<T> incompleteEvents = drain();

        if (incompleteEvents.isEmpty())
            return;

        KafkaException exception = new KafkaException("The consumer is closed");

        // Check each of the events and if it has a Future that is incomplete, complete it exceptionally.
        incompleteEvents
                .stream()
                .filter(e -> e instanceof CompletableEvent)
                .map(e -> ((CompletableEvent<?>) e).future())
                .filter(f -> !f.isDone())
                .forEach(f -> f.completeExceptionally(exception));

        log.debug("Discarding {} {}s because the consumer is closing", incompleteEvents.size(), getEventClass().getSimpleName());
    }

    /**
     * Moves all the events from the queue to the returned list.
     */
    private List<T> drain() {
        LinkedList<T> events = new LinkedList<>();
        eventQueue.drainTo(events);
        return events;
    }

    @Override
    public void close() {
        closer.close(this::closeInternal, () -> log.warn("The event processor was already closed"));
    }
}
