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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * Background thread runnable that consumes {@link ApplicationEvent} and produces
 * {@link org.apache.kafka.clients.consumer.internals.events.BackgroundEvent}. It uses an event loop to consume and
 * produce events, and poll the network client to handle network IO.
 *
 * <p/>
 *
 * The objects used by this thread (the {@link ApplicationEventProcessor}, the {@link NetworkClientDelegate} and the
 * {@link RequestManagers}) are provided as {@link Supplier suppliers}. They are resolved <em>on this thread</em>,
 * once it has started, so that the application thread never creates nor holds a reference to them.
 */
public class ConsumerNetworkThread extends KafkaThread implements Closeable {

    static final long MAX_POLL_TIMEOUT_MS = 5000;
    public static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;
    private static final String BACKGROUND_THREAD_NAME = "consumer_background_thread";

    private final Time time;
    private final Logger log;
    private final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier;
    private final Supplier<NetworkClientDelegate> networkClientDelegateSupplier;
    private final Supplier<RequestManagers> requestManagersSupplier;
    private ApplicationEventProcessor applicationEventProcessor;
    private volatile NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private volatile boolean running;
    private volatile Duration closeTimeout = Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS);
    private final IdempotentCloser closer = new IdempotentCloser();

    public ConsumerNetworkThread(LogContext logContext,
                                 Time time,
                                 Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                                 Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                 Supplier<RequestManagers> requestManagersSupplier) {
        super(BACKGROUND_THREAD_NAME, true);
        this.time = time;
        this.log = logContext.logger(getClass());
        this.applicationEventProcessorSupplier = applicationEventProcessorSupplier;
        this.networkClientDelegateSupplier = networkClientDelegateSupplier;
        this.requestManagersSupplier = requestManagersSupplier;
        this.running = true;
    }

    @Override
    public void run() {
        try {
            log.debug("Consumer network thread started");

            // Wait until we're securely in the background network thread to initialize these objects. The
            // application thread must neither create nor hold a reference to them.
            initializeResources();

            while (isRunning()) {
                try {
                    runOnce();
                } catch (final WakeupException e) {
                    log.debug("WakeupException caught, consumer network thread won't be interrupted");
                    // swallow the wakeup exception to prevent killing the background thread.
                }
            }
        } catch (final Throwable t) {
            log.error("The consumer network thread failed due to unexpected error", t);
            throw new KafkaException(t);
        } finally {
            cleanup();
        }
    }

    void initializeResources() {
        applicationEventProcessor = applicationEventProcessorSupplier.get();
        networkClientDelegate = networkClientDelegateSupplier.get();
        requestManagers = requestManagersSupplier.get();
    }

    /**
     * Poll and process the {@link ApplicationEvent application events}. It performs the following tasks:
     *
     * <ol>
     *     <li>Drains and processes all the events from the application thread's event queue via the
     *         {@link ApplicationEventProcessor}</li>
     *     <li>Iterate through the {@link RequestManager} instances and invoke {@link RequestManager#poll(long)}
     *         to get the requests to send and the time until the next poll</li>
     *     <li>Poll the network client, using the shortest of those times, to send the requests and retrieve
     *         the responses</li>
     * </ol>
     */
    void runOnce() {
        // Errors thrown while processing an individual event are handled by the processor, so that one bad event
        // doesn't prevent the others from being processed.
        applicationEventProcessor.process();

        final long currentTimeMs = time.milliseconds();
        final long pollWaitTimeMs = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(rm -> rm.poll(currentTimeMs))
                .map(networkClientDelegate::addAll)
                .reduce(MAX_POLL_TIMEOUT_MS, Math::min);
        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs);
    }

    public boolean isRunning() {
        return running;
    }

    public void wakeup() {
        // The delegate is created on this thread, so it may not exist yet if the wakeup came from the application
        // thread early on. That's fine: the first pass through the event loop will process whatever is queued.
        NetworkClientDelegate delegate = networkClientDelegate;

        if (delegate != null)
            delegate.wakeup();
    }

    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    public void close(final Duration timeout) {
        Objects.requireNonNull(timeout, "Close timeout for consumer network thread must be non-null");

        closer.close(
                () -> signalClose(timeout),
                () -> log.warn("The consumer network thread was already closed")
        );
    }

    /**
     * Starts the closing process, then waits for the thread to complete it (see {@link #cleanup()}).
     */
    private void signalClose(final Duration timeout) {
        if (timeout.isNegative())
            throw new IllegalArgumentException("The close timeout (" + timeout.toMillis() + " ms) cannot be negative");

        log.trace("Signaling the consumer network thread to close in {} ms", timeout.toMillis());
        closeTimeout = timeout;
        running = false;
        wakeup();

        // Closing from within the network thread itself (e.g. from a callback) must not wait on itself.
        if (Thread.currentThread() == this)
            return;

        try {
            join();
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for the consumer network thread to complete", e);
            throw new InterruptException(e);
        }
    }

    /**
     * Runs on the network thread once the event loop has exited: gives the request managers a last chance to send
     * their final requests (bounded by the close timeout) and then releases the resources owned by this thread.
     */
    private void cleanup() {
        log.trace("Closing the consumer network thread");
        Timer timer = time.timer(closeTimeout);

        try {
            if (requestManagers != null && networkClientDelegate != null)
                runAtClose(requestManagers.entries(), networkClientDelegate, timer);
        } catch (Exception e) {
            log.error("Unexpected error during shutdown of the consumer network thread", e);
        } finally {
            closeQuietly(requestManagers, "request managers");
            closeQuietly(networkClientDelegate, "network client delegate");
            closeQuietly(applicationEventProcessor, "application event processor");
            log.debug("Closed the consumer network thread");
        }
    }

    /**
     * Polls each {@link RequestManager} for its closing requests ({@link RequestManager#pollOnClose()}) and then
     * keeps polling the network client until the requests have been flushed or the timer expires, whichever comes
     * first.
     */
    // Visible for testing
    static void runAtClose(final Collection<Optional<? extends RequestManager>> requestManagers,
                           final NetworkClientDelegate networkClientDelegate,
                           final Timer timer) {
        // These are the optional outgoing requests that managers need to send before the client is closed.
        requestManagers.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(RequestManager::pollOnClose)
                .forEach(networkClientDelegate::addAll);

        timer.update();

        // Poll to ensure that the requests have been written to the socket, but don't block the shutdown beyond
        // the time allotted.
        while (timer.notExpired() && !networkClientDelegate.unsentRequests().isEmpty()) {
            networkClientDelegate.poll(timer.remainingMs(), timer.currentTimeMs());
            timer.update();
        }
    }
}
