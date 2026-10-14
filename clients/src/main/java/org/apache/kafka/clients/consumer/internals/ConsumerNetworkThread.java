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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Background thread runnable that consumes {@link ApplicationEvent application events} and
 * produces {@link org.apache.kafka.clients.consumer.internals.events.BackgroundEvent background events}. It uses an
 * event loop to consume and produce events, and poll the network client to handle network I/O.
 *
 * <p/>
 *
 * The {@link ApplicationEventProcessor}, {@link NetworkClientDelegate} and {@link RequestManagers} are not created
 * by the application thread. Instead, their suppliers are invoked from this thread (see
 * {@link #initializeResources()}) so that the application thread never holds a reference to them.
 */
public class ConsumerNetworkThread extends KafkaThread implements Closeable {

    private static final long MAX_POLL_TIMEOUT_MS = 5000;
    private static final String BACKGROUND_THREAD_NAME = "consumer_background_thread";
    private static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    private final Time time;
    private final Logger log;
    private final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier;
    private final Supplier<NetworkClientDelegate> networkClientDelegateSupplier;
    private final Supplier<RequestManagers> requestManagersSupplier;
    private ApplicationEventProcessor applicationEventProcessor;
    private volatile NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private volatile boolean running;
    private final IdempotentCloser closer = new IdempotentCloser();
    private volatile Duration closeTimeout = Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS);

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

            // Wait until we're securely in the background network thread to initialize these objects. Doing it
            // here ensures the application thread never gets a reference to them.
            initializeResources();

            while (running) {
                try {
                    runOnce();
                } catch (final WakeupException e) {
                    log.debug("WakeupException caught, consumer network thread won't be interrupted");
                    // swallow the wakeup exception to prevent killing the thread.
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
     *     <li>Drains and processes all the events from the application thread's application event queue via the
     *     {@link ApplicationEventProcessor}</li>
     *     <li>Iterate through the {@link RequestManager} list and invoke {@link RequestManager#poll(long)} to get
     *     the {@link NetworkClientDelegate.UnsentRequest} list and the poll time for the network poll</li>
     *     <li>Stage each request to be sent via
     *     {@link NetworkClientDelegate#addAll(NetworkClientDelegate.PollResult)}</li>
     *     <li>Poll the network client to send the requests, as well as retrieve any available responses</li>
     * </ol>
     */
    void runOnce() {
        // Any errors thrown while processing the events are handled by the processor.
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

    /**
     * Performs any final cleanup work: lets the request managers send their closing requests, waits for them to be
     * flushed (or for the close timer to expire) and then releases the resources owned by this thread.
     */
    static void runAtClose(final Collection<Optional<? extends RequestManager>> requestManagers,
                           final NetworkClientDelegate networkClientDelegate,
                           final Timer timer) {
        // These are the optional outgoing requests at the time of closing, such as closing the fetch sessions.
        requestManagers.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(RequestManager::pollOnClose)
                .forEach(networkClientDelegate::addAll);

        long pollWaitTimeMs = timer.remainingMs();

        // Poll to ensure that the requests have been written to the socket. Wait until either the timer has expired
        // or until all the requests have been sent.
        do {
            pollWaitTimeMs = Math.min(pollWaitTimeMs, timer.remainingMs());
            networkClientDelegate.poll(pollWaitTimeMs, timer.currentTimeMs());
            timer.update();
        } while (timer.notExpired() && !networkClientDelegate.unsentRequests().isEmpty());
    }

    public boolean isRunning() {
        return running;
    }

    public void wakeup() {
        // The network client delegate is created on this thread, so it may not be available yet.
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

        closer.close(() -> {
            log.trace("Signaling the consumer network thread to close in {} milliseconds", timeout);
            running = false;
            closeTimeout = timeout;
            wakeup();

            try {
                join();
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for consumer network thread to complete", e);
            }
        }, () -> log.warn("The consumer network thread was already closed"));
    }

    void cleanup() {
        running = false;

        log.trace("Closing the consumer network thread");
        Timer timer = time.timer(closeTimeout);

        try {
            if (requestManagers != null && networkClientDelegate != null)
                runAtClose(requestManagers.entries(), networkClientDelegate, timer);
        } catch (Exception e) {
            log.error("Unexpected error during shutdown. Proceeding with closing.", e);
        } finally {
            Utils.closeQuietly(requestManagers, "request managers");
            Utils.closeQuietly(networkClientDelegate, "network client delegate");
            Utils.closeQuietly(applicationEventProcessor, "application event processor");
            log.debug("Closed the consumer network thread");
        }
    }
}
