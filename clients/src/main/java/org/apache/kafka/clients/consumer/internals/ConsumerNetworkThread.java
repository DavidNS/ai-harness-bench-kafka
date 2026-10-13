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

import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * Background thread runnable that consumes {@link ApplicationEvent} and
 * produces {@link org.apache.kafka.clients.consumer.internals.events.BackgroundEvent}. It uses an event loop to
 * consume and produce events, and poll the network client to handle network IO.
 *
 * <p/>
 *
 * It holds a reference to the {@link SubscriptionState}, which is initialized by the polling thread.
 *
 * <p/>
 *
 * The objects this thread works with (the {@link ApplicationEventProcessor}, the {@link NetworkClientDelegate} and the
 * {@link RequestManagers}) are only created, via the suppliers given to the constructor, once this thread is running.
 * That way the application thread never holds a reference to them.
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
    private volatile boolean running = true;
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
    }

    @Override
    public void run() {
        closer.assertOpen("Consumer network thread is already closed");

        try {
            log.debug("Consumer network thread started");

            // Wait until we're securely in the background thread to initialize these objects.
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
            log.error("The consumer network thread terminated with an unexpected exception", t);
        } finally {
            running = false;
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
     *     <li>Stage each {@link NetworkClientDelegate.UnsentRequest} to be sent via
     *     {@link NetworkClientDelegate#addAll(PollResult)}</li>
     *     <li>Poll the client via {@link NetworkClientDelegate#poll(long, long)} to send the requests, as well as
     *     retrieve any responses</li>
     * </ol>
     */
    void runOnce() {
        // If there are errors processing any events, the error will be thrown immediately. This will have
        // the effect of closing the background thread.
        applicationEventProcessor.process();

        final long currentTimeMs = time.milliseconds();
        final long pollWaitTimeMs = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(rm -> rm.poll(currentTimeMs))
                .mapToLong(networkClientDelegate::addAll)
                .reduce(MAX_POLL_TIMEOUT_MS, Math::min);
        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs);
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Wakes up the network client if it is blocked waiting for I/O. This is a no-op if the thread has not yet
     * created its resources.
     */
    public void wakeup() {
        // The network client can be null if the initializeResources method has not yet been called.
        NetworkClientDelegate delegate = networkClientDelegate;

        if (delegate != null)
            delegate.wakeup();
    }

    @Override
    public void close() {
        close(closeTimeout);
    }

    /**
     * Stops the thread, waiting up to the given timeout for the thread to complete its shutdown (during which the
     * request managers are given the chance to send their final requests).
     */
    public void close(final Duration timeout) {
        Objects.requireNonNull(timeout, "Close timeout for consumer network thread must be non-null");

        closer.close(
                () -> closeInternal(timeout),
                () -> log.warn("The consumer network thread was already closed")
        );
    }

    /**
     * Starts the closing process.
     *
     * <p/>
     *
     * This method is called from the application thread, but our resources are owned by the network thread. As such,
     * we don't actually close any of those resources here, immediately, on the application thread. Instead, we just
     * update our internal state on the application thread. When the network thread next
     * {@link #run() executes its loop}, it will notice that state, cease processing any further events, and begin
     * {@link #cleanup() closing its own resources}.
     *
     * @param timeout Upper bound of the time spent waiting for the network thread to shut down
     */
    private void closeInternal(final Duration timeout) {
        long timeoutMs = timeout.toMillis();

        if (timeoutMs < 0)
            throw new IllegalArgumentException("Close timeout for consumer network thread must be >= 0");

        log.trace("Signaling the consumer network thread to close in {} milliseconds", timeoutMs);
        closeTimeout = timeout;
        running = false;
        wakeup();

        if (Thread.currentThread() == this)
            return;

        try {
            join(timeoutMs);
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for consumer network thread to close", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes the resources owned by this thread: first the request managers get a last chance to send their
     * requests (see {@link #runAtClose(Collection, NetworkClientDelegate, Timer)}), then everything is closed.
     */
    void cleanup() {
        log.trace("Closing the consumer network thread");
        Timer timer = time.timer(closeTimeout);

        try {
            if (requestManagers != null && networkClientDelegate != null)
                runAtClose(requestManagers.entries(), networkClientDelegate, timer);
        } catch (Exception e) {
            log.error("Unexpected error during shutdown. Proceed with closing.", e);
        } finally {
            closeQuietly(requestManagers, "request managers");
            closeQuietly(networkClientDelegate, "network client delegate");
            closeQuietly(applicationEventProcessor, "application event processor");
            log.debug("Closed the consumer network thread");
        }
    }

    /**
     * Gives each of the {@link RequestManager request managers} the chance to send their final requests, and polls
     * the {@link NetworkClientDelegate} until the requests have completed or the timer expires, whichever happens
     * first. The shutdown is never blocked longer than the timer allows.
     */
    // visible for testing
    static void runAtClose(final Collection<Optional<? extends RequestManager>> requestManagers,
                           final NetworkClientDelegate networkClientDelegate,
                           final Timer timer) {
        // These are the optional final requests of the managers, such as closing the fetch sessions.
        final List<CompletableFuture<?>> futures = new ArrayList<>();

        requestManagers.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(RequestManager::pollOnClose)
                .forEach(pollResult -> {
                    pollResult.unsentRequests.forEach(r -> futures.add(r.future()));
                    networkClientDelegate.addAll(pollResult);
                });

        // Poll to ensure that the requests have been written to the socket. Wait until either the timer has
        // expired or until all requests have received a response.
        while (timer.notExpired() && !futures.stream().allMatch(CompletableFuture::isDone)) {
            long currentTimeMs = timer.currentTimeMs();
            networkClientDelegate.poll(timer.remainingMs(), currentTimeMs);
            timer.update();
        }
    }
}
