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

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * An {@code EventHandler} that uses a single background thread to consume {@code ApplicationEvent} and produce
 * {@code BackgroundEvent} from the {@link ConsumerNetworkThread}.
 */
public class ApplicationEventHandler implements Closeable {

    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final ConsumerNetworkThread networkThread;
    private final IdempotentCloser closer = new IdempotentCloser();

    public ApplicationEventHandler(final LogContext logContext,
                                   final Time time,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                                   final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                   final Supplier<RequestManagers> requestManagersSupplier) {
        this.log = logContext.logger(ApplicationEventHandler.class);
        this.applicationEventQueue = applicationEventQueue;
        this.networkThread = new ConsumerNetworkThread(logContext,
                time,
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier);
        this.networkThread.start();
    }

    /**
     * Add an {@link ApplicationEvent} to the handler and then wake up the network thread to process it.
     *
     * @param event An {@link ApplicationEvent} created by the application thread
     */
    public void add(final ApplicationEvent event) {
        Objects.requireNonNull(event, "ApplicationEvent provided to add must be non-null");
        log.trace("Enqueued event: {}", event);
        applicationEventQueue.add(event);
        wakeupNetworkThread();
    }

    /**
     * Wakes up the network thread, which is blocked in a poll of the network client, so that it can
     * process any pending work.
     */
    public void wakeupNetworkThread() {
        networkThread.wakeup();
    }

    /**
     * Add a {@link CompletableApplicationEvent} to the handler. The method blocks waiting for the result, and will
     * return the result value upon successful completion; otherwise throws an error.
     *
     * <p/>
     *
     * See {@link CompletableApplicationEvent#get(Timer)} and {@link java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)} for more details.
     *
     * @param event A {@link CompletableApplicationEvent} created by the polling thread
     * @param timer Timer for which to wait for the event to complete
     * @return      Value that is the result of the event
     * @param <T>   Type of return value of the event
     */
    public <T> T addAndGet(final CompletableApplicationEvent<T> event, final Timer timer) {
        Objects.requireNonNull(event, "CompletableApplicationEvent provided to addAndGet must be non-null");
        Objects.requireNonNull(timer, "Timer provided to addAndGet must be non-null");
        add(event);
        return event.get(timer);
    }

    @Override
    public void close() {
        close(Duration.ofMillis(ConsumerNetworkThread.DEFAULT_CLOSE_TIMEOUT_MS));
    }

    public void close(final Duration timeout) {
        Objects.requireNonNull(timeout, "Close timeout provided to close must be non-null");

        if (timeout.isNegative())
            throw new IllegalArgumentException("The close timeout (" + timeout.toMillis() + " ms) cannot be negative");

        closer.close(
                () -> {
                    log.debug("Closing the application event handler");
                    networkThread.close(timeout);
                    log.debug("The application event handler was closed");
                },
                () -> log.warn("The application event handler was already closed")
        );
    }
}
