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
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerNetworkThreadTest {

    private static final long DEFAULT_MAX_WAIT_MS = 10000;

    private final LogContext logContext = new LogContext();
    private Time time;
    private ApplicationEventProcessor applicationEventProcessor;
    private NetworkClientDelegate networkClient;
    private RequestManagers requestManagers;
    private OffsetsRequestManager offsetsRequestManager;
    private TopicMetadataRequestManager topicMetadataRequestManager;
    private FetchRequestManager fetchRequestManager;
    private CoordinatorRequestManager coordinatorRequestManager;
    private CommitRequestManager commitRequestManager;
    private HeartbeatRequestManager heartbeatRequestManager;
    private ConsumerNetworkThread consumerNetworkThread;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        applicationEventProcessor = mock(ApplicationEventProcessor.class);
        networkClient = mock(NetworkClientDelegate.class);
        offsetsRequestManager = mock(OffsetsRequestManager.class);
        topicMetadataRequestManager = mock(TopicMetadataRequestManager.class);
        fetchRequestManager = mock(FetchRequestManager.class);
        coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        commitRequestManager = mock(CommitRequestManager.class);
        heartbeatRequestManager = mock(HeartbeatRequestManager.class);

        for (RequestManager manager : allManagers()) {
            when(manager.poll(anyLong())).thenReturn(PollResult.EMPTY);
            when(manager.pollOnClose()).thenReturn(PollResult.EMPTY);
        }

        when(networkClient.addAll(any(PollResult.class))).thenAnswer(invocation -> {
            PollResult pollResult = invocation.getArgument(0);
            return pollResult.timeUntilNextPollMs;
        });
        when(networkClient.unsentRequests()).thenReturn(new ArrayDeque<>());

        requestManagers = new RequestManagers(logContext,
                offsetsRequestManager,
                topicMetadataRequestManager,
                fetchRequestManager,
                Optional.of(coordinatorRequestManager),
                Optional.of(commitRequestManager),
                Optional.of(heartbeatRequestManager));
        consumerNetworkThread = new ConsumerNetworkThread(logContext,
                time,
                () -> applicationEventProcessor,
                () -> networkClient,
                () -> requestManagers);
    }

    @AfterEach
    public void tearDown() {
        if (consumerNetworkThread != null)
            consumerNetworkThread.close(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        consumerNetworkThread.start();

        TestCondition isStarted = () -> consumerNetworkThread.isRunning();
        TestCondition isClosed = () -> !(consumerNetworkThread.isRunning() || consumerNetworkThread.isAlive());

        // There's a nonzero amount of time between starting the thread and having it begin to execute our code.
        TestUtils.waitForCondition(isStarted,
                DEFAULT_MAX_WAIT_MS,
                "The consumer network thread did not start within " + DEFAULT_MAX_WAIT_MS + " ms");

        consumerNetworkThread.close(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));

        TestUtils.waitForCondition(isClosed,
                DEFAULT_MAX_WAIT_MS,
                "The consumer network thread did not stop within " + DEFAULT_MAX_WAIT_MS + " ms");
    }

    @Test
    public void testResourcesAreResolvedOnTheBackgroundThread() throws InterruptedException {
        AtomicReference<Thread> processorThread = new AtomicReference<>();
        AtomicReference<Thread> clientThread = new AtomicReference<>();
        AtomicReference<Thread> managersThread = new AtomicReference<>();

        consumerNetworkThread = new ConsumerNetworkThread(logContext,
                time,
                () -> {
                    processorThread.set(Thread.currentThread());
                    return applicationEventProcessor;
                },
                () -> {
                    clientThread.set(Thread.currentThread());
                    return networkClient;
                },
                () -> {
                    managersThread.set(Thread.currentThread());
                    return requestManagers;
                });

        // Nothing is instantiated by the thread that creates the network thread.
        assertNull(processorThread.get());
        assertNull(clientThread.get());
        assertNull(managersThread.get());

        consumerNetworkThread.start();
        TestUtils.waitForCondition(() -> managersThread.get() != null && clientThread.get() != null && processorThread.get() != null,
                DEFAULT_MAX_WAIT_MS,
                "The suppliers were not invoked");

        assertSame(consumerNetworkThread, processorThread.get());
        assertSame(consumerNetworkThread, clientThread.get());
        assertSame(consumerNetworkThread, managersThread.get());
        assertNotSame(Thread.currentThread(), clientThread.get());
    }

    @Test
    public void testRunOnceProcessesEventsAndPollsRequestManagers() {
        consumerNetworkThread.initializeResources();
        consumerNetworkThread.runOnce();

        verify(applicationEventProcessor).process();

        for (RequestManager manager : allManagers())
            verify(manager).poll(anyLong());

        verify(networkClient, times(allManagers().size())).addAll(any(PollResult.class));
        verify(networkClient).poll(anyLong(), anyLong());
    }

    @Test
    public void testRunOncePollsNetworkWithShortestTimeUntilNextPoll() {
        when(commitRequestManager.poll(anyLong())).thenReturn(new PollResult(25L));
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(new PollResult(100L));

        consumerNetworkThread.initializeResources();
        consumerNetworkThread.runOnce();

        verify(networkClient).poll(eq(25L), anyLong());
    }

    @Test
    public void testRunOnceHandsRequestsToNetworkClient() {
        PollResult result = new PollResult(Collections.emptyList());
        when(fetchRequestManager.poll(anyLong())).thenReturn(result);

        consumerNetworkThread.initializeResources();
        consumerNetworkThread.runOnce();

        verify(networkClient).addAll(result);
    }

    @Test
    public void testAbsentRequestManagersAreNotPolled() {
        requestManagers = new RequestManagers(logContext,
                offsetsRequestManager,
                topicMetadataRequestManager,
                fetchRequestManager,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        consumerNetworkThread.initializeResources();
        consumerNetworkThread.runOnce();

        verify(coordinatorRequestManager, never()).poll(anyLong());
        verify(commitRequestManager, never()).poll(anyLong());
        verify(heartbeatRequestManager, never()).poll(anyLong());
        verify(offsetsRequestManager).poll(anyLong());
        verify(topicMetadataRequestManager).poll(anyLong());
        verify(fetchRequestManager).poll(anyLong());
    }

    @Test
    public void testWakeupBeforeInitializationIsIgnored() {
        consumerNetworkThread.wakeup();
        verify(networkClient, never()).wakeup();
    }

    @Test
    public void testWakeupWakesUpNetworkClient() {
        consumerNetworkThread.initializeResources();
        consumerNetworkThread.wakeup();
        verify(networkClient).wakeup();
    }

    @Test
    public void testRunAtCloseInvokesPollOnCloseAndFlushesRequests() {
        Timer timer = time.timer(1000);
        PollResult closeResult = new PollResult(Collections.emptyList());
        when(fetchRequestManager.pollOnClose()).thenReturn(closeResult);

        ConsumerNetworkThread.runAtClose(requestManagers.entries(), networkClient, timer);

        for (RequestManager manager : allManagers())
            verify(manager).pollOnClose();

        verify(networkClient).addAll(closeResult);
        verify(networkClient, atLeastOnce()).poll(anyLong(), anyLong());
    }

    @Test
    public void testRunAtCloseStopsWhenTimerExpires() {
        // The requests are never flushed, so only the timer can stop the loop.
        Queue<NetworkClientDelegate.UnsentRequest> neverEmpty = new ArrayDeque<>();
        neverEmpty.add(mock(NetworkClientDelegate.UnsentRequest.class));
        when(networkClient.unsentRequests()).thenReturn(neverEmpty);
        doAnswer(invocation -> {
            time.sleep(100);
            return null;
        }).when(networkClient).poll(anyLong(), anyLong());

        Timer timer = time.timer(450);
        ConsumerNetworkThread.runAtClose(requestManagers.entries(), networkClient, timer);

        assertTrue(timer.isExpired());
        verify(networkClient, times(5)).poll(anyLong(), anyLong());
    }

    @Test
    public void testRunAtCloseSkipsAbsentManagers() {
        Collection<Optional<? extends RequestManager>> managers = Collections.singletonList(Optional.empty());
        ConsumerNetworkThread.runAtClose(managers, networkClient, time.timer(1000));
        verify(networkClient, never()).addAll(any(PollResult.class));
    }

    @Test
    public void testCleanupClosesResources() throws Exception {
        consumerNetworkThread.initializeResources();
        consumerNetworkThread.cleanup();

        assertFalse(consumerNetworkThread.isRunning());
        for (RequestManager manager : allManagers())
            verify(manager).pollOnClose();

        verify(networkClient).close();
        verify(applicationEventProcessor).close();
    }

    @Test
    public void testCloseIsIdempotent() throws InterruptedException {
        consumerNetworkThread.start();
        TestUtils.waitForCondition(() -> consumerNetworkThread.isRunning(), DEFAULT_MAX_WAIT_MS, "Thread did not start");

        consumerNetworkThread.close(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));
        consumerNetworkThread.close(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));
        consumerNetworkThread.close();

        assertFalse(consumerNetworkThread.isAlive());
        assertNotNull(requestManagers);
        // The resources are closed by the thread only once, even though close() was invoked repeatedly.
        verify(applicationEventProcessor, times(1)).close();
    }

    @Test
    public void testEntriesContainEveryManager() {
        List<RequestManager> present = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        assertEquals(6, present.size());
        assertTrue(present.contains(fetchRequestManager));
    }

    private List<RequestManager> allManagers() {
        return Arrays.asList(offsetsRequestManager,
                topicMetadataRequestManager,
                fetchRequestManager,
                coordinatorRequestManager,
                commitRequestManager,
                heartbeatRequestManager);
    }
}
