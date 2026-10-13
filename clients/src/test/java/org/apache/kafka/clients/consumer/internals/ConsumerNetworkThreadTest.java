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
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerNetworkThreadTest {

    private static final long WAIT_MS = 10_000;

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private ApplicationEventProcessor applicationEventProcessor;
    private NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private RequestManager requestManager;
    private ConsumerNetworkThread networkThread;

    @BeforeEach
    public void setup() throws Exception {
        applicationEventProcessor = mock(ApplicationEventProcessor.class);
        networkClientDelegate = mock(NetworkClientDelegate.class);
        requestManagers = mock(RequestManagers.class);
        requestManager = mock(RequestManager.class);

        when(requestManager.poll(anyLong())).thenReturn(new PollResult(100));
        when(requestManager.pollOnClose()).thenReturn(PollResult.EMPTY);
        when(requestManagers.entries()).thenReturn(Collections.singletonList(Optional.of(requestManager)));
        when(networkClientDelegate.addAll(any(PollResult.class)))
                .thenAnswer(invocation -> ((PollResult) invocation.getArgument(0)).timeUntilNextPollMs);
        when(networkClientDelegate.unsentRequests()).thenReturn(new ArrayDeque<>());

        networkThread = new ConsumerNetworkThread(logContext,
                time,
                () -> applicationEventProcessor,
                () -> networkClientDelegate,
                () -> requestManagers);
    }

    @AfterEach
    public void tearDown() {
        if (networkThread != null)
            networkThread.close(Duration.ZERO);
    }

    @Test
    public void testResourcesAreResolvedOnTheNetworkThreadOnly() throws Exception {
        AtomicReference<Thread> processorThread = new AtomicReference<>();
        AtomicReference<Thread> delegateThread = new AtomicReference<>();
        AtomicReference<Thread> managersThread = new AtomicReference<>();
        Supplier<ApplicationEventProcessor> processorSupplier = () -> {
            processorThread.set(Thread.currentThread());
            return applicationEventProcessor;
        };
        Supplier<NetworkClientDelegate> delegateSupplier = () -> {
            delegateThread.set(Thread.currentThread());
            return networkClientDelegate;
        };
        Supplier<RequestManagers> managersSupplier = () -> {
            managersThread.set(Thread.currentThread());
            return requestManagers;
        };

        networkThread = new ConsumerNetworkThread(logContext, time, processorSupplier, delegateSupplier, managersSupplier);

        // Creating the thread must not touch any of the suppliers.
        assertNull(processorThread.get());
        assertNull(delegateThread.get());
        assertNull(managersThread.get());

        networkThread.start();
        waitForCondition(() -> managersThread.get() != null && delegateThread.get() != null && processorThread.get() != null,
                WAIT_MS, "Suppliers were not resolved");

        assertEquals(networkThread, processorThread.get());
        assertEquals(networkThread, delegateThread.get());
        assertEquals(networkThread, managersThread.get());
        assertNotEquals(Thread.currentThread(), networkThread);
    }

    @Test
    public void testStartupAndTearDown() throws Exception {
        networkThread.start();

        waitForCondition(networkThread::isRunning, WAIT_MS, "The thread did not start");
        verify(applicationEventProcessor, timeout(WAIT_MS).atLeastOnce()).process();
        verify(networkClientDelegate, timeout(WAIT_MS).atLeastOnce()).poll(anyLong(), anyLong());

        networkThread.close(Duration.ofSeconds(5));

        assertFalse(networkThread.isRunning());
        assertFalse(networkThread.isAlive());
        verify(requestManager).pollOnClose();
        verify(requestManagers).close();
        verify(networkClientDelegate).close();
        verify(applicationEventProcessor).close();
    }

    @Test
    public void testRunOnce() {
        networkThread.initializeResources();
        networkThread.runOnce();

        InOrder order = inOrder(applicationEventProcessor, requestManager, networkClientDelegate);
        order.verify(applicationEventProcessor).process();
        order.verify(requestManager).poll(time.milliseconds());
        order.verify(networkClientDelegate).addAll(any(PollResult.class));
        order.verify(networkClientDelegate).poll(100, time.milliseconds());
    }

    @Test
    public void testRunOnceUsesShortestPollTimeoutOfAllRequestManagers() {
        RequestManager slow = mock(RequestManager.class);
        RequestManager fast = mock(RequestManager.class);
        when(slow.poll(anyLong())).thenReturn(new PollResult(2000));
        when(fast.poll(anyLong())).thenReturn(new PollResult(10));
        when(requestManagers.entries()).thenReturn(Arrays.asList(Optional.of(slow), Optional.empty(), Optional.of(fast)));

        networkThread.initializeResources();
        networkThread.runOnce();

        verify(slow).poll(time.milliseconds());
        verify(fast).poll(time.milliseconds());
        verify(networkClientDelegate).poll(10, time.milliseconds());
    }

    @Test
    public void testRunOnceCapsTheNetworkPollTimeout() {
        when(requestManager.poll(anyLong())).thenReturn(PollResult.EMPTY);

        networkThread.initializeResources();
        networkThread.runOnce();

        verify(networkClientDelegate).poll(MAX_POLL_TIMEOUT_MS, time.milliseconds());
    }

    @Test
    public void testRunOnceHandsTheUnsentRequestsToTheNetworkClient() {
        UnsentRequest unsent = newUnsentRequest();
        PollResult pollResult = new PollResult(50, Collections.singletonList(unsent));
        when(requestManager.poll(anyLong())).thenReturn(pollResult);

        networkThread.initializeResources();
        networkThread.runOnce();

        verify(networkClientDelegate).addAll(pollResult);
    }

    @Test
    public void testWakeupBeforeAndAfterInitialization() {
        // The delegate doesn't exist until the thread resolves the suppliers, so there is nothing to wake up yet.
        networkThread.wakeup();
        verify(networkClientDelegate, never()).wakeup();

        networkThread.initializeResources();
        networkThread.wakeup();
        verify(networkClientDelegate).wakeup();
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        networkThread.start();
        waitForCondition(networkThread::isRunning, WAIT_MS, "The thread did not start");

        networkThread.close(Duration.ofSeconds(5));
        networkThread.close(Duration.ofSeconds(5));
        networkThread.close();

        verify(networkClientDelegate, times(1)).close();
        verify(requestManagers, times(1)).close();
        verify(applicationEventProcessor, times(1)).close();
    }

    @Test
    public void testCloseWithNegativeTimeoutFails() {
        assertThrows(IllegalArgumentException.class, () -> networkThread.close(Duration.ofMillis(-1)));
    }

    @Test
    public void testResourcesAreClosedWhenTheThreadFails() throws Exception {
        doThrow(new IllegalStateException("boom")).when(applicationEventProcessor).process();

        networkThread.setUncaughtExceptionHandler((t, e) -> { });
        networkThread.start();

        verify(networkClientDelegate, timeout(WAIT_MS)).close();
        verify(requestManagers, timeout(WAIT_MS)).close();
        verify(applicationEventProcessor, timeout(WAIT_MS)).close();
    }

    @Test
    public void testRunAtCloseFlushesTheClosingRequests() {
        UnsentRequest unsent = newUnsentRequest();
        PollResult closePollResult = new PollResult(Collections.singletonList(unsent));
        RequestManager closingManager = mock(RequestManager.class);
        when(closingManager.pollOnClose()).thenReturn(closePollResult);

        Queue<UnsentRequest> unsentRequests = new ArrayDeque<>();
        when(networkClientDelegate.addAll(closePollResult)).thenAnswer(invocation -> {
            unsentRequests.addAll(closePollResult.unsentRequests);
            return closePollResult.timeUntilNextPollMs;
        });
        when(networkClientDelegate.unsentRequests()).thenReturn(unsentRequests);
        // The request is sent (and therefore removed from the unsent queue) by the network poll.
        doAnswer(invocation -> {
            unsentRequests.clear();
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        Collection<Optional<? extends RequestManager>> managers = Arrays.asList(Optional.empty(), Optional.of(closingManager));
        Timer timer = time.timer(1000);

        ConsumerNetworkThread.runAtClose(managers, networkClientDelegate, timer);

        verify(closingManager).pollOnClose();
        verify(networkClientDelegate).addAll(closePollResult);
        verify(networkClientDelegate, times(1)).poll(anyLong(), anyLong());
        assertTrue(unsentRequests.isEmpty());
    }

    @Test
    public void testRunAtCloseWithoutRequestsDoesNotPoll() {
        RequestManager closingManager = mock(RequestManager.class);
        when(closingManager.pollOnClose()).thenReturn(PollResult.EMPTY);

        ConsumerNetworkThread.runAtClose(Collections.singletonList(Optional.of(closingManager)),
                networkClientDelegate,
                time.timer(1000));

        verify(closingManager).pollOnClose();
        verify(networkClientDelegate, never()).poll(anyLong(), anyLong());
    }

    @Test
    public void testRunAtCloseGivesUpWhenTheTimerExpires() {
        UnsentRequest unsent = newUnsentRequest();
        PollResult closePollResult = new PollResult(Collections.singletonList(unsent));
        RequestManager closingManager = mock(RequestManager.class);
        when(closingManager.pollOnClose()).thenReturn(closePollResult);

        // The requests never get sent, e.g. because the node is unreachable
        Queue<UnsentRequest> unsentRequests = new ArrayDeque<>(Collections.singletonList(unsent));
        when(networkClientDelegate.unsentRequests()).thenReturn(unsentRequests);
        doAnswer(invocation -> {
            long timeoutMs = invocation.getArgument(0);
            time.sleep(timeoutMs);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        Timer timer = time.timer(500);
        ConsumerNetworkThread.runAtClose(Collections.singletonList(Optional.of(closingManager)), networkClientDelegate, timer);

        assertTrue(timer.isExpired());
        verify(networkClientDelegate, atLeastOnce()).poll(anyLong(), anyLong());
    }

    @Test
    public void testRunAtCloseOnTheThreadUsesPollOnCloseOfEveryManager() throws Exception {
        List<Optional<? extends RequestManager>> entries = Arrays.asList(Optional.of(requestManager), Optional.of(mock(RequestManager.class)));
        when(requestManagers.entries()).thenReturn(entries);
        entries.forEach(e -> when(e.get().pollOnClose()).thenReturn(PollResult.EMPTY));

        networkThread.start();
        waitForCondition(networkThread::isRunning, WAIT_MS, "The thread did not start");
        networkThread.close(Duration.ofSeconds(5));

        entries.forEach(e -> verify(e.get()).pollOnClose());
    }

    private static UnsentRequest newUnsentRequest() {
        return new UnsentRequest(
                new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKey("group")),
                Optional.empty());
    }

    @Test
    public void testThreadUsesTheInjectedClock() {
        Time clock = mock(Time.class);
        when(clock.milliseconds()).thenReturn(42L);
        ConsumerNetworkThread thread = new ConsumerNetworkThread(logContext,
                clock,
                () -> applicationEventProcessor,
                () -> networkClientDelegate,
                () -> requestManagers);

        thread.initializeResources();
        thread.runOnce();

        verify(requestManager).poll(42L);
        verify(networkClientDelegate).poll(100, 42L);
    }
}
