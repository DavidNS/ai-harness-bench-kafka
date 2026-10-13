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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerNetworkThreadTest {

    private final LogContext logContext = new LogContext();
    private MockTime time;
    private ApplicationEventProcessor applicationEventProcessor;
    private NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private CoordinatorRequestManager coordinatorRequestManager;
    private CommitRequestManager commitRequestManager;
    private ConsumerNetworkThread consumerNetworkThread;

    private final AtomicInteger processorSupplierCalls = new AtomicInteger();
    private final AtomicInteger delegateSupplierCalls = new AtomicInteger();
    private final AtomicInteger managersSupplierCalls = new AtomicInteger();
    private final AtomicReference<Thread> supplierThread = new AtomicReference<>();

    @BeforeEach
    public void setup() {
        time = new MockTime(0);
        applicationEventProcessor = mock(ApplicationEventProcessor.class);
        networkClientDelegate = mock(NetworkClientDelegate.class);
        requestManagers = mock(RequestManagers.class);
        coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        commitRequestManager = mock(CommitRequestManager.class);

        when(coordinatorRequestManager.poll(anyLong())).thenReturn(PollResult.EMPTY);
        when(commitRequestManager.poll(anyLong())).thenReturn(PollResult.EMPTY);
        when(requestManagers.entries()).thenReturn(Arrays.asList(
                Optional.of(coordinatorRequestManager),
                Optional.empty(),
                Optional.of(commitRequestManager)));
        when(networkClientDelegate.addAll(any(PollResult.class)))
                .thenAnswer(invocation -> ((PollResult) invocation.getArgument(0)).timeUntilNextPollMs);

        // Don't let the loop spin while the thread is running
        doAnswer(invocation -> {
            Thread.sleep(1);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        consumerNetworkThread = newThread(time);
    }

    @AfterEach
    public void tearDown() {
        if (consumerNetworkThread != null)
            consumerNetworkThread.close(Duration.ofSeconds(5));
    }

    private ConsumerNetworkThread newThread(Time time) {
        Supplier<ApplicationEventProcessor> processorSupplier = () -> {
            processorSupplierCalls.incrementAndGet();
            supplierThread.set(Thread.currentThread());
            return applicationEventProcessor;
        };
        Supplier<NetworkClientDelegate> delegateSupplier = () -> {
            delegateSupplierCalls.incrementAndGet();
            return networkClientDelegate;
        };
        Supplier<RequestManagers> managersSupplier = () -> {
            managersSupplierCalls.incrementAndGet();
            return requestManagers;
        };
        return new ConsumerNetworkThread(logContext, time, processorSupplier, delegateSupplier, managersSupplier);
    }

    @Test
    public void testResourcesAreNotCreatedUntilThreadRuns() throws Exception {
        assertEquals(0, processorSupplierCalls.get());
        assertEquals(0, delegateSupplierCalls.get());
        assertEquals(0, managersSupplierCalls.get());
        assertFalse(consumerNetworkThread.isAlive());
    }

    @Test
    public void testResourcesAreCreatedOnTheBackgroundThread() throws Exception {
        consumerNetworkThread.start();

        TestUtils.waitForCondition(() -> managersSupplierCalls.get() == 1 && delegateSupplierCalls.get() == 1,
                "The consumer network thread did not initialize its resources");

        assertNotNull(supplierThread.get());
        assertNotSame(Thread.currentThread(), supplierThread.get());
        assertEquals(consumerNetworkThread, supplierThread.get());
        assertEquals(1, processorSupplierCalls.get());
    }

    @Test
    public void testStartAndClose() throws Exception {
        consumerNetworkThread.start();
        TestUtils.waitForCondition(consumerNetworkThread::isRunning, "The consumer network thread did not start");
        verify(networkClientDelegate, org.mockito.Mockito.timeout(5000).atLeastOnce()).poll(anyLong(), anyLong());

        consumerNetworkThread.close(Duration.ofSeconds(5));

        assertFalse(consumerNetworkThread.isRunning());
        assertFalse(consumerNetworkThread.isAlive());
        verify(requestManagers).close();
        verify(networkClientDelegate).close();
        verify(applicationEventProcessor).close();
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        consumerNetworkThread.start();
        verify(networkClientDelegate, org.mockito.Mockito.timeout(5000).atLeastOnce()).poll(anyLong(), anyLong());

        consumerNetworkThread.close(Duration.ofSeconds(5));
        consumerNetworkThread.close(Duration.ofSeconds(5));
        consumerNetworkThread.close();

        verify(requestManagers, times(1)).close();
        verify(networkClientDelegate, times(1)).close();
        verify(applicationEventProcessor, times(1)).close();
    }

    @Test
    public void testCloseGivesRequestManagersTheChanceToSendFinalRequests() throws Exception {
        PollResult closeResult = new PollResult(newUnsentRequest());
        when(commitRequestManager.pollOnClose()).thenReturn(closeResult);
        when(coordinatorRequestManager.pollOnClose()).thenReturn(PollResult.EMPTY);
        doAnswer(invocation -> {
            Thread.sleep(1);
            closeResult.unsentRequests.forEach(r -> r.future().complete(mock(ClientResponse.class)));
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        consumerNetworkThread.start();
        // Wait for the thread to have created its resources and be in its loop
        verify(networkClientDelegate, org.mockito.Mockito.timeout(5000).atLeastOnce()).poll(anyLong(), anyLong());
        consumerNetworkThread.close(Duration.ofSeconds(5));

        verify(networkClientDelegate).addAll(closeResult);
        assertTrue(closeResult.unsentRequests.get(0).future().isDone());
    }

    @Test
    public void testRunOnceProcessesEventsAndPollsRequestManagers() throws Exception {
        consumerNetworkThread.initializeResources();
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(new PollResult(100));
        when(commitRequestManager.poll(anyLong())).thenReturn(new PollResult(50));
        long now = time.milliseconds();

        consumerNetworkThread.runOnce();

        verify(applicationEventProcessor).process();
        verify(coordinatorRequestManager).poll(now);
        verify(commitRequestManager).poll(now);
        verify(networkClientDelegate, times(2)).addAll(any(PollResult.class));
        // The network client is polled for as long as the manager that needs attention first
        verify(networkClientDelegate).poll(50, now);
    }

    @Test
    public void testRunOnceHandsPollResultsToTheNetworkClient() throws Exception {
        consumerNetworkThread.initializeResources();
        PollResult result = new PollResult(newUnsentRequest());
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(result);

        consumerNetworkThread.runOnce();

        verify(networkClientDelegate).addAll(result);
        verify(networkClientDelegate).addAll(PollResult.EMPTY);
    }

    @Test
    public void testRunOnceDoesNotWaitLongerThanMaximumPollTimeout() throws Exception {
        consumerNetworkThread.initializeResources();

        consumerNetworkThread.runOnce();

        // No manager has anything scheduled, so the maximum timeout applies
        verify(networkClientDelegate).poll(5000, time.milliseconds());
    }

    @Test
    public void testRunOnceProcessesEventsBeforePollingTheManagers() throws Exception {
        consumerNetworkThread.initializeResources();
        org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(applicationEventProcessor, coordinatorRequestManager, networkClientDelegate);

        consumerNetworkThread.runOnce();

        inOrder.verify(applicationEventProcessor).process();
        inOrder.verify(coordinatorRequestManager).poll(anyLong());
        inOrder.verify(networkClientDelegate).poll(anyLong(), anyLong());
    }

    @Test
    public void testWakeupBeforeInitializationIsNoOp() throws Exception {
        consumerNetworkThread.wakeup();

        verify(networkClientDelegate, never()).wakeup();
    }

    @Test
    public void testWakeupWakesUpTheNetworkClient() throws Exception {
        consumerNetworkThread.initializeResources();

        consumerNetworkThread.wakeup();

        verify(networkClientDelegate).wakeup();
    }

    @Test
    public void testCloseWakesUpTheNetworkClient() throws Exception {
        consumerNetworkThread.start();
        verify(networkClientDelegate, org.mockito.Mockito.timeout(5000).atLeastOnce()).poll(anyLong(), anyLong());

        consumerNetworkThread.close(Duration.ofSeconds(5));

        verify(networkClientDelegate, org.mockito.Mockito.atLeastOnce()).wakeup();
    }

    @Test
    public void testCloseWithNegativeTimeoutFails() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> consumerNetworkThread.close(Duration.ofMillis(-1)));
    }

    @Test
    public void testThreadStopsWhenAnUnexpectedErrorOccurs() throws Exception {
        doThrow(new IllegalStateException("Unexpected")).when(applicationEventProcessor).process();

        consumerNetworkThread.start();
        consumerNetworkThread.join(5000);

        assertFalse(consumerNetworkThread.isAlive());
        assertFalse(consumerNetworkThread.isRunning());
        // The resources are still released
        verify(requestManagers).close();
        verify(networkClientDelegate).close();
    }

    @Test
    public void testRunAtCloseFlushesFinalRequests() throws Exception {
        UnsentRequest request = newUnsentRequest();
        PollResult closeResult = new PollResult(request);
        RequestManager manager = mock(RequestManager.class);
        when(manager.pollOnClose()).thenReturn(closeResult);
        doAnswer(invocation -> {
            request.future().complete(mock(ClientResponse.class));
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());
        Timer timer = time.timer(1000);

        ConsumerNetworkThread.runAtClose(managers(Optional.of(manager)), networkClientDelegate, timer);

        verify(networkClientDelegate).addAll(closeResult);
        verify(networkClientDelegate, times(1)).poll(anyLong(), anyLong());
        assertTrue(timer.notExpired());
    }

    @Test
    public void testRunAtClosePollsEveryManagerThatIsPresent() throws Exception {
        RequestManager first = mock(RequestManager.class);
        RequestManager second = mock(RequestManager.class);
        when(first.pollOnClose()).thenReturn(PollResult.EMPTY);
        when(second.pollOnClose()).thenReturn(PollResult.EMPTY);

        ConsumerNetworkThread.runAtClose(managers(Optional.of(first), Optional.empty(), Optional.of(second)),
                networkClientDelegate, time.timer(1000));

        verify(first).pollOnClose();
        verify(second).pollOnClose();
        verify(networkClientDelegate, times(2)).addAll(PollResult.EMPTY);
        // There is nothing to wait for
        verify(networkClientDelegate, never()).poll(anyLong(), anyLong());
    }

    @Test
    public void testRunAtCloseStopsWhenTheTimerExpires() throws Exception {
        // The request never completes, so only the timer ends the polling
        UnsentRequest request = newUnsentRequest();
        RequestManager manager = mock(RequestManager.class);
        when(manager.pollOnClose()).thenReturn(new PollResult(request));
        doAnswer(invocation -> {
            time.sleep(100);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());
        Timer timer = time.timer(500);

        ConsumerNetworkThread.runAtClose(managers(Optional.of(manager)), networkClientDelegate, timer);

        assertTrue(timer.isExpired());
        assertFalse(request.future().isDone());
        verify(networkClientDelegate, times(5)).poll(anyLong(), anyLong());
    }

    @Test
    public void testCleanupClosesResourcesEvenWhenFlushingFails() throws Exception {
        consumerNetworkThread.initializeResources();
        RequestManager failing = mock(RequestManager.class);
        when(failing.pollOnClose()).thenThrow(new IllegalStateException("Failure on close"));
        when(requestManagers.entries()).thenReturn(Collections.singletonList(Optional.of(failing)));

        consumerNetworkThread.cleanup();

        verify(requestManagers).close();
        verify(networkClientDelegate).close();
        verify(applicationEventProcessor).close();
    }

    private UnsentRequest newUnsentRequest() {
        return new UnsentRequest(mock(AbstractRequest.Builder.class), (Node) null);
    }

    @SafeVarargs
    private static Collection<Optional<? extends RequestManager>> managers(Optional<? extends RequestManager>... managers) {
        List<Optional<? extends RequestManager>> list = new ArrayList<>();

        for (Optional<? extends RequestManager> manager : managers)
            list.add(manager);

        return list;
    }
}
