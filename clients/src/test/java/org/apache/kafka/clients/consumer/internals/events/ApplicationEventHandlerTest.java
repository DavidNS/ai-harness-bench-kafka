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

import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApplicationEventHandlerTest {

    private static final long WAIT_MS = 10000;

    private final LogContext logContext = new LogContext();
    private final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger processorCreated = new AtomicInteger();
    private ApplicationEventProcessor processor;
    private NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private ApplicationEventHandler handler;

    @BeforeEach
    public void setup() {
        processor = mock(ApplicationEventProcessor.class);
        networkClientDelegate = mock(NetworkClientDelegate.class);
        when(networkClientDelegate.addAll(any(PollResult.class))).thenReturn(Long.MAX_VALUE);

        OffsetsRequestManager offsets = mock(OffsetsRequestManager.class);
        TopicMetadataRequestManager topicMetadata = mock(TopicMetadataRequestManager.class);
        FetchRequestManager fetch = mock(FetchRequestManager.class);
        when(offsets.poll(anyLong())).thenReturn(PollResult.EMPTY);
        when(topicMetadata.poll(anyLong())).thenReturn(PollResult.EMPTY);
        when(fetch.poll(anyLong())).thenReturn(PollResult.EMPTY);
        when(offsets.pollOnClose()).thenReturn(PollResult.EMPTY);
        when(topicMetadata.pollOnClose()).thenReturn(PollResult.EMPTY);
        when(fetch.pollOnClose()).thenReturn(PollResult.EMPTY);
        requestManagers = new RequestManagers(logContext,
                offsets,
                topicMetadata,
                fetch,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        handler = new ApplicationEventHandler(logContext,
                Time.SYSTEM,
                applicationEventQueue,
                () -> {
                    processorCreated.incrementAndGet();
                    return processor;
                },
                () -> networkClientDelegate,
                () -> requestManagers);
    }

    @AfterEach
    public void tearDown() {
        handler.close(Duration.ofMillis(WAIT_MS));
    }

    @Test
    public void testNetworkThreadIsStartedAndUsesProcessor() {
        // The network thread resolves the supplier on its own thread, and it then keeps invoking the processor.
        verify(processor, timeout(WAIT_MS).atLeastOnce()).process();
        assertEquals(1, processorCreated.get());
    }

    @Test
    public void testAddEnqueuesEventAndWakesUpNetworkThread() {
        // Wait for the network thread to initialize its resources, before the wakeup can have an effect.
        verify(processor, timeout(WAIT_MS).atLeastOnce()).process();

        ApplicationEvent event = new PollApplicationEvent(1L);

        handler.add(event);

        // The (mock) processor does not drain the queue, so the event stays in there.
        assertTrue(applicationEventQueue.contains(event));
        verify(networkClientDelegate, timeout(WAIT_MS).atLeastOnce()).wakeup();
    }

    @Test
    public void testAddRejectsNullEvent() {
        assertThrows(NullPointerException.class, () -> handler.add(null));
    }

    @Test
    public void testWakeupNetworkThread() {
        verify(processor, timeout(WAIT_MS).atLeastOnce()).process();
        handler.wakeupNetworkThread();
        verify(networkClientDelegate, timeout(WAIT_MS).atLeastOnce()).wakeup();
    }

    @Test
    public void testAddAndGetReturnsTheResultOfTheEvent() throws Exception {
        ListOffsetsApplicationEvent event = new ListOffsetsApplicationEvent(Collections.emptyMap(), false);
        Thread completer = new Thread(() -> {
            try {
                TestUtils.waitForCondition(() -> applicationEventQueue.contains(event), WAIT_MS, "Event was not added");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            event.future().complete(Collections.emptyMap());
        });
        completer.start();

        assertEquals(Collections.emptyMap(), handler.addAndGet(event, Time.SYSTEM.timer(WAIT_MS)));
        completer.join(WAIT_MS);
    }

    @Test
    public void testAddAndGetTimesOutIfTheEventIsNotCompleted() {
        ListOffsetsApplicationEvent event = new ListOffsetsApplicationEvent(Collections.emptyMap(), false);
        assertThrows(TimeoutException.class, () -> handler.addAndGet(event, Time.SYSTEM.timer(50)));
        assertTrue(applicationEventQueue.contains(event));
    }

    @Test
    public void testAddAndGetRejectsNullArguments() {
        ListOffsetsApplicationEvent event = new ListOffsetsApplicationEvent(Collections.emptyMap(), false);
        assertThrows(NullPointerException.class, () -> handler.addAndGet(null, Time.SYSTEM.timer(1)));
        assertThrows(NullPointerException.class, () -> handler.addAndGet(event, null));
    }

    @Test
    public void testCloseIsIdempotentAndClosesTheNetworkThreadResources() throws Exception {
        verify(processor, timeout(WAIT_MS).atLeastOnce()).process();

        handler.close();
        handler.close(Duration.ofMillis(WAIT_MS));
        handler.close();

        verify(networkClientDelegate, atLeastOnce()).close();
        // Closing the handler multiple times closes the resources of the network thread exactly once.
        verify(processor).close();
    }
}
