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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.RequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApplicationEventHandlerTest {

    private static final long WAIT_MS = 10_000;

    private final LogContext logContext = new LogContext();
    private final TopicPartition tp = new TopicPartition("topic", 0);
    private final List<ApplicationEvent> processedEvents = new CopyOnWriteArrayList<>();
    private BlockingQueue<ApplicationEvent> queue;
    private ApplicationEventProcessor processor;
    private NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private ApplicationEventHandler handler;

    @BeforeEach
    public void setup() {
        queue = new LinkedBlockingQueue<>();
        processor = mock(ApplicationEventProcessor.class);
        networkClientDelegate = mock(NetworkClientDelegate.class);
        requestManagers = mock(RequestManagers.class);
        RequestManager requestManager = mock(RequestManager.class);

        when(requestManager.poll(anyLong())).thenReturn(PollResult.EMPTY);
        when(requestManager.pollOnClose()).thenReturn(PollResult.EMPTY);
        when(requestManagers.entries()).thenReturn(Collections.singletonList(Optional.of(requestManager)));
        when(networkClientDelegate.addAll(any(PollResult.class))).thenReturn(PollResult.WAIT_FOREVER);

        // The network client mock returns immediately from poll, so give the loop some air.
        doAnswer(invocation -> {
            Thread.sleep(2);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        // The real processor drains the queue; do the same, remembering what was drained. Completable events
        // are completed with a value so that addAndGet can return.
        doAnswer(invocation -> {
            List<ApplicationEvent> drained = new ArrayList<>();
            queue.drainTo(drained);
            for (ApplicationEvent event : drained) {
                processedEvents.add(event);

                if (event instanceof OffsetFetchApplicationEvent)
                    ((OffsetFetchApplicationEvent) event).future().complete(
                            Collections.singletonMap(tp, new OffsetAndMetadata(42)));
            }
            return null;
        }).when(processor).process();

        handler = new ApplicationEventHandler(logContext,
                Time.SYSTEM,
                queue,
                () -> processor,
                () -> networkClientDelegate,
                () -> requestManagers);
    }

    @AfterEach
    public void tearDown() {
        handler.close(Duration.ofSeconds(1));
    }

    @Test
    public void testAddEnqueuesTheEventForTheNetworkThread() throws Exception {
        PollApplicationEvent event = new PollApplicationEvent(1L);

        handler.add(event);

        waitForCondition(() -> processedEvents.contains(event), WAIT_MS, "The event was not processed by the network thread");
    }

    @Test
    public void testAddWakesUpTheNetworkThread() {
        // Wait for the network thread to be up (its delegate resolved) before adding the event.
        verify(networkClientDelegate, timeout(WAIT_MS).atLeastOnce()).poll(anyLong(), anyLong());

        handler.add(new PollApplicationEvent(1L));

        verify(networkClientDelegate, timeout(WAIT_MS).atLeastOnce()).wakeup();
    }

    @Test
    public void testWakeupNetworkThread() {
        // Wait for the network thread to be up (its delegate resolved) before asking for a wakeup.
        verify(networkClientDelegate, timeout(WAIT_MS).atLeastOnce()).poll(anyLong(), anyLong());

        handler.wakeupNetworkThread();

        verify(networkClientDelegate, timeout(WAIT_MS).atLeastOnce()).wakeup();
    }

    @Test
    public void testAddAndGetReturnsTheResultOfTheEvent() {
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(Collections.singleton(tp));

        Map<TopicPartition, OffsetAndMetadata> result = handler.addAndGet(event, Time.SYSTEM.timer(WAIT_MS));

        assertEquals(Collections.singletonMap(tp, new OffsetAndMetadata(42)), result);
    }

    @Test
    public void testAddAndGetTimesOutIfTheEventIsNeverCompleted() {
        // This event type is not completed by the test processor
        ListOffsetsApplicationEvent event = new ListOffsetsApplicationEvent(Collections.singletonMap(tp, 1L), false);

        assertThrows(TimeoutException.class, () -> handler.addAndGet(event, Time.SYSTEM.timer(50)));
        assertFalse(event.future().isDone());
    }

    @Test
    public void testAddAndGetPropagatesFailureOfTheEvent() {
        ListOffsetsApplicationEvent event = new ListOffsetsApplicationEvent(Collections.singletonMap(tp, 1L), false);
        event.future().completeExceptionally(new IllegalStateException("failed in the network thread"));

        assertThrows(KafkaException.class, () -> handler.addAndGet(event, Time.SYSTEM.timer(WAIT_MS)));
    }

    @Test
    public void testNullArgumentsAreRejected() {
        assertThrows(NullPointerException.class, () -> handler.add(null));
        assertThrows(NullPointerException.class, () -> handler.addAndGet(null, Time.SYSTEM.timer(1)));
        assertThrows(NullPointerException.class, () -> handler.addAndGet(new OffsetFetchApplicationEvent(Collections.emptySet()), null));
    }

    @Test
    public void testClose() throws Exception {
        verify(networkClientDelegate, timeout(WAIT_MS).atLeastOnce()).poll(anyLong(), anyLong());

        handler.close(Duration.ofSeconds(5));

        verify(networkClientDelegate, timeout(WAIT_MS)).close();
        verify(requestManagers, timeout(WAIT_MS)).close();
        verify(processor, timeout(WAIT_MS)).close();
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        verify(networkClientDelegate, timeout(WAIT_MS).atLeastOnce()).poll(anyLong(), anyLong());

        handler.close(Duration.ofSeconds(5));
        handler.close(Duration.ofSeconds(5));
        handler.close();

        verify(networkClientDelegate, times(1)).close();
        verify(requestManagers, times(1)).close();
    }

    @Test
    public void testCloseWithNegativeTimeoutFails() {
        assertThrows(IllegalArgumentException.class, () -> handler.close(Duration.ofMillis(-1)));
    }
}
