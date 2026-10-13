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
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApplicationEventHandlerTest {

    private final LogContext logContext = new LogContext();
    private final TopicPartition tp = new TopicPartition("topic", 0);
    private BlockingQueue<ApplicationEvent> queue;
    private ApplicationEventProcessor processor;
    private NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private AtomicInteger suppliersCalled;
    private ApplicationEventHandler handler;

    @BeforeEach
    public void setup() {
        queue = new LinkedBlockingQueue<>();
        processor = mock(ApplicationEventProcessor.class);
        networkClientDelegate = mock(NetworkClientDelegate.class);
        requestManagers = mock(RequestManagers.class);
        suppliersCalled = new AtomicInteger();
        when(requestManagers.entries()).thenReturn(Collections.emptyList());
        when(networkClientDelegate.addAll(org.mockito.ArgumentMatchers.any(NetworkClientDelegate.PollResult.class))).thenReturn(Long.MAX_VALUE);

        // Don't let the network thread spin while the test is running
        doAnswer(invocation -> {
            Thread.sleep(1);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        handler = new ApplicationEventHandler(logContext,
                Time.SYSTEM,
                queue,
                () -> {
                    suppliersCalled.incrementAndGet();
                    return processor;
                },
                () -> {
                    suppliersCalled.incrementAndGet();
                    return networkClientDelegate;
                },
                () -> {
                    suppliersCalled.incrementAndGet();
                    return requestManagers;
                });
    }

    @AfterEach
    public void tearDown() {
        handler.close(Duration.ofSeconds(5));
    }

    @Test
    public void testNetworkThreadIsStartedAndCreatesItsResources() throws Exception {
        // Each supplier is invoked once, by the network thread that the handler started
        verify(networkClientDelegate, timeout(5000).atLeastOnce()).poll(anyLong(), anyLong());
        assertEquals(3, suppliersCalled.get());
    }

    @Test
    public void testAddEnqueuesTheEventAndWakesUpTheNetworkThread() throws Exception {
        verify(networkClientDelegate, timeout(5000).atLeastOnce()).poll(anyLong(), anyLong());
        ApplicationEvent event = new PollApplicationEvent(1);

        handler.add(event);

        // The processor is a mock so it doesn't drain the queue
        assertTrue(queue.contains(event));
        verify(networkClientDelegate, timeout(5000).atLeastOnce()).wakeup();
    }

    @Test
    public void testWakeupNetworkThread() throws Exception {
        verify(networkClientDelegate, timeout(5000).atLeastOnce()).poll(anyLong(), anyLong());

        handler.wakeupNetworkThread();

        verify(networkClientDelegate, timeout(5000).atLeastOnce()).wakeup();
    }

    @Test
    public void testAddRejectsNullEvent() throws Exception {
        assertThrows(NullPointerException.class, () -> handler.add(null));
    }

    @Test
    public void testAddAndGetReturnsTheResultOfTheEvent() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = Collections.singletonMap(tp, new OffsetAndMetadata(10));
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(Collections.singleton(tp));
        event.future().complete(committed);

        Map<TopicPartition, OffsetAndMetadata> result = handler.addAndGet(event, Time.SYSTEM.timer(5000));

        assertSame(committed, result);
        assertTrue(queue.contains(event));
    }

    @Test
    public void testAddAndGetTimesOutWhenTheEventIsNotCompleted() throws Exception {
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(Collections.singleton(tp));

        assertThrows(TimeoutException.class, () -> handler.addAndGet(event, Time.SYSTEM.timer(50)));
    }

    @Test
    public void testAddAndGetRejectsNullArguments() throws Exception {
        assertThrows(NullPointerException.class, () -> handler.addAndGet(null, Time.SYSTEM.timer(50)));
        assertThrows(NullPointerException.class,
                () -> handler.addAndGet(new OffsetFetchApplicationEvent(Collections.singleton(tp)), null));
    }

    @Test
    public void testCloseStopsTheNetworkThreadAndIsIdempotent() throws Exception {
        verify(networkClientDelegate, timeout(5000).atLeastOnce()).poll(anyLong(), anyLong());

        handler.close(Duration.ofSeconds(5));
        handler.close(Duration.ofSeconds(5));
        handler.close();

        verify(networkClientDelegate, times(1)).close();
        verify(requestManagers, times(1)).close();
        verify(processor, times(1)).close();
        verify(networkClientDelegate, atLeastOnce()).wakeup();
    }

    @Test
    public void testCloseRejectsNegativeTimeout() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> handler.close(Duration.ofMillis(-1)));
    }
}
