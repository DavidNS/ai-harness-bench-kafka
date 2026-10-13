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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RequestManagersTest {

    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
    private Metrics metrics;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private FetchBuffer fetchBuffer;
    private NetworkClientDelegate networkClientDelegate;
    private AtomicInteger delegateSupplied;
    private Supplier<NetworkClientDelegate> networkClientDelegateSupplier;

    @BeforeEach
    public void setup() {
        metrics = ConsumerUtils.createMetrics(config("group"), time);
        subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        metadata = mock(ConsumerMetadata.class);
        fetchBuffer = new FetchBuffer(logContext);
        networkClientDelegate = mock(NetworkClientDelegate.class);
        delegateSupplied = new AtomicInteger();
        networkClientDelegateSupplier = () -> {
            delegateSupplied.incrementAndGet();
            return networkClientDelegate;
        };
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void testSupplierDoesNotCreateAnythingUntilRequested() {
        newSupplier(config("group"));

        assertEquals(0, delegateSupplied.get());
    }

    @Test
    public void testSupplierCreatesTheRequestManagersOnlyOnce() {
        Supplier<RequestManagers> supplier = newSupplier(config("group"));

        RequestManagers first = supplier.get();
        RequestManagers second = supplier.get();

        assertSame(first, second);
        assertEquals(1, delegateSupplied.get());
    }

    @Test
    public void testSupplierCreatesTheGroupManagersWhenThereIsAGroupId() {
        RequestManagers requestManagers = newSupplier(config("group")).get();

        assertTrue(requestManagers.coordinatorRequestManager.isPresent());
        assertTrue(requestManagers.commitRequestManager.isPresent());
        assertTrue(requestManagers.heartbeatRequestManager.isPresent());
        // 3 group managers + offsets + topic metadata + fetch
        assertEquals(6, requestManagers.entries().stream().filter(Optional::isPresent).count());
    }

    @Test
    public void testSupplierDoesNotCreateTheGroupManagersWithoutGroupId() {
        RequestManagers requestManagers = newSupplier(config(null)).get();

        assertFalse(requestManagers.coordinatorRequestManager.isPresent());
        assertFalse(requestManagers.commitRequestManager.isPresent());
        assertFalse(requestManagers.heartbeatRequestManager.isPresent());
        assertEquals(3, requestManagers.entries().stream().filter(Optional::isPresent).count());
    }

    @Test
    public void testEntriesContainEveryRequestManagerIncludingFetch() {
        RequestManagers requestManagers = newSupplier(config("group")).get();

        assertTrue(requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList())
                .contains(requestManagers.fetchRequestManager));
        assertTrue(requestManagers.entries().contains(Optional.of(requestManagers.offsetsRequestManager)));
        assertTrue(requestManagers.entries().contains(Optional.of(requestManagers.topicMetadataRequestManager)));
    }

    @Test
    public void testCloseClosesTheCloseableManagersAndIsIdempotent() {
        FetchRequestManager fetchRequestManager = mock(FetchRequestManager.class);
        RequestManagers requestManagers = new RequestManagers(logContext,
                mock(OffsetsRequestManager.class),
                mock(TopicMetadataRequestManager.class),
                fetchRequestManager,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        requestManagers.close();
        requestManagers.close();

        verify(fetchRequestManager, times(1)).close();
    }

    private Supplier<RequestManagers> newSupplier(ConsumerConfig config) {
        return RequestManagers.supplier(time,
                logContext,
                backgroundEventQueue,
                metadata,
                subscriptions,
                fetchBuffer,
                config,
                new GroupRebalanceConfig(config, GroupRebalanceConfig.ProtocolType.CONSUMER),
                new ApiVersions(),
                ConsumerUtils.createFetchMetricsManager(metrics),
                networkClientDelegateSupplier);
    }

    private ConsumerConfig config(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        if (groupId != null)
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new ConsumerConfig(props);
    }
}
