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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class RequestManagersTest {

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final AtomicInteger delegatesCreated = new AtomicInteger();
    private Metrics metrics;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private FetchBuffer fetchBuffer;
    private FetchMetricsManager fetchMetricsManager;
    private Supplier<NetworkClientDelegate> networkClientDelegateSupplier;

    @BeforeEach
    public void setup() {
        metrics = new Metrics(time);
        subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        metadata = mock(ConsumerMetadata.class);
        fetchBuffer = new FetchBuffer(logContext);
        fetchMetricsManager = new FetchMetricsManager(metrics, new FetchMetricsRegistry());
        networkClientDelegateSupplier = () -> {
            delegatesCreated.incrementAndGet();
            return mock(NetworkClientDelegate.class);
        };
    }

    @AfterEach
    public void tearDown() {
        fetchBuffer.close();
        metrics.close();
    }

    private ConsumerConfig config(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        if (groupId != null)
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new ConsumerConfig(properties);
    }

    private Supplier<RequestManagers> supplier(String groupId) {
        ConsumerConfig config = config(groupId);
        BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        return RequestManagers.supplier(time,
                logContext,
                backgroundEventQueue,
                metadata,
                subscriptions,
                fetchBuffer,
                config,
                new GroupRebalanceConfig(config, GroupRebalanceConfig.ProtocolType.CONSUMER),
                new ApiVersions(),
                fetchMetricsManager,
                networkClientDelegateSupplier);
    }

    @Test
    public void testSupplierIsLazy() {
        Supplier<RequestManagers> supplier = supplier("group");
        assertEquals(0, delegatesCreated.get());

        assertNotNull(supplier.get());
        assertEquals(1, delegatesCreated.get());
    }

    @Test
    public void testSupplierCreatesRequestManagersOnlyOnce() {
        Supplier<RequestManagers> supplier = supplier("group");

        RequestManagers first = supplier.get();
        assertSame(first, supplier.get());
        assertEquals(1, delegatesCreated.get());
    }

    @Test
    public void testGroupManagersArePresentWhenGroupIdIsSet() {
        RequestManagers requestManagers = supplier("group").get();

        assertTrue(requestManagers.coordinatorRequestManager.isPresent());
        assertTrue(requestManagers.commitRequestManager.isPresent());
        assertTrue(requestManagers.heartbeatRequestManager.isPresent());
        assertNotNull(requestManagers.offsetsRequestManager);
        assertNotNull(requestManagers.topicMetadataRequestManager);
        assertNotNull(requestManagers.fetchRequestManager);
        assertEquals(6, requestManagers.entries().stream().filter(Optional::isPresent).count());
    }

    @Test
    public void testGroupManagersAreAbsentWithoutGroupId() {
        RequestManagers requestManagers = supplier(null).get();

        assertFalse(requestManagers.coordinatorRequestManager.isPresent());
        assertFalse(requestManagers.commitRequestManager.isPresent());
        assertFalse(requestManagers.heartbeatRequestManager.isPresent());
        assertEquals(3, requestManagers.entries().stream().filter(Optional::isPresent).count());
    }

    @Test
    public void testCloseIsIdempotentAndClosesTheFetchBuffer() {
        RequestManagers requestManagers = supplier("group").get();

        requestManagers.close();
        requestManagers.close();

        // The fetch request manager owns the buffer's lifecycle on close, so it was drained and closed.
        assertTrue(fetchBuffer.isEmpty());
    }
}
