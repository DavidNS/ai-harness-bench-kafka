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
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class RequestManagersTest {

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final AtomicInteger delegatesRequested = new AtomicInteger();
    private final NetworkClientDelegate networkClientDelegate = mock(NetworkClientDelegate.class);
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
    private FetchBuffer fetchBuffer;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private FetchMetricsManager fetchMetricsManager;

    @BeforeEach
    public void setup() {
        fetchBuffer = new FetchBuffer(logContext);
        subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        metadata = new ConsumerMetadata(0, 1000, 10000, false, false, subscriptions, logContext, new ClusterResourceListeners());
        fetchMetricsManager = ConsumerUtils.createFetchMetricsManager(
                new Metrics(new MetricConfig().tags(Collections.singletonMap("client-id", "test-client")), time));
    }

    @AfterEach
    public void tearDown() {
        fetchBuffer.close();
    }

    @Test
    public void testSupplierIsLazy() {
        Supplier<RequestManagers> supplier = newSupplier(newConfig(true));

        assertEquals(0, delegatesRequested.get());

        assertNotNull(supplier.get());
        assertEquals(1, delegatesRequested.get());
    }

    @Test
    public void testSupplierCreatesTheManagersOnlyOnce() {
        Supplier<RequestManagers> supplier = newSupplier(newConfig(true));

        RequestManagers first = supplier.get();
        RequestManagers second = supplier.get();

        assertSame(first, second);
        assertEquals(1, delegatesRequested.get());
    }

    @Test
    public void testManagersWithGroupId() {
        RequestManagers requestManagers = newSupplier(newConfig(true)).get();

        assertTrue(requestManagers.coordinatorRequestManager.isPresent());
        assertTrue(requestManagers.commitRequestManager.isPresent());
        assertTrue(requestManagers.heartbeatRequestManager.isPresent());
        assertNotNull(requestManagers.offsetsRequestManager);
        assertNotNull(requestManagers.topicMetadataRequestManager);
        assertNotNull(requestManagers.fetchRequestManager);

        // All of them are polled by the network thread
        assertEquals(6, requestManagers.entries().stream().filter(Optional::isPresent).count());
    }

    @Test
    public void testManagersWithoutGroupId() {
        RequestManagers requestManagers = newSupplier(newConfig(false)).get();

        assertFalse(requestManagers.coordinatorRequestManager.isPresent());
        assertFalse(requestManagers.commitRequestManager.isPresent());
        assertFalse(requestManagers.heartbeatRequestManager.isPresent());
        assertNotNull(requestManagers.offsetsRequestManager);
        assertNotNull(requestManagers.topicMetadataRequestManager);
        assertNotNull(requestManagers.fetchRequestManager);

        assertEquals(3, requestManagers.entries().stream().filter(Optional::isPresent).count());
        assertEquals(6, requestManagers.entries().size());
    }

    @Test
    public void testClosingTheManagersClosesTheSharedFetchBuffer() {
        RequestManagers requestManagers = newSupplier(newConfig(false)).get();
        fetchBuffer.add(mock(CompletedFetch.class));
        assertFalse(fetchBuffer.isEmpty());

        // The fetch request manager owns the buffer that it shares with the application thread
        requestManagers.close();

        assertTrue(fetchBuffer.isEmpty());
    }

    @Test
    public void testCloseIsIdempotent() {
        RequestManagers requestManagers = newSupplier(newConfig(true)).get();

        assertDoesNotThrow(() -> {
            requestManagers.close();
            requestManagers.close();
        });
    }

    private Supplier<RequestManagers> newSupplier(ConsumerConfig config) {
        Supplier<NetworkClientDelegate> delegateSupplier = () -> {
            delegatesRequested.incrementAndGet();
            return networkClientDelegate;
        };

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
                delegateSupplier);
    }

    private static ConsumerConfig newConfig(boolean withGroupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        if (withGroupId)
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group");

        return new ConsumerConfig(properties);
    }
}
