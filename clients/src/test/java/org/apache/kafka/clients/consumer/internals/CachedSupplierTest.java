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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CachedSupplierTest {

    @Test
    public void testCreateIsNotInvokedUntilGet() {
        AtomicInteger created = new AtomicInteger();
        Supplier<Object> supplier = countingSupplier(created);

        assertEquals(0, created.get());
        assertNotNull(supplier.get());
        assertEquals(1, created.get());
    }

    @Test
    public void testCreateIsInvokedOnlyOnce() {
        AtomicInteger created = new AtomicInteger();
        Supplier<Object> supplier = countingSupplier(created);

        Object first = supplier.get();
        Object second = supplier.get();
        Object third = supplier.get();

        assertSame(first, second);
        assertSame(first, third);
        assertEquals(1, created.get());
    }

    @Test
    public void testNetworkClientDelegateSupplierIsLazyAndCached() throws Exception {
        MockTime time = new MockTime();
        LogContext logContext = new LogContext();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        ConsumerConfig config = new ConsumerConfig(props);
        SubscriptionState subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners());

        try (Metrics metrics = ConsumerUtils.createMetrics(config, time)) {
            FetchMetricsManager fetchMetricsManager = ConsumerUtils.createFetchMetricsManager(metrics);
            int metricsBefore = metrics.metrics().size();

            Supplier<NetworkClientDelegate> supplier = NetworkClientDelegate.supplier(time,
                    logContext,
                    metadata,
                    config,
                    new ApiVersions(),
                    metrics,
                    fetchMetricsManager,
                    fetchMetricsManager.throttleTimeSensor());

            // Nothing is built, no network client registered its metrics, until the supplier is used
            assertEquals(metricsBefore, metrics.metrics().size());

            NetworkClientDelegate delegate = supplier.get();
            try {
                assertTrue(metrics.metrics().size() > metricsBefore);
                int metricsAfter = metrics.metrics().size();
                assertSame(delegate, supplier.get());
                assertEquals(metricsAfter, metrics.metrics().size());
            } finally {
                delegate.close();
            }
        }
    }

    private static Supplier<Object> countingSupplier(AtomicInteger created) {
        return new CachedSupplier<Object>() {
            @Override
            protected Object create() {
                created.incrementAndGet();
                return new Object();
            }
        };
    }
}
