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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NetworkClientDelegateTest {
    private static final int REQUEST_TIMEOUT_MS = 5000;
    private static final String GROUP_ID = "group";
    private MockTime time;
    private MockClient client;

    @BeforeEach
    public void setup() {
        this.time = new MockTime(0);
        this.client = new MockClient(time, Collections.singletonList(mockNode()));
    }

    @Test
    public void testSuccessfulResponse() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate()) {
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            prepareFindCoordinatorResponse(Errors.NONE);

            ncd.send(unsentRequest);
            ncd.poll(0, time.milliseconds());

            assertTrue(unsentRequest.future().isDone());
            assertNotNull(unsentRequest.future().get());
        }
    }

    @Test
    public void testTimeoutBeforeSend() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate()) {
            client.setUnreachable(mockNode(), REQUEST_TIMEOUT_MS);
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            ncd.send(unsentRequest);
            ncd.poll(0, time.milliseconds());
            time.sleep(REQUEST_TIMEOUT_MS);
            ncd.poll(0, time.milliseconds());
            assertTrue(unsentRequest.future().isDone());
            TestUtils.assertFutureThrows(unsentRequest.future(), TimeoutException.class);
        }
    }

    @Test
    public void testTimeoutAfterSend() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate()) {
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            ncd.send(unsentRequest);
            ncd.poll(0, time.milliseconds());
            time.sleep(REQUEST_TIMEOUT_MS);
            ncd.poll(0, time.milliseconds());
            assertTrue(unsentRequest.future().isDone());
            TestUtils.assertFutureThrows(unsentRequest.future(), DisconnectException.class);
        }
    }

    @Test
    public void testEnsureCorrectCompletionTimeOnFailure() {
        NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
        long timeMs = time.milliseconds();
        unsentRequest.handler().onFailure(timeMs, new TimeoutException());

        time.sleep(100);
        assertEquals(timeMs, unsentRequest.handler().completionTimeMs());
    }

    @Test
    public void testEnsureCorrectCompletionTimeOnComplete() {
        NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
        long timeMs = time.milliseconds();
        final ClientResponse response = mock(ClientResponse.class);
        when(response.receivedTimeMs()).thenReturn(timeMs);
        unsentRequest.handler().onComplete(response);
        time.sleep(100);
        assertEquals(timeMs, unsentRequest.handler().completionTimeMs());
    }

    @Test
    public void testAddAllPollResultEnqueuesTheRequestsAndReturnsTheNextPollTime() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate()) {
            NetworkClientDelegate.UnsentRequest first = newUnsentFindCoordinatorRequest();
            NetworkClientDelegate.UnsentRequest second = newUnsentFindCoordinatorRequest();

            long nextPollMs = ncd.addAll(new NetworkClientDelegate.PollResult(250, Arrays.asList(first, second)));

            assertEquals(250, nextPollMs);
            assertEquals(2, ncd.unsentRequests().size());
            assertTrue(ncd.unsentRequests().contains(first));
            assertTrue(ncd.unsentRequests().contains(second));
        }
    }

    @Test
    public void testAddAllEmptyPollResult() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate()) {
            long nextPollMs = ncd.addAll(NetworkClientDelegate.PollResult.EMPTY);

            assertEquals(NetworkClientDelegate.PollResult.WAIT_FOREVER, nextPollMs);
            assertTrue(ncd.unsentRequests().isEmpty());
        }
    }

    @Test
    public void testPollResultConstructors() {
        NetworkClientDelegate.UnsentRequest request = newUnsentFindCoordinatorRequest();

        NetworkClientDelegate.PollResult withTimeAndRequests = new NetworkClientDelegate.PollResult(5, Collections.singletonList(request));
        assertEquals(5, withTimeAndRequests.timeUntilNextPollMs);
        assertEquals(Collections.singletonList(request), withTimeAndRequests.unsentRequests);

        NetworkClientDelegate.PollResult withRequests = new NetworkClientDelegate.PollResult(Collections.singletonList(request));
        assertEquals(NetworkClientDelegate.PollResult.WAIT_FOREVER, withRequests.timeUntilNextPollMs);
        assertEquals(Collections.singletonList(request), withRequests.unsentRequests);

        NetworkClientDelegate.PollResult withRequest = new NetworkClientDelegate.PollResult(request);
        assertEquals(NetworkClientDelegate.PollResult.WAIT_FOREVER, withRequest.timeUntilNextPollMs);
        assertEquals(Collections.singletonList(request), withRequest.unsentRequests);

        NetworkClientDelegate.PollResult withTime = new NetworkClientDelegate.PollResult(7);
        assertEquals(7, withTime.timeUntilNextPollMs);
        assertTrue(withTime.unsentRequests.isEmpty());

        assertEquals(Long.MAX_VALUE, NetworkClientDelegate.PollResult.EMPTY.timeUntilNextPollMs);
        assertTrue(NetworkClientDelegate.PollResult.EMPTY.unsentRequests.isEmpty());
    }

    @Test
    public void testUnsentRequestNode() {
        Node node = mockNode();
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKey(GROUP_ID));

        assertEquals(Optional.of(node), new NetworkClientDelegate.UnsentRequest(builder, Optional.of(node)).node());
        assertEquals(Optional.empty(), new NetworkClientDelegate.UnsentRequest(builder, Optional.empty()).node());
        assertEquals(Optional.of(node), new NetworkClientDelegate.UnsentRequest(builder, node, (response, error) -> { }).node());
        assertEquals(Optional.empty(), new NetworkClientDelegate.UnsentRequest(builder, Optional.<Node>empty(), (response, error) -> { }).node());
    }

    @Test
    public void testUnsentRequestCallbackReceivesTheOutcome() {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<ClientResponse> response = new AtomicReference<>();
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKey(GROUP_ID));
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(builder, mockNode(), (r, t) -> {
            response.set(r);
            error.set(t);
        });

        request.handler().onFailure(time.milliseconds(), new DisconnectException());

        assertNull(response.get());
        assertTrue(error.get() instanceof DisconnectException);
    }

    @Test
    public void testIsUnavailableAndAuthFailureAreDelegatedToTheClient() throws Exception {
        KafkaClient kafkaClient = mock(KafkaClient.class);
        Node node = mockNode();
        AuthenticationException authenticationException = new AuthenticationException("denied");

        try (NetworkClientDelegate ncd = newNetworkClientDelegate(kafkaClient)) {
            // Healthy node
            assertFalse(ncd.isUnavailable(node));
            ncd.maybeThrowAuthFailure(node);

            // Failed connection that is still in the reconnect backoff window
            when(kafkaClient.connectionFailed(node)).thenReturn(true);
            when(kafkaClient.connectionDelay(node, time.milliseconds())).thenReturn(100L);
            when(kafkaClient.authenticationException(node)).thenReturn(authenticationException);

            assertTrue(ncd.isUnavailable(node));
            assertSame(authenticationException, assertThrows(AuthenticationException.class, () -> ncd.maybeThrowAuthFailure(node)));

            // Failed connection that can be retried right away
            when(kafkaClient.connectionDelay(node, time.milliseconds())).thenReturn(0L);
            assertFalse(ncd.isUnavailable(node));
        }
    }

    @Test
    public void testDoSend() throws Exception {
        KafkaClient kafkaClient = mock(KafkaClient.class);
        Node node = mockNode();
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKey(GROUP_ID)),
                node,
                (response, error) -> { });
        request.setTimer(time, REQUEST_TIMEOUT_MS);

        try (NetworkClientDelegate ncd = newNetworkClientDelegate(kafkaClient)) {
            // The node is not ready to take the request
            assertFalse(ncd.doSend(request, time.milliseconds()));
            verify(kafkaClient, never()).send(any(), anyLong());

            when(kafkaClient.ready(node, time.milliseconds())).thenReturn(true);
            assertTrue(ncd.doSend(request, time.milliseconds()));
            verify(kafkaClient).send(any(), anyLong());
        }
    }

    @Test
    public void testDoSendWithoutAvailableNode() throws Exception {
        KafkaClient kafkaClient = mock(KafkaClient.class);
        NetworkClientDelegate.UnsentRequest request = newUnsentFindCoordinatorRequest();

        try (NetworkClientDelegate ncd = newNetworkClientDelegate(kafkaClient)) {
            // No target node was given and none is available
            when(kafkaClient.leastLoadedNode(anyLong())).thenReturn(null);

            assertFalse(ncd.doSend(request, time.milliseconds()));
            verify(kafkaClient, never()).send(any(), anyLong());
        }
    }

    @Test
    public void testRequestsToFailedNodesAreFailedByCheckDisconnects() throws Exception {
        KafkaClient kafkaClient = mock(KafkaClient.class);
        Node node = mockNode();
        AuthenticationException authenticationException = new AuthenticationException("denied");
        when(kafkaClient.connectionFailed(node)).thenReturn(true);
        when(kafkaClient.authenticationException(node)).thenReturn(authenticationException);
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKey(GROUP_ID)),
                node,
                (response, error) -> { });

        try (NetworkClientDelegate ncd = newNetworkClientDelegate(kafkaClient)) {
            ncd.send(request);
            ncd.poll(0, time.milliseconds());

            assertTrue(request.future().isCompletedExceptionally());
            assertTrue(ncd.unsentRequests().isEmpty());
        }
    }

    @Test
    public void testCheckDisconnectsCanBeOverridden() throws Exception {
        AtomicInteger checks = new AtomicInteger();
        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (NetworkClientDelegate ncd = new NetworkClientDelegate(time, new ConsumerConfig(properties), new LogContext(), client) {
            @Override
            protected void checkDisconnects(long currentTimeMs) {
                checks.incrementAndGet();
            }
        }) {
            ncd.poll(0, time.milliseconds());
            ncd.poll(0, time.milliseconds());
        }

        assertEquals(2, checks.get());
    }

    @Test
    public void testSupplierCreatesTheDelegateLazilyAndOnlyOnce() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ConsumerConfig config = new ConsumerConfig(properties);
        LogContext logContext = new LogContext();
        SubscriptionState subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = new ConsumerMetadata(0, 1000, 10000, false, false, subscriptions, logContext, new ClusterResourceListeners());
        Metrics metrics = new Metrics(new MetricConfig().tags(Collections.singletonMap("client-id", "test-client")), time);
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

        // Creating the network client registers its metrics, so nothing may have been created yet.
        assertEquals(metricsBefore, metrics.metrics().size());

        NetworkClientDelegate delegate = supplier.get();
        int metricsAfterFirstGet = metrics.metrics().size();

        try {
            assertNotNull(delegate);
            assertTrue(metricsAfterFirstGet > metricsBefore);
            assertSame(delegate, supplier.get());
            assertEquals(metricsAfterFirstGet, metrics.metrics().size());
        } finally {
            Utils.closeQuietly(delegate, "network client delegate");
        }
    }

    public NetworkClientDelegate newNetworkClientDelegate() {
        return newNetworkClientDelegate(this.client);
    }

    public NetworkClientDelegate newNetworkClientDelegate(KafkaClient kafkaClient) {
        LogContext logContext = new LogContext();
        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(GROUP_ID_CONFIG, GROUP_ID);
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        return new NetworkClientDelegate(this.time, new ConsumerConfig(properties), logContext, kafkaClient);
    }

    public NetworkClientDelegate.UnsentRequest newUnsentFindCoordinatorRequest() {
        Objects.requireNonNull(GROUP_ID);
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                    .setKey(GROUP_ID)
                    .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                ),
            Optional.empty()
        );
        req.setTimer(this.time, REQUEST_TIMEOUT_MS);
        return req;
    }

    public void prepareFindCoordinatorResponse(Errors error) {
        FindCoordinatorResponse findCoordinatorResponse =
            FindCoordinatorResponse.prepareResponse(error, GROUP_ID, mockNode());
        client.prepareResponse(findCoordinatorResponse);
    }

    private Node mockNode() {
        return new Node(0, "localhost", 99);
    }
}
