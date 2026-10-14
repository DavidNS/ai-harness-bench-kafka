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
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
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
    public void testAddAllPollResultQueuesRequestsAndReturnsTimeUntilNextPoll() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate()) {
            NetworkClientDelegate.UnsentRequest first = newUnsentFindCoordinatorRequest();
            NetworkClientDelegate.UnsentRequest second = newUnsentFindCoordinatorRequest();

            assertEquals(1234L, ncd.addAll(new NetworkClientDelegate.PollResult(1234L, Arrays.asList(first, second))));
            assertEquals(2, ncd.unsentRequests().size());

            // An empty result does not queue anything and reports "no need to poll".
            assertEquals(Long.MAX_VALUE, ncd.addAll(NetworkClientDelegate.PollResult.EMPTY));
            assertEquals(2, ncd.unsentRequests().size());
        }
    }

    @Test
    public void testPollResultConstructors() {
        NetworkClientDelegate.UnsentRequest request = newUnsentFindCoordinatorRequest();

        assertEquals(Long.MAX_VALUE, NetworkClientDelegate.PollResult.EMPTY.timeUntilNextPollMs);
        assertTrue(NetworkClientDelegate.PollResult.EMPTY.unsentRequests.isEmpty());

        NetworkClientDelegate.PollResult single = new NetworkClientDelegate.PollResult(request);
        assertEquals(Long.MAX_VALUE, single.timeUntilNextPollMs);
        assertEquals(Collections.singletonList(request), single.unsentRequests);

        NetworkClientDelegate.PollResult list = new NetworkClientDelegate.PollResult(Collections.singletonList(request));
        assertEquals(Long.MAX_VALUE, list.timeUntilNextPollMs);
        assertEquals(1, list.unsentRequests.size());

        NetworkClientDelegate.PollResult timeOnly = new NetworkClientDelegate.PollResult(15L);
        assertEquals(15L, timeOnly.timeUntilNextPollMs);
        assertTrue(timeOnly.unsentRequests.isEmpty());
    }

    @Test
    public void testUnsentRequestWithNodeAndCallback() {
        AtomicReference<Throwable> error = new AtomicReference<>();
        Node node = mockNode();
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                        .setKey(GROUP_ID)
                        .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())),
                node,
                (response, t) -> error.set(t));

        assertEquals(Optional.of(node), request.node());
        request.handler().onFailure(time.milliseconds(), new TimeoutException());
        assertTrue(error.get() instanceof TimeoutException);
    }

    @Test
    public void testSupplierCreatesDelegateOnlyOnceAndLazily() throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ConsumerConfig config = new ConsumerConfig(properties);
        LogContext logContext = new LogContext();
        SubscriptionState subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners());
        metadata.bootstrap(ClientUtils.parseAndValidateAddresses(config));

        try (Metrics metrics = new Metrics(time)) {
            FetchMetricsManager fetchMetricsManager = new FetchMetricsManager(metrics, new FetchMetricsRegistry());
            Supplier<NetworkClientDelegate> supplier = NetworkClientDelegate.supplier(time,
                    logContext,
                    metadata,
                    config,
                    new ApiVersions(),
                    metrics,
                    fetchMetricsManager);

            NetworkClientDelegate first = supplier.get();
            assertNotNull(first);
            assertSame(first, supplier.get());
            first.close();
        }
    }

    public NetworkClientDelegate newNetworkClientDelegate() {
        LogContext logContext = new LogContext();
        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(GROUP_ID_CONFIG, GROUP_ID);
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        return new NetworkClientDelegate(this.time, new ConsumerConfig(properties), logContext, this.client);
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
