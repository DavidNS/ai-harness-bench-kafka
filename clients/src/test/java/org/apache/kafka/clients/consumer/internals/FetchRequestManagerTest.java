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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FetchRequestManagerTest {

    private final String topicName = "test";
    private final Uuid topicId = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = singletonMap(topicName, topicId);
    private final TopicPartition tp0 = new TopicPartition(topicName, 0);
    private final TopicPartition tp1 = new TopicPartition(topicName, 1);
    private final TopicIdPartition tidp0 = new TopicIdPartition(topicId, tp0);
    private final int validLeaderEpoch = 0;

    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime(1);
    private final AtomicInteger deserializedKeys = new AtomicInteger();

    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private MockClient client;
    private Metrics metrics;
    private FetchBuffer fetchBuffer;
    private FetchConfig fetchConfig;
    private FetchMetricsManager metricsManager;
    private NetworkClientDelegate networkClientDelegate;
    private FetchRequestManager fetchRequestManager;
    private FetchCollector<String, String> fetchCollector;

    @BeforeEach
    public void setup() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
        ConsumerConfig config = new ConsumerConfig(p);

        subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        metadata = new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false, subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = new Metrics(time);
        metricsManager = new FetchMetricsManager(metrics, new FetchMetricsRegistry(Collections.emptySet(), "consumer-test"));
        fetchBuffer = new FetchBuffer(logContext);
        fetchConfig = new FetchConfig(config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
                config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                0,
                config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                true,
                ConsumerConfig.DEFAULT_CLIENT_RACK,
                IsolationLevel.READ_UNCOMMITTED);
        networkClientDelegate = new NetworkClientDelegate(time, config, logContext, client);
        fetchRequestManager = new FetchRequestManager(logContext,
                time,
                metadata,
                subscriptions,
                fetchConfig,
                fetchBuffer,
                metricsManager,
                networkClientDelegate);

        Deserializer<String> countingKeyDeserializer = new StringDeserializer() {
            @Override
            public String deserialize(String topic, Headers headers, ByteBuffer data) {
                deserializedKeys.incrementAndGet();
                return super.deserialize(topic, headers, data);
            }
        };
        fetchCollector = new FetchCollector<>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                new Deserializers<>(countingKeyDeserializer, new StringDeserializer()),
                metricsManager,
                time);
    }

    @AfterEach
    public void teardown() {
        fetchRequestManager.close();
        metrics.close();
    }

    @Test
    public void testPollCreatesOneFetchRequestPerNode() {
        assignAndSeek(tp0, tp1);

        PollResult result = fetchRequestManager.poll(time.milliseconds());

        // Both partitions are led by the same node, so there is a single request for both of them.
        assertEquals(1, result.unsentRequests.size());
        UnsentRequest unsent = result.unsentRequests.get(0);
        assertEquals(ApiKeys.FETCH, unsent.requestBuilder().apiKey());
        assertTrue(unsent.node().isPresent());

        FetchRequest request = (FetchRequest) unsent.requestBuilder().build();
        assertEquals(2, request.fetchData(Collections.singletonMap(topicId, topicName)).size());
    }

    @Test
    public void testPollWithoutFetchablePartitionsHasNoRequests() {
        PollResult result = fetchRequestManager.poll(time.milliseconds());
        assertTrue(result.unsentRequests.isEmpty());
    }

    @Test
    public void testNoNewRequestWhilePreviousRequestIsPending() {
        assignAndSeek(tp0);

        assertEquals(1, fetchRequestManager.poll(time.milliseconds()).unsentRequests.size());
        assertTrue(fetchRequestManager.poll(time.milliseconds()).unsentRequests.isEmpty());
    }

    @Test
    public void testSuccessfulResponseIsBufferedUndeserialized() {
        assignAndSeek(tp0);

        client.prepareResponse(fullFetchResponse(INVALID_SESSION_ID, buildRecords(0L, 3)));
        sendAndReceive(fetchRequestManager.poll(time.milliseconds()));

        // The network thread only buffers the raw data...
        assertFalse(fetchBuffer.isEmpty());
        assertEquals(0, deserializedKeys.get());

        // ... and the records are deserialized by the collector, i.e. on the application thread.
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);
        List<ConsumerRecord<String, String>> records = fetch.records().get(tp0);
        assertEquals(3, records.size());
        assertEquals(3, deserializedKeys.get());
        assertEquals(3L, subscriptions.position(tp0).offset);
        assertTrue(fetchBuffer.isEmpty());
    }

    @Test
    public void testResponseAllowsNextRequestForTheNode() {
        assignAndSeek(tp0);

        client.prepareResponse(fullFetchResponse(INVALID_SESSION_ID, buildRecords(0L, 2)));
        sendAndReceive(fetchRequestManager.poll(time.milliseconds()));

        // The data is still buffered for the partition, so there is nothing new to fetch.
        assertTrue(fetchRequestManager.poll(time.milliseconds()).unsentRequests.isEmpty());

        fetchCollector.collectFetch(fetchBuffer);
        assertEquals(1, fetchRequestManager.poll(time.milliseconds()).unsentRequests.size());
    }

    @Test
    public void testFailedRequestClearsPendingStateAndAllowsRetry() {
        assignAndSeek(tp0);

        client.prepareResponse(fullFetchResponse(INVALID_SESSION_ID, buildRecords(0L, 2)), true);
        sendAndReceive(fetchRequestManager.poll(time.milliseconds()));

        assertTrue(fetchBuffer.isEmpty());
        assertEquals(1, fetchRequestManager.poll(time.milliseconds()).unsentRequests.size());
    }

    @Test
    public void testHandleFetchFailureCallbackIsInvokedOnError() {
        assignAndSeek(tp0);

        PollResult result = fetchRequestManager.poll(time.milliseconds());
        UnsentRequest unsent = result.unsentRequests.get(0);

        unsent.future().completeExceptionally(new RuntimeException("boom"));

        assertTrue(fetchBuffer.isEmpty());
        // The pending request for the node was removed by the failure handler.
        assertEquals(1, fetchRequestManager.poll(time.milliseconds()).unsentRequests.size());
    }

    @Test
    public void testPollOnCloseWithoutSessionsHasNoRequests() {
        assertTrue(fetchRequestManager.pollOnClose().unsentRequests.isEmpty());
    }

    @Test
    public void testPollOnCloseClosesFetchSessions() {
        assignAndSeek(tp0);

        // Establish a fetch session with the node.
        client.prepareResponse(fullFetchResponse(1, buildRecords(0L, 1)));
        sendAndReceive(fetchRequestManager.poll(time.milliseconds()));
        fetchCollector.collectFetch(fetchBuffer);

        PollResult result = fetchRequestManager.pollOnClose();
        assertEquals(1, result.unsentRequests.size());

        UnsentRequest unsent = result.unsentRequests.get(0);
        FetchRequest request = (FetchRequest) unsent.requestBuilder().build();
        assertEquals(1, request.metadata().sessionId());
        assertEquals(FetchMetadata.FINAL_EPOCH, request.metadata().epoch());

        // The sessions were cleared, so there is nothing more to close.
        assertTrue(fetchRequestManager.pollOnClose().unsentRequests.isEmpty());
    }

    @Test
    public void testCloseFetchSessionCallbacksDoNotThrow() {
        assignAndSeek(tp0);

        client.prepareResponse(fullFetchResponse(1, buildRecords(0L, 1)));
        sendAndReceive(fetchRequestManager.poll(time.milliseconds()));
        fetchCollector.collectFetch(fetchBuffer);

        PollResult result = fetchRequestManager.pollOnClose();
        UnsentRequest unsent = result.unsentRequests.get(0);
        unsent.future().completeExceptionally(new RuntimeException("unable to close"));
    }

    @Test
    public void testSkipsUnavailableNodeAndChecksAuthFailure() {
        NetworkClientDelegate delegate = mock(NetworkClientDelegate.class);
        when(delegate.isUnavailable(any(Node.class))).thenReturn(true);
        FetchRequestManager manager = new FetchRequestManager(logContext,
                time,
                metadata,
                subscriptions,
                fetchConfig,
                fetchBuffer,
                metricsManager,
                delegate);
        assignAndSeek(tp0);

        assertTrue(manager.poll(time.milliseconds()).unsentRequests.isEmpty());
        verify(delegate).maybeThrowAuthFailure(any(Node.class));
        manager.close();
    }

    @Test
    public void testAuthFailureOfUnavailableNodeIsThrown() {
        NetworkClientDelegate delegate = mock(NetworkClientDelegate.class);
        when(delegate.isUnavailable(any(Node.class))).thenReturn(true);
        doThrow(new AuthenticationException("denied"))
                .when(delegate).maybeThrowAuthFailure(any(Node.class));
        FetchRequestManager manager = new FetchRequestManager(logContext,
                time,
                metadata,
                subscriptions,
                fetchConfig,
                fetchBuffer,
                metricsManager,
                delegate);
        assignAndSeek(tp0);

        assertThrows(AuthenticationException.class, () -> manager.poll(time.milliseconds()));
        manager.close();
    }

    @Test
    public void testCloseIsIdempotent() {
        fetchRequestManager.close();
        fetchRequestManager.close();
    }

    /**
     * Hands the requests to the {@link NetworkClientDelegate} and polls the (mock) client, which sends the
     * requests and invokes the callbacks with the prepared responses.
     */
    private void sendAndReceive(PollResult pollResult) {
        networkClientDelegate.addAll(pollResult);
        networkClientDelegate.poll(0, time.milliseconds());
    }

    private void assignAndSeek(TopicPartition... partitions) {
        subscriptions.assignFromUser(new HashSet<>(Arrays.asList(partitions)));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 2), topicIds));

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), singletonMap(topicName, 2), tp -> validLeaderEpoch, topicIds), false, 0L);

        for (TopicPartition tp : partitions) {
            subscriptions.seekValidated(tp, new SubscriptionState.FetchPosition(0,
                    Optional.empty(), metadata.currentLeader(tp)));
        }
    }

    private FetchResponse fullFetchResponse(int sessionId, MemoryRecords records) {
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = new LinkedHashMap<>();
        partitions.put(tidp0, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition())
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(100L)
                .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                .setLogStartOffset(0)
                .setRecords(records));
        return FetchResponse.of(Errors.NONE, 0, sessionId, new LinkedHashMap<>(partitions));
    }

    private MemoryRecords buildRecords(long baseOffset, int count) {
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, baseOffset)) {
            for (int i = 0; i < count; i++)
                builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());
            return builder.build();
        }
    }
}
