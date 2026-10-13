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

import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Exercises the {@link FetchRequestManager} the way the {@link ConsumerNetworkThread} does: the manager is polled for
 * requests, which are staged in a (real) {@link NetworkClientDelegate} that sends them using a {@link MockClient}.
 */
public class FetchRequestManagerTest {

    private static final String TOPIC = "test";
    private static final int NUM_PARTITIONS = 4;

    private final Uuid topicId = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = Collections.singletonMap(TOPIC, topicId);
    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime(1);

    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private MockClient client;
    private Metrics metrics;
    private FetchBuffer fetchBuffer;
    private NetworkClientDelegate networkClientDelegate;
    private FetchRequestManager fetchRequestManager;
    private FetchConfig fetchConfig;

    @BeforeEach
    public void setup() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        ConsumerConfig config = new ConsumerConfig(props);

        subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        metadata = new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false, subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = ConsumerUtils.createMetrics(config, time);
        fetchBuffer = new FetchBuffer(logContext);
        fetchConfig = new FetchConfig(config);
        networkClientDelegate = spy(new NetworkClientDelegate(time, config, logContext, client));
        fetchRequestManager = spy(newFetchRequestManager(networkClientDelegate));
    }

    @AfterEach
    public void tearDown() {
        fetchRequestManager.close();
        metrics.close();
    }

    private FetchRequestManager newFetchRequestManager(NetworkClientDelegate delegate) {
        return new FetchRequestManager(logContext,
                time,
                metadata,
                subscriptions,
                fetchConfig,
                fetchBuffer,
                ConsumerUtils.createFetchMetricsManager(metrics),
                delegate);
    }

    @Test
    public void testPollCreatesOneFetchRequestPerNode() {
        assignAndSeek(partitions());

        PollResult result = fetchRequestManager.poll(time.milliseconds());

        Set<Node> leaders = partitions().stream().map(this::leader).collect(Collectors.toSet());
        // The test is only meaningful if the partitions are led by more than one node
        assertTrue(leaders.size() > 1);
        assertEquals(leaders.size(), result.unsentRequests.size());
        result.unsentRequests.forEach(r -> assertInstanceOf(FetchRequest.Builder.class, r.requestBuilder()));
        assertEquals(PollResult.EMPTY.timeUntilNextPollMs, result.timeUntilNextPollMs);
    }

    @Test
    public void testPollWithoutFetchablePartitionsHasNoRequests() {
        assertTrue(fetchRequestManager.poll(time.milliseconds()).unsentRequests.isEmpty());
    }

    @Test
    public void testNoNewRequestWhileFetchIsPending() {
        assignAndSeek(partitions());

        assertFalse(sendFetches().isEmpty());
        // No response yet, so the requests are still in flight
        assertTrue(fetchRequestManager.poll(time.milliseconds()).unsentRequests.isEmpty());
    }

    @Test
    public void testSuccessfulResponseIsBufferedWithoutBeingDecoded() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        Node leader = leader(tp);
        List<UnsentRequest> requests = sendFetches();
        assertEquals(1, requests.size());

        client.respondFrom(fullFetchResponse(tp, MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()))), leader);
        networkClientDelegate.poll(0, time.milliseconds());

        assertFalse(fetchBuffer.isEmpty());
        CompletedFetch completedFetch = fetchBuffer.peek();
        assertEquals(tp, completedFetch.partition);
        // Decompression and deserialization are left to the application thread
        assertFalse(completedFetch.isInitialized());

        // ...which does that work when it collects the fetch from the buffer
        FetchCollector<String, String> collector = new FetchCollector<>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                new Deserializers<>(new StringDeserializer(), new StringDeserializer()),
                ConsumerUtils.createFetchMetricsManager(metrics),
                time);
        Fetch<String, String> fetch = collector.collectFetch(fetchBuffer);
        assertEquals(1, fetch.numRecords());
        ConsumerRecord<String, String> record = fetch.records().get(tp).get(0);
        assertEquals("key", record.key());
        assertEquals("value", record.value());
        assertEquals(1, subscriptions.position(tp).offset);
    }

    @Test
    public void testNewRequestCanBeSentOnceTheResponseWasHandled() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        sendFetches();
        client.respondFrom(fullFetchResponse(tp, MemoryRecords.EMPTY), leader(tp));
        networkClientDelegate.poll(0, time.milliseconds());

        // The response is buffered, and a partition with buffered data isn't fetched again until it is consumed
        assertFalse(fetchBuffer.isEmpty());
        assertTrue(fetchRequestManager.poll(time.milliseconds()).unsentRequests.isEmpty());

        fetchBuffer.poll();
        assertEquals(1, sendFetches().size());
    }

    @Test
    public void testFailureResponseDoesNotBufferAndAllowsNewRequest() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        sendFetches();

        client.respondFrom(fullFetchResponse(tp, MemoryRecords.EMPTY), leader(tp), true);
        networkClientDelegate.poll(0, time.milliseconds());

        assertTrue(fetchBuffer.isEmpty());
        // The failure releases the node, so a new request is made
        assertEquals(1, sendFetches().size());
    }

    @Test
    public void testFailureResponseClearsPreferredReadReplica() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        subscriptions.updatePreferredReadReplica(tp, leader(tp).id(), () -> time.milliseconds() + 60_000);
        assertTrue(subscriptions.preferredReadReplica(tp, time.milliseconds()).isPresent());
        sendFetches();

        client.respondFrom(fullFetchResponse(tp, MemoryRecords.EMPTY), leader(tp), true);
        networkClientDelegate.poll(0, time.milliseconds());

        assertFalse(subscriptions.preferredReadReplica(tp, time.milliseconds()).isPresent());
    }

    @Test
    public void testUnavailableNodeIsSkipped() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        doReturn(true).when(networkClientDelegate).isUnavailable(leader(tp));

        assertTrue(fetchRequestManager.poll(time.milliseconds()).unsentRequests.isEmpty());
        verify(networkClientDelegate).maybeThrowAuthFailure(leader(tp));
    }

    @Test
    public void testAuthenticationFailureIsThrownForUnavailableNode() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        doReturn(true).when(networkClientDelegate).isUnavailable(leader(tp));
        doThrow(new AuthenticationException("Bad credentials")).when(networkClientDelegate).maybeThrowAuthFailure(leader(tp));

        assertThrows(AuthenticationException.class, () -> fetchRequestManager.poll(time.milliseconds()));
    }

    @Test
    public void testPollOnCloseCreatesCloseFetchSessionRequests() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        sendFetches();
        client.respondFrom(fullFetchResponse(tp, MemoryRecords.EMPTY), leader(tp));
        networkClientDelegate.poll(0, time.milliseconds());

        PollResult result = fetchRequestManager.pollOnClose();

        assertEquals(1, result.unsentRequests.size());
        UnsentRequest unsentRequest = result.unsentRequests.get(0);
        FetchRequest.Builder builder = assertInstanceOf(FetchRequest.Builder.class, unsentRequest.requestBuilder());
        FetchRequest request = builder.build(ApiKeys.FETCH.latestVersion());
        assertEquals(FetchMetadata.FINAL_EPOCH, request.metadata().epoch());

        // The close request is sent, and handling its response doesn't blow up
        networkClientDelegate.addAll(result);
        networkClientDelegate.poll(0, time.milliseconds());
        client.respondFrom(fullFetchResponse(tp, MemoryRecords.EMPTY), leader(tp));
        networkClientDelegate.poll(0, time.milliseconds());
        assertTrue(unsentRequest.future().isDone());
        assertFalse(unsentRequest.future().isCompletedExceptionally());

        // The sessions were closed, so there is nothing left to close
        assertTrue(fetchRequestManager.pollOnClose().unsentRequests.isEmpty());
    }

    @Test
    public void testPollOnCloseHandlesFailureOfCloseFetchSessionRequest() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        sendFetches();
        client.respondFrom(fullFetchResponse(tp, MemoryRecords.EMPTY), leader(tp));
        networkClientDelegate.poll(0, time.milliseconds());
        fetchBuffer.poll();

        PollResult result = fetchRequestManager.pollOnClose();
        networkClientDelegate.addAll(result);
        networkClientDelegate.poll(0, time.milliseconds());
        client.respondFrom(fullFetchResponse(tp, MemoryRecords.EMPTY), leader(tp), true);
        networkClientDelegate.poll(0, time.milliseconds());

        assertTrue(result.unsentRequests.get(0).future().isCompletedExceptionally());
        assertTrue(fetchBuffer.isEmpty());
    }

    @Test
    public void testPollOnCloseSkipsUnavailableNodes() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        sendFetches();
        client.respondFrom(fullFetchResponse(tp, MemoryRecords.EMPTY), leader(tp));
        networkClientDelegate.poll(0, time.milliseconds());
        doReturn(true).when(networkClientDelegate).isUnavailable(any(Node.class));

        assertTrue(fetchRequestManager.pollOnClose().unsentRequests.isEmpty());
    }

    @Test
    public void testPollOnCloseWithoutSessionsHasNoRequests() {
        assertTrue(fetchRequestManager.pollOnClose().unsentRequests.isEmpty());
    }

    @Test
    public void testCloseIsIdempotent() {
        fetchRequestManager.close();
        fetchRequestManager.close();
        fetchRequestManager.close(time.timer(0));

        verify(fetchRequestManager, times(1)).closeInternal(any());
    }

    @Test
    public void testCloseClosesFetchBuffer() {
        TopicPartition tp = partitions().get(0);
        assignAndSeek(Collections.singletonList(tp));
        sendFetches();
        client.respondFrom(fullFetchResponse(tp, MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord("value".getBytes()))), leader(tp));
        networkClientDelegate.poll(0, time.milliseconds());
        assertFalse(fetchBuffer.isEmpty());

        fetchRequestManager.close();

        assertTrue(fetchBuffer.isEmpty());
    }

    @Test
    public void testSessionIdsAreTrackedPerNode() {
        assignAndSeek(partitions());
        List<UnsentRequest> requests = sendFetches();
        Set<Integer> nodeIds = partitions().stream().map(tp -> leader(tp).id()).collect(Collectors.toSet());
        assertEquals(requests.size(), nodeIds.size());

        for (int nodeId : nodeIds) {
            FetchSessionHandler handler = fetchRequestManager.sessionHandler(nodeId);
            assertNotEquals(null, handler);
        }
    }

    private List<UnsentRequest> sendFetches() {
        PollResult result = fetchRequestManager.poll(time.milliseconds());
        networkClientDelegate.addAll(result);
        networkClientDelegate.poll(0, time.milliseconds());
        return result.unsentRequests;
    }

    private List<TopicPartition> partitions() {
        return java.util.stream.IntStream.range(0, NUM_PARTITIONS)
                .mapToObj(i -> new TopicPartition(TOPIC, i))
                .collect(Collectors.toList());
    }

    private Node leader(TopicPartition tp) {
        return metadata.currentLeader(tp).leader.orElseThrow(() -> new IllegalStateException("No leader for " + tp));
    }

    private void assignAndSeek(List<TopicPartition> partitions) {
        subscriptions.assignFromUser(new HashSet<>(partitions));
        // The metadata only retains the topics that are assigned or subscribed, so it is updated after the assignment
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(2, Collections.singletonMap(TOPIC, NUM_PARTITIONS), topicIds));

        for (TopicPartition tp : partitions)
            subscriptions.seekValidated(tp, new SubscriptionState.FetchPosition(0, Optional.empty(), metadata.currentLeader(tp)));
    }

    private FetchResponse fullFetchResponse(TopicPartition tp, MemoryRecords records) {
        TopicIdPartition tidp = new TopicIdPartition(topicId, tp);
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(tp.partition())
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(100)
                .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                .setLogStartOffset(0)
                .setRecords(records);
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
        responseData.put(tidp, partitionData);
        return FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, responseData);
    }
}
