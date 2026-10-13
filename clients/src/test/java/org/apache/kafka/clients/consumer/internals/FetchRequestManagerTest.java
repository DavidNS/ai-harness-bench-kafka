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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FetchRequestManagerTest {

    private static final String TOPIC = "test";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);
    private static final int FETCH_REQUEST_VERSION = 12;

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private FetchBuffer fetchBuffer;
    private FetchMetricsManager metricsManager;
    private NetworkClientDelegate networkClientDelegate;
    private FetchRequestManager manager;
    private Node leader;

    @BeforeEach
    public void setup() {
        subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        metadata = new ConsumerMetadata(0, 1000, 10000, false, false, subscriptions, logContext, new ClusterResourceListeners());

        fetchBuffer = new FetchBuffer(logContext);
        metricsManager = ConsumerUtils.createFetchMetricsManager(
                new Metrics(new MetricConfig().tags(Collections.singletonMap("client-id", "test-client")), time));
        networkClientDelegate = mock(NetworkClientDelegate.class);
        manager = new FetchRequestManager(logContext,
                time,
                metadata,
                subscriptions,
                new FetchConfig(1, 1024 * 1024, 500, 1024 * 1024, 100, true, "", IsolationLevel.READ_UNCOMMITTED),
                fetchBuffer,
                metricsManager,
                networkClientDelegate);
    }

    @AfterEach
    public void tearDown() {
        manager.close();
    }

    @Test
    public void testPollWithNothingToFetch() {
        PollResult pollResult = manager.poll(time.milliseconds());

        assertTrue(pollResult.unsentRequests.isEmpty());
        assertEquals(NetworkClientDelegate.PollResult.WAIT_FOREVER, pollResult.timeUntilNextPollMs);
    }

    @Test
    public void testPollCreatesOneFetchRequestPerNode() {
        assignAndSeek(TP0, TP1);

        PollResult pollResult = manager.poll(time.milliseconds());

        // Both partitions are led by the same node, so they share a request.
        assertEquals(1, pollResult.unsentRequests.size());
        UnsentRequest unsent = pollResult.unsentRequests.get(0);
        assertEquals(leader, unsent.node().orElse(null));
        assertTrue(unsent.requestBuilder() instanceof FetchRequest.Builder);
        assertEquals(ApiKeys.FETCH, unsent.requestBuilder().apiKey());
    }

    @Test
    public void testNoNewRequestWhileOneIsPending() {
        assignAndSeek(TP0);

        assertEquals(1, manager.poll(time.milliseconds()).unsentRequests.size());
        assertTrue(manager.poll(time.milliseconds()).unsentRequests.isEmpty());
    }

    @Test
    public void testSuccessfulResponseIsAddedToFetchBuffer() {
        assignAndSeek(TP0);
        UnsentRequest unsent = onlyRequest(manager.poll(time.milliseconds()));

        assertTrue(fetchBuffer.isEmpty());
        unsent.handler().onComplete(fetchResponse(TP0, 0));

        assertFalse(fetchBuffer.isEmpty());
        assertEquals(TP0, fetchBuffer.peek().partition);
        // The partition has buffered data, so nothing more is requested for it until the data has been consumed.
        assertTrue(manager.poll(time.milliseconds()).unsentRequests.isEmpty());
    }

    @Test
    public void testFailureAllowsNewRequest() {
        assignAndSeek(TP0);
        UnsentRequest unsent = onlyRequest(manager.poll(time.milliseconds()));
        assertTrue(manager.poll(time.milliseconds()).unsentRequests.isEmpty());

        unsent.handler().onFailure(time.milliseconds(), DisconnectException.INSTANCE);

        assertTrue(fetchBuffer.isEmpty());
        assertEquals(1, manager.poll(time.milliseconds()).unsentRequests.size());
    }

    @Test
    public void testUnavailableNodeIsSkipped() {
        assignAndSeek(TP0);
        when(networkClientDelegate.isUnavailable(leader)).thenReturn(true);

        assertTrue(manager.poll(time.milliseconds()).unsentRequests.isEmpty());
        verify(networkClientDelegate).maybeThrowAuthFailure(leader);
    }

    @Test
    public void testAuthenticationFailureIsThrownByPoll() {
        assignAndSeek(TP0);
        when(networkClientDelegate.isUnavailable(leader)).thenReturn(true);
        doThrow(new AuthenticationException("denied")).when(networkClientDelegate).maybeThrowAuthFailure(any());

        assertThrows(AuthenticationException.class, () -> manager.poll(time.milliseconds()));
    }

    @Test
    public void testFetchSessionIsReusedAcrossRequests() {
        assignAndSeek(TP0);
        int sessionId = 123;
        UnsentRequest first = onlyRequest(manager.poll(time.milliseconds()));
        assertTrue(buildRequest(first).metadata().isFull());
        first.handler().onComplete(fetchResponse(sessionId, TP0, 0));

        // Consume what was fetched so that the partition becomes fetchable again
        fetchBuffer.poll();
        UnsentRequest second = onlyRequest(manager.poll(time.milliseconds()));

        FetchRequest secondRequest = buildRequest(second);
        assertFalse(secondRequest.metadata().isFull());
        assertEquals(sessionId, secondRequest.metadata().sessionId());
    }

    @Test
    public void testPollOnCloseCreatesCloseSessionRequests() {
        assignAndSeek(TP0);
        int sessionId = 456;
        onlyRequest(manager.poll(time.milliseconds())).handler().onComplete(fetchResponse(sessionId, TP0, 0));

        PollResult pollResult = manager.pollOnClose();

        UnsentRequest closeRequest = onlyRequest(pollResult);
        assertEquals(leader, closeRequest.node().orElse(null));
        FetchRequest request = buildRequest(closeRequest);
        assertEquals(sessionId, request.metadata().sessionId());
        assertEquals(FetchMetadata.FINAL_EPOCH, request.metadata().epoch());

        // The success callback of a close request only logs.
        int bufferedBefore = fetchBuffer.bufferedPartitions().size();
        closeRequest.handler().onComplete(fetchResponse(sessionId, TP0, 0));
        assertTrue(closeRequest.future().isDone());
        assertFalse(closeRequest.future().isCompletedExceptionally());
        assertEquals(bufferedBefore, fetchBuffer.bufferedPartitions().size());
    }

    @Test
    public void testCloseSessionFailureIsHandled() {
        assignAndSeek(TP0);
        onlyRequest(manager.poll(time.milliseconds())).handler().onComplete(fetchResponse(789, TP0, 0));

        UnsentRequest closeRequest = onlyRequest(manager.pollOnClose());
        closeRequest.handler().onFailure(time.milliseconds(), DisconnectException.INSTANCE);

        assertTrue(closeRequest.future().isCompletedExceptionally());
    }

    @Test
    public void testPollOnCloseWithoutSessionsIsEmpty() {
        assertTrue(manager.pollOnClose().unsentRequests.isEmpty());
    }

    @Test
    public void testPollOnCloseSkipsUnavailableNodes() {
        assignAndSeek(TP0);
        onlyRequest(manager.poll(time.milliseconds())).handler().onComplete(fetchResponse(11, TP0, 0));
        when(networkClientDelegate.isUnavailable(leader)).thenReturn(true);

        assertTrue(manager.pollOnClose().unsentRequests.isEmpty());
    }

    @Test
    public void testCloseIsIdempotentAndClosesFetchBuffer() {
        assignAndSeek(TP0);
        onlyRequest(manager.poll(time.milliseconds())).handler().onComplete(fetchResponse(TP0, 0));
        assertFalse(fetchBuffer.isEmpty());

        manager.close();
        assertTrue(fetchBuffer.isEmpty());

        // Closing again must be harmless.
        manager.close();
    }

    @Test
    public void testRecordsAreNotDeserializedByTheRequestManager() {
        assignAndSeek(TP0);
        AtomicInteger deserializations = new AtomicInteger();
        Deserializer<String> countingDeserializer = new StringDeserializer() {
            @Override
            public String deserialize(String topic, Headers headers, ByteBuffer data) {
                deserializations.incrementAndGet();
                return super.deserialize(topic, headers, data);
            }
        };

        onlyRequest(manager.poll(time.milliseconds())).handler().onComplete(fetchResponse(TP0, 0));
        assertEquals(0, deserializations.get());

        // Deserialization happens when the application thread collects the fetch from the buffer.
        FetchCollector<String, String> collector = new FetchCollector<>(logContext,
                metadata,
                subscriptions,
                new FetchConfig(1, 1024 * 1024, 500, 1024 * 1024, 100, true, "", IsolationLevel.READ_UNCOMMITTED),
                new Deserializers<>(countingDeserializer, countingDeserializer),
                metricsManager,
                time);
        Fetch<String, String> fetch = collector.collectFetch(fetchBuffer);

        assertEquals(2, fetch.numRecords());
        // Both the key and the value of the two records
        assertEquals(4, deserializations.get());
    }

    private void assignAndSeek(TopicPartition... partitions) {
        subscriptions.assignFromUser(new HashSet<>(Arrays.asList(partitions)));

        // The metadata only retains the topics that the consumer is interested in, so it must be updated after the
        // assignment.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap(TOPIC, 2)),
                false, time.milliseconds());
        leader = metadata.fetch().nodes().get(0);

        for (TopicPartition tp : partitions) {
            subscriptions.seekValidated(tp, new SubscriptionState.FetchPosition(0, Optional.empty(), metadata.currentLeader(tp)));
        }
    }

    private static UnsentRequest onlyRequest(PollResult pollResult) {
        List<UnsentRequest> requests = pollResult.unsentRequests;
        assertEquals(1, requests.size());
        UnsentRequest unsent = requests.get(0);
        assertNotNull(unsent);
        return unsent;
    }

    private static FetchRequest buildRequest(UnsentRequest unsent) {
        return ((FetchRequest.Builder) unsent.requestBuilder()).build((short) FETCH_REQUEST_VERSION);
    }

    private ClientResponse fetchResponse(TopicPartition tp, long hw) {
        return fetchResponse(FetchMetadata.INVALID_SESSION_ID, tp, hw);
    }

    private ClientResponse fetchResponse(int sessionId, TopicPartition tp, long hw) {
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord("key".getBytes(), "value1".getBytes()),
                new SimpleRecord("key".getBytes(), "value2".getBytes()));
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = Collections.singletonMap(
                new TopicIdPartition(Uuid.ZERO_UUID, tp),
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp.partition())
                        .setErrorCode(Errors.NONE.code())
                        .setHighWatermark(hw + 2)
                        .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                        .setLogStartOffset(0)
                        .setRecords(records));
        FetchResponse response = FetchResponse.of(Errors.NONE, 0, sessionId, new LinkedHashMap<>(partitions));
        RequestHeader header = new RequestHeader(ApiKeys.FETCH, (short) FETCH_REQUEST_VERSION, "client", 1);
        return new ClientResponse(header, null, leader.idString(), time.milliseconds(), time.milliseconds() + 10,
                false, null, null, response);
    }
}
