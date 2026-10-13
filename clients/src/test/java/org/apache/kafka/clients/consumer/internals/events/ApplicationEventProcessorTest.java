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
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.HeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApplicationEventProcessorTest {

    private final LogContext logContext = new LogContext();
    private final TopicPartition tp = new TopicPartition("topic", 0);
    private BlockingQueue<ApplicationEvent> queue;
    private ConsumerMetadata metadata;
    private OffsetsRequestManager offsetsRequestManager;
    private TopicMetadataRequestManager topicMetadataRequestManager;
    private CommitRequestManager commitRequestManager;
    private ApplicationEventProcessor processor;

    @BeforeEach
    public void setup() {
        queue = new LinkedBlockingQueue<>();
        metadata = mock(ConsumerMetadata.class);
        offsetsRequestManager = mock(OffsetsRequestManager.class);
        topicMetadataRequestManager = mock(TopicMetadataRequestManager.class);
        commitRequestManager = mock(CommitRequestManager.class);
        processor = newProcessor(Optional.of(commitRequestManager));
    }

    private ApplicationEventProcessor newProcessor(Optional<CommitRequestManager> commit) {
        RequestManagers requestManagers = new RequestManagers(logContext,
                offsetsRequestManager,
                topicMetadataRequestManager,
                mock(FetchRequestManager.class),
                commit.map(c -> mock(CoordinatorRequestManager.class)),
                commit,
                commit.map(c -> mock(HeartbeatRequestManager.class)));
        return new ApplicationEventProcessor(logContext, queue, requestManagers, metadata);
    }

    @Test
    public void testPollEventUpdatesAutoCommitTimer() {
        processor.process(new PollApplicationEvent(123));

        verify(commitRequestManager).updateAutoCommitTimer(123);
    }

    @Test
    public void testPollEventWithoutCommitManagerIsIgnored() {
        assertDoesNotThrow(() -> newProcessor(Optional.empty()).process(new PollApplicationEvent(123)));
    }

    @Test
    public void testCommitEventIsChainedToTheCommitRequest() {
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(tp, new OffsetAndMetadata(10));
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        when(commitRequestManager.addOffsetCommitRequest(offsets)).thenReturn(commitFuture);
        CommitApplicationEvent event = new CommitApplicationEvent(offsets);

        processor.process(event);
        assertFalse(event.future().isDone());
        commitFuture.complete(null);

        assertTrue(event.future().isDone());
    }

    @Test
    public void testCommitEventWithoutCommitManagerFails() {
        CommitApplicationEvent event = new CommitApplicationEvent(Collections.singletonMap(tp, new OffsetAndMetadata(10)));

        newProcessor(Optional.empty()).process(event);

        assertTrue(event.future().isCompletedExceptionally());
    }

    @Test
    public void testOffsetFetchEventIsChainedToTheOffsetFetchRequest() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = Collections.singletonMap(tp, new OffsetAndMetadata(5));
        when(commitRequestManager.addOffsetFetchRequest(Collections.singleton(tp)))
                .thenReturn(CompletableFuture.completedFuture(committed));
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(Collections.singleton(tp));

        processor.process(event);

        assertEquals(committed, event.future().get());
    }

    @Test
    public void testOffsetFetchEventWithoutCommitManagerFails() {
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(Collections.singleton(tp));

        newProcessor(Optional.empty()).process(event);

        assertTrue(event.future().isCompletedExceptionally());
    }

    @Test
    public void testNewTopicsMetadataUpdateRequestsMetadataUpdate() {
        processor.process(new NewTopicsMetadataUpdateRequestEvent());

        verify(metadata).requestUpdateForNewTopics();
    }

    @Test
    public void testAssignmentChangeUpdatesTimerAndAutoCommits() {
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(tp, new OffsetAndMetadata(10));

        processor.process(new AssignmentChangeApplicationEvent(offsets, 456));

        verify(commitRequestManager).updateAutoCommitTimer(456);
        verify(commitRequestManager).maybeAutoCommit(offsets);
    }

    @Test
    public void testListOffsetsEventIsChainedToTheListOffsetsRequest() throws Exception {
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(tp, 100L);
        Map<TopicPartition, OffsetAndTimestamp> result = Collections.singletonMap(tp, new OffsetAndTimestamp(7, 100L));
        when(offsetsRequestManager.fetchOffsets(timestamps, true)).thenReturn(CompletableFuture.completedFuture(result));
        ListOffsetsApplicationEvent event = new ListOffsetsApplicationEvent(timestamps, true);

        processor.process(event);

        assertEquals(result, event.future().get());
    }

    @Test
    public void testResetPositionsEvent() {
        processor.process(new ResetPositionsApplicationEvent());

        verify(offsetsRequestManager).resetPositionsIfNeeded();
    }

    @Test
    public void testValidatePositionsEvent() {
        processor.process(new ValidatePositionsApplicationEvent());

        verify(offsetsRequestManager).validatePositionsIfNeeded();
    }

    @Test
    public void testTopicMetadataEventIsChainedToTheTopicMetadataRequest() throws Exception {
        Map<String, List<PartitionInfo>> metadataResult = Collections.singletonMap("topic", Collections.emptyList());
        when(topicMetadataRequestManager.requestTopicMetadata(Optional.of("topic")))
                .thenReturn(CompletableFuture.completedFuture(metadataResult));
        TopicMetadataApplicationEvent event = new TopicMetadataApplicationEvent("topic");

        processor.process(event);

        assertEquals(metadataResult, event.future().get());
    }

    @Test
    public void testProcessDrainsTheQueue() {
        queue.add(new PollApplicationEvent(1));
        queue.add(new PollApplicationEvent(2));
        queue.add(new ResetPositionsApplicationEvent());

        processor.process();

        assertTrue(queue.isEmpty());
        verify(commitRequestManager).updateAutoCommitTimer(1);
        verify(commitRequestManager).updateAutoCommitTimer(2);
        verify(offsetsRequestManager).resetPositionsIfNeeded();
    }

    @Test
    public void testFailureProcessingAnEventFailsItsFutureAndDoesNotStopTheOthers() {
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(tp, new OffsetAndMetadata(10));
        KafkaException failure = new KafkaException("Failure creating the commit request");
        doThrow(failure).when(commitRequestManager).addOffsetCommitRequest(offsets);
        CommitApplicationEvent failing = new CommitApplicationEvent(offsets);
        queue.add(failing);
        queue.add(new ResetPositionsApplicationEvent());

        assertDoesNotThrow(() -> processor.process());

        assertTrue(failing.future().isCompletedExceptionally());
        TestUtils.assertFutureThrows(failing.future(), KafkaException.class);
        verify(offsetsRequestManager).resetPositionsIfNeeded();
    }

    @Test
    public void testFailureProcessingAnEventWithoutFutureIsNotPropagated() {
        doThrow(new IllegalStateException("Failure")).when(offsetsRequestManager).resetPositionsIfNeeded();
        queue.add(new ResetPositionsApplicationEvent());
        queue.add(new ValidatePositionsApplicationEvent());

        assertDoesNotThrow(() -> processor.process());

        verify(offsetsRequestManager).validatePositionsIfNeeded();
    }

    @Test
    public void testEventClass() {
        assertEquals(ApplicationEvent.class, processor.getEventClass());
    }

    @Test
    public void testCloseCompletesPendingEventsExceptionally() {
        CommitApplicationEvent pending = new CommitApplicationEvent(Collections.singletonMap(tp, new OffsetAndMetadata(10)));
        queue.add(pending);

        processor.close();

        assertTrue(queue.isEmpty());
        assertTrue(pending.future().isCompletedExceptionally());
        TestUtils.assertFutureThrows(pending.future(), KafkaException.class);
    }

    @Test
    public void testCloseIsIdempotent() {
        processor.close();

        assertDoesNotThrow(processor::close);
    }

    @Test
    public void testProcessAfterCloseFails() {
        processor.close();

        assertThrows(IllegalStateException.class, processor::process);
    }

    @Test
    public void testSupplierCreatesTheProcessorLazilyAndOnlyOnce() {
        AtomicInteger requestManagersCreated = new AtomicInteger();
        RequestManagers requestManagers = new RequestManagers(logContext,
                offsetsRequestManager,
                topicMetadataRequestManager,
                mock(FetchRequestManager.class),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        Supplier<RequestManagers> requestManagersSupplier = () -> {
            requestManagersCreated.incrementAndGet();
            return requestManagers;
        };

        Supplier<ApplicationEventProcessor> supplier = ApplicationEventProcessor.supplier(logContext, metadata, queue, requestManagersSupplier);

        assertEquals(0, requestManagersCreated.get());
        ApplicationEventProcessor first = supplier.get();
        ApplicationEventProcessor second = supplier.get();
        assertSame(first, second);
        assertEquals(1, requestManagersCreated.get());
    }
}
