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
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApplicationEventProcessorTest {

    private final LogContext logContext = new LogContext();
    private final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
    private final TopicPartition tp = new TopicPartition("topic", 0);

    private ConsumerMetadata metadata;
    private OffsetsRequestManager offsetsRequestManager;
    private TopicMetadataRequestManager topicMetadataRequestManager;
    private CommitRequestManager commitRequestManager;
    private ApplicationEventProcessor processor;

    @BeforeEach
    public void setup() {
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
                Optional.empty(),
                commit,
                Optional.empty());
        return new ApplicationEventProcessor(logContext, applicationEventQueue, requestManagers, metadata);
    }

    @Test
    public void testPollEventUpdatesAutoCommitTimer() {
        processor.process(new PollApplicationEvent(1234L));
        verify(commitRequestManager).updateAutoCommitTimer(1234L);
    }

    @Test
    public void testPollEventWithoutCommitManagerIsIgnored() {
        ApplicationEventProcessor withoutCommit = newProcessor(Optional.empty());
        assertDoesNotThrow(() -> withoutCommit.process(new PollApplicationEvent(1234L)));
    }

    @Test
    public void testCommitEventIsChainedToTheCommitManager() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(tp, new OffsetAndMetadata(10));
        CompletableFuture<Void> managerFuture = new CompletableFuture<>();
        when(commitRequestManager.addOffsetCommitRequest(offsets)).thenReturn(managerFuture);
        CommitApplicationEvent event = new CommitApplicationEvent(offsets);

        processor.process(event);
        assertFalse(event.future().isDone());

        managerFuture.complete(null);
        assertTrue(event.future().isDone());
        assertEquals(null, event.future().get());
    }

    @Test
    public void testCommitEventWithoutCommitManagerFailsTheFuture() {
        ApplicationEventProcessor withoutCommit = newProcessor(Optional.empty());
        CommitApplicationEvent event = new CommitApplicationEvent(singletonMap(tp, new OffsetAndMetadata(10)));

        withoutCommit.process(event);

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(KafkaException.class, e.getCause());
    }

    @Test
    public void testOffsetFetchEventIsChainedToTheCommitManager() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(tp, new OffsetAndMetadata(10));
        when(commitRequestManager.addOffsetFetchRequest(anySet())).thenReturn(CompletableFuture.completedFuture(offsets));
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(Collections.singleton(tp));

        processor.process(event);

        verify(commitRequestManager).addOffsetFetchRequest(Collections.singleton(tp));
        assertEquals(offsets, event.future().get());
    }

    @Test
    public void testOffsetFetchEventWithoutCommitManagerFailsTheFuture() {
        ApplicationEventProcessor withoutCommit = newProcessor(Optional.empty());
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(Collections.singleton(tp));

        withoutCommit.process(event);

        assertTrue(event.future().isCompletedExceptionally());
    }

    @Test
    public void testNewTopicsMetadataUpdateEvent() {
        processor.process(new NewTopicsMetadataUpdateRequestEvent());
        verify(metadata).requestUpdateForNewTopics();
    }

    @Test
    public void testAssignmentChangeEvent() {
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(tp, new OffsetAndMetadata(10));

        processor.process(new AssignmentChangeApplicationEvent(offsets, 99L));

        verify(commitRequestManager).updateAutoCommitTimer(99L);
        verify(commitRequestManager).maybeAutoCommit(offsets);
    }

    @Test
    public void testListOffsetsEventIsChainedToTheOffsetsManager() throws Exception {
        Map<TopicPartition, Long> timestamps = singletonMap(tp, 5L);
        Map<TopicPartition, OffsetAndTimestamp> result = singletonMap(tp, new OffsetAndTimestamp(1L, 5L));
        when(offsetsRequestManager.fetchOffsets(anyMap(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(result));
        ListOffsetsApplicationEvent event = new ListOffsetsApplicationEvent(timestamps, true);

        processor.process(event);

        verify(offsetsRequestManager).fetchOffsets(timestamps, true);
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
    public void testTopicMetadataEventIsChainedToTheTopicMetadataManager() throws Exception {
        Map<String, List<PartitionInfo>> result = singletonMap("topic", Collections.emptyList());
        when(topicMetadataRequestManager.requestTopicMetadata(any())).thenReturn(CompletableFuture.completedFuture(result));
        TopicMetadataApplicationEvent event = new TopicMetadataApplicationEvent("topic");

        processor.process(event);

        verify(topicMetadataRequestManager).requestTopicMetadata(Optional.of("topic"));
        assertEquals(result, event.future().get());
    }

    @Test
    public void testProcessDrainsTheQueueAndSwallowsErrors() {
        CommitApplicationEvent failing = new CommitApplicationEvent(singletonMap(tp, new OffsetAndMetadata(1)));
        when(commitRequestManager.addOffsetCommitRequest(any())).thenThrow(new IllegalStateException("boom"));
        applicationEventQueue.add(failing);
        applicationEventQueue.add(new NewTopicsMetadataUpdateRequestEvent());
        applicationEventQueue.add(new ResetPositionsApplicationEvent());

        // The error of the first event is logged, and does not prevent the others from being processed.
        assertDoesNotThrow(() -> processor.process());

        assertTrue(applicationEventQueue.isEmpty());
        verify(metadata).requestUpdateForNewTopics();
        verify(offsetsRequestManager).resetPositionsIfNeeded();
    }

    @Test
    public void testCloseFailsPendingCompletableEvents() {
        OffsetFetchApplicationEvent pending = new OffsetFetchApplicationEvent(Collections.singleton(tp));
        applicationEventQueue.add(pending);

        processor.close();
        processor.close();

        assertTrue(applicationEventQueue.isEmpty());
        assertTrue(pending.future().isCompletedExceptionally());
        verify(commitRequestManager, never()).addOffsetFetchRequest(anySet());
    }

    @Test
    public void testSupplierIsLazyAndCreatesTheProcessorOnlyOnce() {
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

        Supplier<ApplicationEventProcessor> supplier = ApplicationEventProcessor.supplier(logContext,
                metadata,
                applicationEventQueue,
                requestManagersSupplier);

        assertEquals(0, requestManagersCreated.get());

        ApplicationEventProcessor first = supplier.get();
        assertNotNull(first);
        assertSame(first, supplier.get());
        assertEquals(1, requestManagersCreated.get());
    }
}
