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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class ApplicationEventProcessorTest {

    private final LogContext logContext = new LogContext();
    private final TopicPartition tp = new TopicPartition("topic", 0);
    private BlockingQueue<ApplicationEvent> queue;
    private ConsumerMetadata metadata;
    private CommitRequestManager commitRequestManager;
    private OffsetsRequestManager offsetsRequestManager;
    private TopicMetadataRequestManager topicMetadataRequestManager;
    private ApplicationEventProcessor processor;

    @BeforeEach
    public void setup() {
        queue = new LinkedBlockingQueue<>();
        metadata = mock(ConsumerMetadata.class);
        commitRequestManager = mock(CommitRequestManager.class);
        offsetsRequestManager = mock(OffsetsRequestManager.class);
        topicMetadataRequestManager = mock(TopicMetadataRequestManager.class);
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
        return new ApplicationEventProcessor(logContext, queue, requestManagers, metadata);
    }

    @Test
    public void testPollEvent() {
        queue.add(new PollApplicationEvent(1234L));

        processor.process();

        verify(commitRequestManager).updateAutoCommitTimer(1234L);
    }

    @Test
    public void testPollEventWithoutCommitManager() {
        processor = newProcessor(Optional.empty());
        queue.add(new PollApplicationEvent(1234L));

        processor.process();

        verifyNoInteractions(commitRequestManager);
    }

    @Test
    public void testCommitEvent() {
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(tp, new OffsetAndMetadata(10));
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        when(commitRequestManager.addOffsetCommitRequest(offsets)).thenReturn(commitFuture);
        CommitApplicationEvent event = new CommitApplicationEvent(offsets);
        queue.add(event);

        processor.process();

        assertFalse(event.future().isDone());
        commitFuture.complete(null);
        assertTrue(event.future().isDone());
    }

    @Test
    public void testCommitEventWithoutCommitManagerFailsTheEvent() {
        processor = newProcessor(Optional.empty());
        CommitApplicationEvent event = new CommitApplicationEvent(Collections.singletonMap(tp, new OffsetAndMetadata(10)));
        queue.add(event);

        processor.process();

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(KafkaException.class, e.getCause());
    }

    @Test
    public void testOffsetFetchEvent() throws Exception {
        Set<TopicPartition> partitions = Collections.singleton(tp);
        Map<TopicPartition, OffsetAndMetadata> committed = Collections.singletonMap(tp, new OffsetAndMetadata(5));
        when(commitRequestManager.addOffsetFetchRequest(partitions)).thenReturn(CompletableFuture.completedFuture(committed));
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(partitions);
        queue.add(event);

        processor.process();

        assertEquals(committed, event.future().get());
    }

    @Test
    public void testOffsetFetchEventForNoPartitionsCompletesWithoutRequest() throws Exception {
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(Collections.emptySet());
        queue.add(event);

        processor.process();

        assertTrue(event.future().get().isEmpty());
        verify(commitRequestManager, never()).addOffsetFetchRequest(any());
    }

    @Test
    public void testOffsetFetchEventWithoutCommitManagerFailsTheEvent() {
        processor = newProcessor(Optional.empty());
        OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(Collections.singleton(tp));
        queue.add(event);

        processor.process();

        assertTrue(event.future().isCompletedExceptionally());
    }

    @Test
    public void testNewTopicsMetadataUpdateEvent() {
        queue.add(new NewTopicsMetadataUpdateRequestEvent());

        processor.process();

        verify(metadata).requestUpdateForNewTopics();
    }

    @Test
    public void testAssignmentChangeEvent() {
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(tp, new OffsetAndMetadata(10));
        queue.add(new AssignmentChangeApplicationEvent(offsets, 99L));

        processor.process();

        verify(commitRequestManager).updateAutoCommitTimer(99L);
        verify(commitRequestManager).maybeAutoCommit(offsets);
    }

    @Test
    public void testListOffsetsEvent() throws Exception {
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(tp, 100L);
        Map<TopicPartition, OffsetAndTimestamp> result = Collections.singletonMap(tp, new OffsetAndTimestamp(7, 100L));
        when(offsetsRequestManager.fetchOffsets(timestamps, true)).thenReturn(CompletableFuture.completedFuture(result));
        ListOffsetsApplicationEvent event = new ListOffsetsApplicationEvent(timestamps, true);
        queue.add(event);

        processor.process();

        assertEquals(result, event.future().get());
    }

    @Test
    public void testResetPositionsEvent() {
        queue.add(new ResetPositionsApplicationEvent());

        processor.process();

        verify(offsetsRequestManager).resetPositionsIfNeeded();
    }

    @Test
    public void testValidatePositionsEvent() {
        queue.add(new ValidatePositionsApplicationEvent());

        processor.process();

        verify(offsetsRequestManager).validatePositionsIfNeeded();
    }

    @Test
    public void testTopicMetadataEvent() throws Exception {
        Map<String, List<PartitionInfo>> result = Collections.singletonMap("topic", Collections.emptyList());
        when(topicMetadataRequestManager.requestTopicMetadata(Optional.of("topic")))
                .thenReturn(CompletableFuture.completedFuture(result));
        TopicMetadataApplicationEvent event = new TopicMetadataApplicationEvent("topic");
        queue.add(event);

        processor.process();

        assertEquals(result, event.future().get());
    }

    @Test
    public void testEventsAreAllProcessedAndFailuresCompleteTheEventExceptionally() throws Exception {
        IllegalStateException failure = new IllegalStateException("cannot fetch offsets");
        doThrow(failure).when(offsetsRequestManager).fetchOffsets(any(), any(Boolean.class));
        ListOffsetsApplicationEvent failing = new ListOffsetsApplicationEvent(Collections.singletonMap(tp, 100L), true);
        queue.add(new PollApplicationEvent(1L));
        queue.add(failing);
        queue.add(new PollApplicationEvent(2L));

        // The failure must not propagate to the network thread
        processor.process();

        verify(commitRequestManager).updateAutoCommitTimer(1L);
        verify(commitRequestManager).updateAutoCommitTimer(2L);
        ExecutionException e = assertThrows(ExecutionException.class, () -> failing.future().get());
        assertSame(failure, e.getCause());
    }

    @Test
    public void testFailureOfNonCompletableEventIsNotPropagated() {
        doThrow(new IllegalStateException("boom")).when(offsetsRequestManager).resetPositionsIfNeeded();
        queue.add(new ResetPositionsApplicationEvent());
        queue.add(new ValidatePositionsApplicationEvent());

        processor.process();

        verify(offsetsRequestManager).validatePositionsIfNeeded();
    }

    @Test
    public void testCloseCompletesPendingEventsExceptionally() {
        OffsetFetchApplicationEvent pending = new OffsetFetchApplicationEvent(Collections.singleton(tp));
        queue.add(pending);

        processor.close();

        assertTrue(pending.future().isCompletedExceptionally());
        verifyNoInteractions(commitRequestManager);
    }

    @Test
    public void testCloseIsIdempotentAndProcessAfterCloseFails() {
        processor.close();
        processor.close();

        assertThrows(IllegalStateException.class, () -> processor.process());
    }

    @Test
    public void testSupplierIsLazyAndCachesTheProcessor() {
        int[] requestManagersCreated = {0};
        RequestManagers requestManagers = new RequestManagers(logContext,
                offsetsRequestManager,
                topicMetadataRequestManager,
                mock(FetchRequestManager.class),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        Supplier<RequestManagers> requestManagersSupplier = () -> {
            requestManagersCreated[0]++;
            return requestManagers;
        };

        Supplier<ApplicationEventProcessor> supplier =
                ApplicationEventProcessor.supplier(logContext, metadata, queue, requestManagersSupplier);

        assertEquals(0, requestManagersCreated[0]);

        ApplicationEventProcessor first = supplier.get();
        ApplicationEventProcessor second = supplier.get();

        assertNotNull(first);
        assertSame(first, second);
        assertEquals(1, requestManagersCreated[0]);
        verify(commitRequestManager, times(0)).updateAutoCommitTimer(any(Long.class));
    }
}
