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
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.PollApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.join;
import static org.apache.kafka.common.utils.Utils.propsToMap;

/**
 * This {@link Consumer} implementation uses an {@link ApplicationEventHandler event handler} to process
 * {@link ApplicationEvent application events} so that the network I/O can be processed in a dedicated
 * {@link ConsumerNetworkThread network thread}. Visit
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Proposal%3A+Consumer+Threading+Model+Refactor" >this document</a>
 * for detail implementation.
 *
 * <p/>
 *
 * The fetch requests are sent by the network thread, which places the responses in a {@link FetchBuffer} shared with
 * the application thread. The decompression and deserialization of the records is performed on the application
 * thread, by the {@link FetchCollector}, when {@link #poll(Duration)} collects the fetch from the buffer.
 */
public class PrototypeAsyncConsumer<K, V> implements Consumer<K, V> {

    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    private final ApplicationEventHandler applicationEventHandler;
    private final Time time;
    private final Optional<String> groupId;
    private final String clientId;
    private final Logger log;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final Metrics metrics;
    private final long retryBackoffMs;
    private final long defaultApiTimeoutMs;
    private final List<ConsumerPartitionAssignor> assignors;
    private final Deserializers<K, V> deserializers;
    private final FetchBuffer fetchBuffer;
    private final FetchCollector<K, V> fetchCollector;
    private final ConsumerInterceptors<K, V> interceptors;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final BackgroundEventProcessor backgroundEventProcessor;
    private final WakeupTrigger wakeupTrigger = new WakeupTrigger();

    private volatile boolean closed = false;

    public PrototypeAsyncConsumer(final Properties properties,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this(Time.SYSTEM,
                new ConsumerConfig(appendDeserializerToConfig(propsToMap(properties), keyDeserializer, valueDeserializer)),
                keyDeserializer,
                valueDeserializer);
    }

    public PrototypeAsyncConsumer(final Time time,
                                  final ConsumerConfig config,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this.time = time;
        final GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                GroupRebalanceConfig.ProtocolType.CONSUMER);
        this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
        this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        final LogContext logContext = createLogContext(config, groupRebalanceConfig);
        this.log = logContext.logger(getClass());

        try {
            log.debug("Initializing the Kafka consumer");
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.metrics = createMetrics(config, time);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            final List<ConsumerInterceptor<K, V>> interceptorList = configuredConsumerInterceptors(config);
            this.interceptors = new ConsumerInterceptors<>(interceptorList);
            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
            this.subscriptions = createSubscriptionState(config, logContext);
            final ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    metrics.reporters(),
                    interceptorList,
                    Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
            this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            this.metadata.bootstrap(addresses);

            final FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
            final FetchConfig fetchConfig = new FetchConfig(config);
            final ApiVersions apiVersions = new ApiVersions();
            final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
            this.backgroundEventQueue = new LinkedBlockingQueue<>();
            this.backgroundEventProcessor = new BackgroundEventProcessor(logContext, backgroundEventQueue);

            // This FetchBuffer is shared between the application and network threads.
            this.fetchBuffer = new FetchBuffer(logContext);

            // None of these objects are instantiated here: they belong to the network thread, which creates them
            // (once) when it starts, so the application thread never holds a reference to them.
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(time,
                    logContext,
                    metadata,
                    config,
                    apiVersions,
                    metrics,
                    fetchMetricsManager,
                    fetchMetricsManager.throttleTimeSensor());
            final Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(time,
                    logContext,
                    backgroundEventQueue,
                    metadata,
                    subscriptions,
                    fetchBuffer,
                    config,
                    groupRebalanceConfig,
                    apiVersions,
                    fetchMetricsManager,
                    networkClientDelegateSupplier);
            final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(logContext,
                    metadata,
                    applicationEventQueue,
                    requestManagersSupplier);
            this.applicationEventHandler = new ApplicationEventHandler(logContext,
                    time,
                    applicationEventQueue,
                    applicationEventProcessorSupplier,
                    networkClientDelegateSupplier,
                    requestManagersSupplier);

            this.fetchCollector = new FetchCollector<>(logContext,
                    metadata,
                    subscriptions,
                    fetchConfig,
                    deserializers,
                    fetchMetricsManager,
                    time);
            this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );

            // no coordinator will be constructed for the default (null) group id
            config.logUnused();
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                close(Duration.ZERO, true);
            }
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    // Visible for testing
    PrototypeAsyncConsumer(final LogContext logContext,
                           final String clientId,
                           final Deserializers<K, V> deserializers,
                           final FetchBuffer fetchBuffer,
                           final FetchCollector<K, V> fetchCollector,
                           final ConsumerInterceptors<K, V> interceptors,
                           final Time time,
                           final ApplicationEventHandler applicationEventHandler,
                           final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                           final Metrics metrics,
                           final SubscriptionState subscriptions,
                           final ConsumerMetadata metadata,
                           final long retryBackoffMs,
                           final int defaultApiTimeoutMs,
                           final List<ConsumerPartitionAssignor> assignors,
                           final String groupId) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.clientId = clientId;
        this.fetchBuffer = fetchBuffer;
        this.fetchCollector = fetchCollector;
        this.interceptors = Objects.requireNonNull(interceptors);
        this.time = time;
        this.backgroundEventQueue = backgroundEventQueue;
        this.backgroundEventProcessor = new BackgroundEventProcessor(logContext, backgroundEventQueue);
        this.metrics = metrics;
        this.groupId = Optional.ofNullable(groupId);
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.deserializers = deserializers;
        this.applicationEventHandler = applicationEventHandler;
        this.assignors = assignors;
    }

    /**
     * poll implementation using {@link ApplicationEventHandler}.
     * <ol>
     *     <li>Let the network thread know that the consumer is still polling, and process the events that the network
     *     thread produced for the application thread; errors are rethrown here.</li>
     *     <li>Update the fetch positions, if needed.</li>
     *     <li>Collect the records that the network thread placed in the {@link FetchBuffer}, waiting for them to
     *     arrive until the timeout expires. The records are decompressed and deserialized on this thread.</li>
     * </ol>
     * If the timeout expires, an empty {@link ConsumerRecords} is returned.
     *
     * @param timeout timeout of the poll loop
     * @return ConsumerRecord.  It can be empty if time timeout expires.
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        maybeThrowIfClosed();
        final Timer timer = time.timer(timeout);

        if (subscriptions.hasNoSubscriptionOrUserAssignment())
            throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");

        do {
            // Make sure to let the background thread know that we are still polling.
            applicationEventHandler.add(new PollApplicationEvent(timer.currentTimeMs()));

            // Errors that occurred in the network thread are rethrown here.
            backgroundEventProcessor.process();

            final Fetch<K, V> fetch = pollForFetches(timer);

            if (!fetch.isEmpty()) {
                if (fetch.records().isEmpty()) {
                    log.trace("Returning empty records from `poll()` "
                            + "since the consumer's position has advanced for at least one topic partition");
                }

                return interceptors.onConsume(new ConsumerRecords<>(fetch.records()));
            }
            // We will wait for retryBackoffMs
        } while (timer.notExpired());

        return ConsumerRecords.empty();
    }

    /**
     * Collects the fetch from the {@link FetchBuffer}. If there is nothing available yet, the network thread is
     * awakened so that it sends new fetches and the buffer is awaited until data arrives or the timer expires.
     */
    private Fetch<K, V> pollForFetches(final Timer timer) {
        long pollTimeout = timer.remainingMs();

        // if data is available already, return it immediately
        final Fetch<K, V> fetch = fetchCollector.collectFetch(fetchBuffer);

        if (!fetch.isEmpty())
            return fetch;

        // If the positions are not all known yet, the lookup (e.g. the committed offsets) may be backing off after a
        // failure, so do not block for longer than the retry backoff.
        if (!updateFetchPositionsIfNeeded(timer))
            pollTimeout = Math.min(pollTimeout, retryBackoffMs);

        // Make sure the network thread sends new fetches (it never re-sends the pending ones).
        applicationEventHandler.wakeupNetworkThread();

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        // Attempt to fetch any data. This will wait until the new data arrives or the timeout expires.
        fetchBuffer.awaitNotEmpty(time.timer(pollTimeout));

        return fetchCollector.collectFetch(fetchBuffer);
    }

    /**
     * Set the fetch position to the committed position (if there is one) or reset it using the
     * offset reset policy the user has configured (if partitions require reset)
     *
     * @return true if the operation completed without timing out
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws NoOffsetForPartitionException                          If no offset is stored for a given partition and no offset reset policy is
     *                                                                defined
     */
    private boolean updateFetchPositionsIfNeeded(final Timer timer) {
        if (subscriptions.hasAllFetchPositions())
            return true;

        // Validate positions using the partition leader end offsets, to detect if any partition
        // has been truncated due to a leader change. This will trigger an OffsetForLeaderEpoch
        // request, retrieve the partition end offsets, and validate the current position against it.
        applicationEventHandler.add(new ValidatePositionsApplicationEvent());

        if (subscriptions.hasAllFetchPositions())
            return true;

        // Reset positions using committed offsets retrieved from the group coordinator, for any
        // partitions which do not have a valid position and are not awaiting reset. This will
        // trigger an OffsetFetch request and update positions with the offsets retrieved. This
        // will only do a coordinator lookup if there are partitions which have missing
        // positions, so a consumer with manually assigned partitions can avoid a coordinator
        // dependence by always ensuring that assigned partitions have an initial position.
        if (isCommittedOffsetsManagementEnabled() && !refreshCommittedOffsetsIfNeeded(timer))
            return false;

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise a NoOffsetForPartitionException exception.
        subscriptions.resetInitializingPositions();

        // Reset positions using partition offsets retrieved from the leader, for any partitions
        // which are awaiting reset. This will trigger a ListOffset request, retrieve the
        // partition offsets according to the strategy (ex. earliest, latest), and update the
        // positions.
        applicationEventHandler.add(new ResetPositionsApplicationEvent());
        return true;
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     */
    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * This method sends a commit event to the ApplicationEventHandler and return.
     */
    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        CompletableFuture<Void> future = commit(offsets, false);
        final OffsetCommitCallback commitCallback = callback == null ? new DefaultOffsetCommitCallback() : callback;
        future.whenComplete((r, t) -> {
            if (t != null) {
                commitCallback.onComplete(offsets, new KafkaException(t));
            } else {
                commitCallback.onComplete(offsets, null);
            }
        }).exceptionally(e -> {
            throw new KafkaException(e);
        });
    }

    // Visible for testing
    CompletableFuture<Void> commit(Map<TopicPartition, OffsetAndMetadata> offsets, final boolean isWakeupable) {
        maybeThrowInvalidGroupIdException();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offsets);
        if (isWakeupable) {
            // the task can only be woken up if the top level API call is commitSync
            wakeupTrigger.setActiveTask(commitEvent.future());
        }
        applicationEventHandler.add(commitEvent);
        return commitEvent.future();
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public long position(TopicPartition partition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        maybeThrowInvalidGroupIdException();
        if (partitions.isEmpty()) {
            return new HashMap<>();
        }

        final OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(partitions);
        wakeupTrigger.setActiveTask(event.future());
        try {
            return applicationEventHandler.addAndGet(event, time.timer(timeout));
        } finally {
            wakeupTrigger.clearActiveTask();
        }
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent() || groupId.get().isEmpty()) {
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Set<TopicPartition> paused() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        // Keeping same argument validation error thrown by the current consumer implementation
        // to avoid API level changes.
        requireNonNull(timestampsToSearch, "Timestamps to search cannot be null");
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            // Exclude the earliest and latest offset here so the timestamp in the returned
            // OffsetAndTimestamp is always positive.
            if (entry.getValue() < 0)
                throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                        entry.getValue() + ". The target time cannot be negative.");
        }

        if (timestampsToSearch.isEmpty()) {
            return Collections.emptyMap();
        }
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampsToSearch,
                true);

        // If timeout is set to zero return empty immediately; otherwise try to get the results
        // and throw timeout exception if it cannot complete in time.
        if (timeout.toMillis() == 0L)
            return listOffsetsEvent.emptyResult();

        return applicationEventHandler.addAndGet(listOffsetsEvent, time.timer(timeout));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timeout);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           Duration timeout) {
        // Keeping same argument validation error thrown by the current consumer implementation
        // to avoid API level changes.
        requireNonNull(partitions, "Partitions cannot be null");

        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<TopicPartition, Long> timestampToSearch =
                partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> timestamp));
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampToSearch,
                false);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                applicationEventHandler.addAndGet(listOffsetsEvent, time.timer(timeout));
        return offsetAndTimestampMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(),
                e -> e.getValue().offset()));
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void enforceRebalance() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void enforceRebalance(String reason) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    @Override
    public void close(Duration timeout) {
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        if (!closed) {
            // need to close before setting the flag since the close function itself may need the consumer to be open
            try {
                close(timeout, false);
            } finally {
                closed = true;
            }
        }
    }

    private void close(Duration timeout, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        final AtomicReference<Throwable> firstException = new AtomicReference<>();

        if (applicationEventHandler != null) {
            try {
                applicationEventHandler.close(timeout);
            } catch (Throwable t) {
                firstException.compareAndSet(null, t);
                log.error("Failed to close the application event handler", t);
            }
        }

        closeQuietly(backgroundEventProcessor, "background event processor", firstException);
        closeQuietly(fetchBuffer, "fetch buffer", firstException);
        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);

        if (clientId != null && metrics != null)
            AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);

        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();

        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException)
                throw (InterruptException) exception;

            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    private void maybeThrowIfClosed() {
        if (closed)
            throw new IllegalStateException("This consumer has already been closed.");
    }

    @Override
    public void wakeup() {
        wakeupTrigger.wakeup();
    }

    /**
     * This method sends a commit event to the ApplicationEventHandler and waits for
     * the event to finish.
     *
     * @param timeout max wait time for the blocking operation.
     */
    @Override
    public void commitSync(final Duration timeout) {
        commitSync(subscriptions.allConsumed(), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        CompletableFuture<Void> commitFuture = commit(offsets, true);
        try {
            commitFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            throw new org.apache.kafka.common.errors.TimeoutException(e);
        } catch (final InterruptedException e) {
            throw new InterruptException(e);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof WakeupException)
                throw new WakeupException();
            throw new KafkaException(e);
        } finally {
            wakeupTrigger.clearActiveTask();
        }
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Set<TopicPartition> assignment() {
        return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
    }

    /**
     * Get the current subscription.  or an empty set if no such call has
     * been made.
     * @return The set of topics currently subscribed to
     */
    @Override
    public Set<String> subscription() {
        return Collections.unmodifiableSet(this.subscriptions.subscription());
    }

    @Override
    public void subscribe(Collection<String> topics) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Topic partitions collection to assign to cannot be null");
        }

        if (partitions.isEmpty()) {
            // TODO: implementation of unsubscribe() will be included in forthcoming commits.
            // this.unsubscribe();
            return;
        }

        for (TopicPartition tp : partitions) {
            String topic = (tp != null) ? tp.topic() : null;
            if (isBlank(topic))
                throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
        }

        fetchBuffer.retainAll(new HashSet<>(partitions));

        // assignment change event will trigger autocommit if it is configured and the group id is specified. This is
        // to make sure offsets of topic partitions the consumer is unsubscribing from are committed since there will
        // be no following rebalance
        applicationEventHandler.add(new AssignmentChangeApplicationEvent(this.subscriptions.allConsumed(), time.milliseconds()));

        log.info("Assigned to partition(s): {}", join(partitions, ", "));
        if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
            applicationEventHandler.add(new NewTopicsMetadataUpdateRequestEvent());
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        maybeThrowInvalidGroupIdException();

        if (pattern == null || pattern.toString().isEmpty())
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));

        throwIfNoAssignorsConfigured();
        log.info("Subscribed to pattern: '{}'", pattern);
        subscriptions.subscribe(pattern, listener);
        updatePatternSubscription(metadata.fetch());
        metadata.requestUpdateForNewTopics();
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    /**
     * Subscribes to the topics of the cluster that match the subscribed pattern. This mirrors what the classic
     * consumer's coordinator does when the metadata changes.
     */
    private void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = cluster.topics().stream()
                .filter(subscriptions::matchesSubscribedPattern)
                .collect(Collectors.toSet());

        if (subscriptions.subscribeFromPattern(topicsToSubscribe))
            metadata.requestUpdateForNewTopics();
    }

    private void throwIfNoAssignorsConfigured() {
        if (assignors == null || assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    @Override
    public void unsubscribe() {
        throw new KafkaException("method not implemented");
    }

    @Override
    @Deprecated
    public ConsumerRecords<K, V> poll(long timeout) {
        throw new KafkaException("method not implemented");
    }

    // Visible for testing
    WakeupTrigger wakeupTrigger() {
        return wakeupTrigger;
    }

    /**
     *
     * Indicates if the consumer is using the Kafka-based offset management strategy,
     * according to config {@link CommonClientConfigs#GROUP_ID_CONFIG}
     */
    private boolean isCommittedOffsetsManagementEnabled() {
        return groupId.isPresent();
    }

    /**
     * Refresh the committed offsets for partitions that require initialization.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    private boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();

        log.debug("Refreshing committed offsets for partitions {}", initializingPartitions);
        try {
            final Map<TopicPartition, OffsetAndMetadata> offsets = applicationEventHandler.addAndGet(new OffsetFetchApplicationEvent(initializingPartitions), timer);
            ConsumerUtils.refreshCommittedOffsets(offsets, metadata, subscriptions);
            return true;
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            log.error("Couldn't refresh committed offsets before timeout expired");
            return false;
        }
    }

    // This is here temporary as we don't have public access to the ConsumerConfig in this module.
    public static Map<String, Object> appendDeserializerToConfig(Map<String, Object> configs,
                                                                 Deserializer<?> keyDeserializer,
                                                                 Deserializer<?> valueDeserializer) {
        // validate deserializer configuration, if the passed deserializer instance is null, the user must explicitly set a valid deserializer configuration value
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keyDeserializer != null)
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        else if (newConfigs.get(KEY_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(KEY_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        if (valueDeserializer != null)
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        else if (newConfigs.get(VALUE_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(VALUE_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        return newConfigs;
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }
}
