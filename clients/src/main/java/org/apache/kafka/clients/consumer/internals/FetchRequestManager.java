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
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * {@code FetchRequestManager} is responsible for generating {@link FetchRequest} that represent the
 * {@link SubscriptionState#fetchablePartitions fetchable partitions} based on the user's topic subscription/partition
 * assignment.
 *
 * <p/>
 *
 * This class runs on the consumer network thread. It does <em>not</em> decompress or deserialize the records: the
 * responses are only turned into {@link CompletedFetch}es that are added to the {@link FetchBuffer}, from where
 * they are collected, decompressed and deserialized on the application thread by the {@link FetchCollector}.
 */
public class FetchRequestManager extends AbstractFetch implements RequestManager {

    private final NetworkClientDelegate networkClientDelegate;

    public FetchRequestManager(final LogContext logContext,
                               final Time time,
                               final ConsumerMetadata metadata,
                               final SubscriptionState subscriptions,
                               final FetchConfig fetchConfig,
                               final FetchBuffer fetchBuffer,
                               final FetchMetricsManager metricsManager,
                               final NetworkClientDelegate networkClientDelegate) {
        super(logContext, metadata, subscriptions, fetchConfig, fetchBuffer, metricsManager, time);
        this.networkClientDelegate = networkClientDelegate;
    }

    @Override
    protected boolean isUnavailable(Node node) {
        return networkClientDelegate.isUnavailable(node);
    }

    @Override
    protected void maybeThrowAuthFailure(Node node) {
        networkClientDelegate.maybeThrowAuthFailure(node);
    }

    /**
     * Creates the fetch requests for the nodes that have fetchable partitions and no request in flight.
     *
     * @param currentTimeMs The current system time in milliseconds at the time of the poll
     * @return A {@link PollResult} with one {@link UnsentRequest} per node
     */
    @Override
    public PollResult poll(long currentTimeMs) {
        return pollInternal(
                prepareFetchRequests(),
                this::handleFetchSuccess,
                this::handleFetchFailure
        );
    }

    /**
     * Creates the requests that close the fetch sessions with the brokers. This is invoked by the network thread
     * when it is shutting down.
     */
    @Override
    public PollResult pollOnClose() {
        return pollInternal(
                prepareCloseFetchSessionRequests(),
                this::handleCloseFetchSessionSuccess,
                this::handleCloseFetchSessionFailure
        );
    }

    /**
     * Creates the {@link PollResult} from the per-node request data, wiring each request's completion to the
     * given success and failure handlers.
     */
    private PollResult pollInternal(Map<Node, FetchSessionHandler.FetchRequestData> fetchRequests,
                                    ResponseHandler<ClientResponse> successHandler,
                                    ResponseHandler<Throwable> errorHandler) {
        List<UnsentRequest> requests = fetchRequests.entrySet().stream().map(entry -> {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
            final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                if (error != null)
                    errorHandler.handle(fetchTarget, data, error);
                else
                    successHandler.handle(fetchTarget, data, clientResponse);
            };

            return new UnsentRequest(request, Optional.of(fetchTarget), responseHandler);
        }).collect(Collectors.toList());

        return new PollResult(requests);
    }

    @FunctionalInterface
    private interface ResponseHandler<T> {

        void handle(Node target, FetchSessionHandler.FetchRequestData data, T response);
    }
}
