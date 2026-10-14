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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * {@code FetchRequestManager} is responsible for generating {@link FetchRequest} that represent the
 * {@link SubscriptionState#fetchablePartitions} based on the user's topic subscription/partition
 * assignment.
 *
 * <p/>
 *
 * The requests are handed to the {@link ConsumerNetworkThread network thread} in the form of a {@link PollResult}.
 * The responses are processed on the network thread, but only to the extent that the raw {@link CompletedFetch}
 * objects are added to the {@link FetchBuffer}. Decompression and deserialization of the records are deferred to the
 * application thread, which performs them inside {@link CompletedFetch} and {@link FetchCollector} when the fetch is
 * collected from the {@link FetchBuffer}.
 */
public class FetchRequestManager extends AbstractFetch implements RequestManager {

    private final NetworkClientDelegate networkClientDelegate;

    FetchRequestManager(final LogContext logContext,
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

    @Override
    public PollResult poll(long currentTimeMs) {
        return pollInternal(
                prepareFetchRequests(),
                this::handleFetchSuccess,
                this::handleFetchFailure
        );
    }

    @Override
    public PollResult pollOnClose() {
        return pollInternal(
                prepareCloseFetchSessionRequests(),
                this::handleCloseFetchSessionSuccess,
                this::handleCloseFetchSessionFailure
        );
    }

    /**
     * Creates the {@link PollResult} for the given set of requests, using the provided handlers for the successful
     * and the failed outcome of each request.
     *
     * @param fetchRequests  {@link Map} of {@link Node nodes} to their {@link FetchSessionHandler.FetchRequestData}
     * @param successHandler {@link ResponseHandler Handler} for successful responses
     * @param errorHandler   {@link ResponseHandler Handler} for failed responses
     * @return {@link PollResult} with one {@link UnsentRequest} per node
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

            return new UnsentRequest(request, fetchTarget, responseHandler);
        }).collect(Collectors.toList());

        return new PollResult(requests);
    }

    /**
     * Callback for the outcome (success or failure) of a single fetch request.
     */
    @FunctionalInterface
    private interface ResponseHandler<T> {

        void handle(Node target, FetchSessionHandler.FetchRequestData data, T response);
    }
}
