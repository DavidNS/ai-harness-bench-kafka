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
 * {@code FetchRequestManager} is responsible for generating {@link FetchRequest} for the fetchable
 * partitions of the {@link SubscriptionState}, based on the user's topic subscription/partition assignment.
 *
 * <p/>
 *
 * The responses are not processed here beyond being placed in the {@link FetchBuffer}: the decompression and
 * deserialization of the records is performed on the application thread, by {@link CompletedFetch} and
 * {@link FetchCollector}, when the fetch is collected from the {@link FetchBuffer}.
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
     * Creates one fetch request for each node that has fetchable partitions and no request in flight. There is
     * nothing to schedule: the result never limits how long the network thread can wait before polling again.
     */
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
     * Creates the {@link PollResult poll result} for the given fetch requests, wiring each request's callback to
     * the given handlers.
     *
     * @param fetchRequests  {@link Map} of {@link Node nodes} to their {@link FetchSessionHandler.FetchRequestData}
     * @param successHandler {@link ResponseHandler Handler} for successful responses
     * @param errorHandler   {@link ResponseHandler Handler} for failure responses
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
     * Callback invoked with the outcome (a response or an error) of a request to a node.
     */
    @FunctionalInterface
    private interface ResponseHandler<T> {

        void handle(Node target, FetchSessionHandler.FetchRequestData data, T response);
    }
}
