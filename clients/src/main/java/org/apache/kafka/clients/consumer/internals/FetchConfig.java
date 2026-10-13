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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredIsolationLevel;

/**
 * {@link FetchConfig} represents the static configuration for fetching records from Kafka. It is simply a way
 * to bundle the immutable settings that were presented at the time the {@link org.apache.kafka.clients.consumer.Consumer} was created for later use by
 * classes like {@link Fetcher}, {@link CompletedFetch}, etc.
 *
 * <p/>
 *
 * In most cases, the values stored and returned by {@link FetchConfig} will be those stored in the following
 * {@link ConsumerConfig consumer configuration} settings:
 *
 * <ul>
 *     <li>{@link #minBytes}: {@link ConsumerConfig#FETCH_MIN_BYTES_CONFIG}</li>
 *     <li>{@link #maxBytes}: {@link ConsumerConfig#FETCH_MAX_BYTES_CONFIG}</li>
 *     <li>{@link #maxWaitMs}: {@link ConsumerConfig#FETCH_MAX_WAIT_MS_CONFIG}</li>
 *     <li>{@link #fetchSize}: {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG}</li>
 *     <li>{@link #maxPollRecords}: {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG}</li>
 *     <li>{@link #checkCrcs}: {@link ConsumerConfig#CHECK_CRCS_CONFIG}</li>
 *     <li>{@link #clientRackId}: {@link ConsumerConfig#CLIENT_RACK_CONFIG}</li>
 *     <li>{@link #isolationLevel}: {@link ConsumerConfig#ISOLATION_LEVEL_CONFIG}</li>
 * </ul>
 *
 */
public class FetchConfig {

    final int minBytes;
    final int maxBytes;
    final int maxWaitMs;
    final int fetchSize;
    final int maxPollRecords;
    final boolean checkCrcs;
    final String clientRackId;
    final IsolationLevel isolationLevel;

    /**
     * Constructs a new {@link FetchConfig} using explicitly provided values. This is only intended for tests that
     * want to exercise specific scenarios without having to construct a {@link ConsumerConfig}.
     */
    // visible for testing
    FetchConfig(int minBytes,
                int maxBytes,
                int maxWaitMs,
                int fetchSize,
                int maxPollRecords,
                boolean checkCrcs,
                String clientRackId,
                IsolationLevel isolationLevel) {
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.checkCrcs = checkCrcs;
        this.clientRackId = clientRackId;
        this.isolationLevel = isolationLevel;
    }

    /**
     * Constructs a new {@link FetchConfig} using values from the given {@link ConsumerConfig consumer configuration}
     * surrounding the fetching of records.
     */
    public FetchConfig(ConsumerConfig config) {
        this.minBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        this.maxBytes = config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        this.maxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        this.fetchSize = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        this.maxPollRecords = config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        this.checkCrcs = config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG);
        this.clientRackId = config.getString(ConsumerConfig.CLIENT_RACK_CONFIG);
        this.isolationLevel = configuredIsolationLevel(config);
    }

    @Override
    public String toString() {
        return "FetchConfig{" +
                "minBytes=" + minBytes +
                ", maxBytes=" + maxBytes +
                ", maxWaitMs=" + maxWaitMs +
                ", fetchSize=" + fetchSize +
                ", maxPollRecords=" + maxPollRecords +
                ", checkCrcs=" + checkCrcs +
                ", clientRackId='" + clientRackId + '\'' +
                ", isolationLevel=" + isolationLevel +
                '}';
    }
}
