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

import java.util.concurrent.CompletableFuture;

/**
 * Interface for events that carry a {@link CompletableFuture} which is completed (successfully or exceptionally)
 * once the event has been processed. If the event is discarded without being processed, for example because the
 * consumer is being closed, the future is completed exceptionally so that callers waiting on it do not hang.
 *
 * @param <T> Type of the result of the event
 */
public interface CompletableEvent<T> {

    /**
     * @return {@link CompletableFuture} that is completed when the event has been processed
     */
    CompletableFuture<T> future();
}
