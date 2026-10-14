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
 * Interface provides a way for an event to provide a future for its result.
 *
 * @param <T> Return type of the event
 */
public interface CompletableEvent<T> {

    /**
     * Returns the {@link CompletableFuture future} that will be completed when the event has finished processing.
     *
     * @return Future that can be used to retrieve the result of the event
     */
    CompletableFuture<T> future();
}
