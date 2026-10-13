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

import java.util.function.Supplier;

/**
 * Based on the {@link Supplier} contract, {@link #get()} is expected to be called multiple times and each call should
 * return the same instance. This {@link Supplier} creates the instance lazily, on the first call to {@link #get()},
 * via the {@link #create()} method and caches it for all subsequent calls.
 *
 * <p/>
 *
 * This is used for objects that must be instantiated on a specific thread (e.g. the consumer network thread) and
 * not on the thread that holds the supplier.
 *
 * @param <T> Type of object that is created by the supplier
 */
public abstract class CachedSupplier<T> implements Supplier<T> {

    private T result;

    /**
     * Creates the instance that will be cached and returned by {@link #get()}. This is invoked at most once.
     */
    protected abstract T create();

    @Override
    public synchronized T get() {
        if (result == null)
            result = create();

        return result;
    }
}
