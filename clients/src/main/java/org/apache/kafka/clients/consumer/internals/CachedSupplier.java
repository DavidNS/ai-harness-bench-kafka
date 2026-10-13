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
 * The objects used by the background thread must not be created by (or visible to) the application thread.
 * {@code CachedSupplier} defers creation of such an object until the first call to {@link #get()}, which then
 * caches it for all subsequent calls. Subclasses implement {@link #create()} to build the object.
 *
 * <p/>
 *
 * Note: this class is intentionally not thread-safe. It is expected to be resolved on the single thread that
 * owns the object it supplies.
 */
public abstract class CachedSupplier<T> implements Supplier<T> {

    private T result;

    protected abstract T create();

    @Override
    public T get() {
        if (result == null)
            result = create();

        return result;
    }
}
