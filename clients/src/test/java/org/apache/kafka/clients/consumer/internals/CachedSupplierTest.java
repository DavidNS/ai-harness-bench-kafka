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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class CachedSupplierTest {

    @Test
    public void testCreateIsLazy() {
        AtomicInteger created = new AtomicInteger();
        CachedSupplier<Object> supplier = new CachedSupplier<Object>() {
            @Override
            protected Object create() {
                created.incrementAndGet();
                return new Object();
            }
        };

        assertEquals(0, created.get());
        supplier.get();
        assertEquals(1, created.get());
    }

    @Test
    public void testCreateIsInvokedOnlyOnce() {
        AtomicInteger created = new AtomicInteger();
        CachedSupplier<Object> supplier = new CachedSupplier<Object>() {
            @Override
            protected Object create() {
                created.incrementAndGet();
                return new Object();
            }
        };

        Object first = supplier.get();
        Object second = supplier.get();
        Object third = supplier.get();

        assertSame(first, second);
        assertSame(first, third);
        assertEquals(1, created.get());
    }

    @Test
    public void testSuppliersDoNotShareInstances() {
        CachedSupplier<Object> one = new CachedSupplier<Object>() {
            @Override
            protected Object create() {
                return new Object();
            }
        };
        CachedSupplier<Object> two = new CachedSupplier<Object>() {
            @Override
            protected Object create() {
                return new Object();
            }
        };

        assertNotSame(one.get(), two.get());
    }
}
