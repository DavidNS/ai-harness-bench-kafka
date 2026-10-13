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
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CachedSupplierTest {

    @Test
    public void testNothingIsCreatedUntilGetIsCalled() {
        AtomicInteger created = new AtomicInteger();
        Supplier<Object> supplier = new CountingSupplier(created);

        assertEquals(0, created.get());

        supplier.get();

        assertEquals(1, created.get());
    }

    @Test
    public void testTheObjectIsCreatedOnceAndCached() {
        AtomicInteger created = new AtomicInteger();
        Supplier<Object> supplier = new CountingSupplier(created);

        Object first = supplier.get();
        Object second = supplier.get();
        Object third = supplier.get();

        assertSame(first, second);
        assertSame(first, third);
        assertEquals(1, created.get());
    }

    @Test
    public void testEachSupplierHasItsOwnObject() {
        AtomicInteger created = new AtomicInteger();

        Object first = new CountingSupplier(created).get();
        Object second = new CountingSupplier(created).get();

        assertEquals(2, created.get());
        assertEquals(false, first == second);
    }

    @Test
    public void testFailureToCreateIsNotCached() {
        AtomicInteger attempts = new AtomicInteger();
        Supplier<String> supplier = new CachedSupplier<String>() {
            @Override
            protected String create() {
                if (attempts.incrementAndGet() == 1)
                    throw new IllegalStateException("not ready");

                return "created";
            }
        };

        assertThrows(IllegalStateException.class, supplier::get);
        assertEquals("created", supplier.get());
        assertEquals("created", supplier.get());
        assertEquals(2, attempts.get());
    }

    private static class CountingSupplier extends CachedSupplier<Object> {
        private final AtomicInteger created;

        private CountingSupplier(AtomicInteger created) {
            this.created = created;
        }

        @Override
        protected Object create() {
            created.incrementAndGet();
            return new Object();
        }
    }
}
