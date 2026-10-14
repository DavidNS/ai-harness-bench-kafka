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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FetchConfigTest {

    @Test
    public void testFromConsumerConfig() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "10");
        p.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "2000");
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "300");
        p.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        p.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
        p.put(ConsumerConfig.CLIENT_RACK_CONFIG, "rack-a");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        FetchConfig fetchConfig = new FetchConfig(new ConsumerConfig(p));

        assertEquals(10, fetchConfig.minBytes);
        assertEquals(2000, fetchConfig.maxBytes);
        assertEquals(300, fetchConfig.maxWaitMs);
        assertEquals(1000, fetchConfig.fetchSize);
        assertEquals(50, fetchConfig.maxPollRecords);
        assertEquals(false, fetchConfig.checkCrcs);
        assertEquals("rack-a", fetchConfig.clientRackId);
        assertEquals(IsolationLevel.READ_COMMITTED, fetchConfig.isolationLevel);
    }

    @Test
    public void testDefaultIsolationLevelFromConsumerConfig() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        FetchConfig fetchConfig = new FetchConfig(new ConsumerConfig(p));

        assertEquals(IsolationLevel.READ_UNCOMMITTED, fetchConfig.isolationLevel);
        assertTrue(fetchConfig.checkCrcs);
    }

    @Test
    public void testFromValues() {
        FetchConfig fetchConfig = new FetchConfig(ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
                ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
                ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
                true,
                ConsumerConfig.DEFAULT_CLIENT_RACK,
                IsolationLevel.READ_UNCOMMITTED);

        assertEquals(ConsumerConfig.DEFAULT_FETCH_MIN_BYTES, fetchConfig.minBytes);
        assertEquals(ConsumerConfig.DEFAULT_MAX_POLL_RECORDS, fetchConfig.maxPollRecords);
        assertEquals(IsolationLevel.READ_UNCOMMITTED, fetchConfig.isolationLevel);
    }

    /**
     * The deserializers are no longer part of the {@link FetchConfig}; verify {@link Deserializers} still prevents
     * {@code null} key or value deserializers.
     */
    @Test
    @SuppressWarnings("resources")
    public void testPreventNullDeserializers() {
        try (StringDeserializer deserializer = new StringDeserializer()) {
            assertThrows(NullPointerException.class, () -> new Deserializers<>(null, deserializer));
            assertThrows(NullPointerException.class, () -> new Deserializers<>(deserializer, null));
        }
    }
}
