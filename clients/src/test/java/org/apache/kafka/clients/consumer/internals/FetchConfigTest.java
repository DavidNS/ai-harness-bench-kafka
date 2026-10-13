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
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FetchConfigTest {

    @Test
    public void testDefaultsFromConsumerConfig() {
        FetchConfig fetchConfig = new FetchConfig(newConsumerConfig(new Properties()));

        assertEquals(ConsumerConfig.DEFAULT_FETCH_MIN_BYTES, fetchConfig.minBytes);
        assertEquals(ConsumerConfig.DEFAULT_FETCH_MAX_BYTES, fetchConfig.maxBytes);
        assertEquals(ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS, fetchConfig.maxWaitMs);
        assertEquals(ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES, fetchConfig.fetchSize);
        assertEquals(ConsumerConfig.DEFAULT_MAX_POLL_RECORDS, fetchConfig.maxPollRecords);
        assertTrue(fetchConfig.checkCrcs);
        assertEquals(ConsumerConfig.DEFAULT_CLIENT_RACK, fetchConfig.clientRackId);
        assertEquals(IsolationLevel.READ_UNCOMMITTED, fetchConfig.isolationLevel);
    }

    @Test
    public void testOverridesFromConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "10");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "2000");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "30");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "400");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
        props.put(ConsumerConfig.CLIENT_RACK_CONFIG, "rack-1");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        FetchConfig fetchConfig = new FetchConfig(newConsumerConfig(props));

        assertEquals(10, fetchConfig.minBytes);
        assertEquals(2000, fetchConfig.maxBytes);
        assertEquals(30, fetchConfig.maxWaitMs);
        assertEquals(400, fetchConfig.fetchSize);
        assertEquals(5, fetchConfig.maxPollRecords);
        assertFalse(fetchConfig.checkCrcs);
        assertEquals("rack-1", fetchConfig.clientRackId);
        assertEquals(IsolationLevel.READ_COMMITTED, fetchConfig.isolationLevel);
    }

    private static ConsumerConfig newConsumerConfig(Properties overrides) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.putAll(overrides);
        return new ConsumerConfig(p);
    }
}
