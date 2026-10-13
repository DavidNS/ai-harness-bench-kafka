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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FetchConfigTest {

    @Test
    public void testFromConsumerConfig() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "2");
        p.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "3000");
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "400");
        p.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "5000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "60");
        p.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
        p.put(ConsumerConfig.CLIENT_RACK_CONFIG, "rack-1");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        FetchConfig fetchConfig = new FetchConfig(new ConsumerConfig(p));

        assertEquals(2, fetchConfig.minBytes);
        assertEquals(3000, fetchConfig.maxBytes);
        assertEquals(400, fetchConfig.maxWaitMs);
        assertEquals(5000, fetchConfig.fetchSize);
        assertEquals(60, fetchConfig.maxPollRecords);
        assertEquals(false, fetchConfig.checkCrcs);
        assertEquals("rack-1", fetchConfig.clientRackId);
        assertEquals(IsolationLevel.READ_COMMITTED, fetchConfig.isolationLevel);
    }

    @Test
    public void testFromConsumerConfigDefaults() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        FetchConfig fetchConfig = new FetchConfig(new ConsumerConfig(p));

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
    public void testFromExplicitValues() {
        FetchConfig fetchConfig = new FetchConfig(ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
                ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
                ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
                true,
                ConsumerConfig.DEFAULT_CLIENT_RACK,
                IsolationLevel.READ_COMMITTED);

        assertEquals(ConsumerConfig.DEFAULT_FETCH_MIN_BYTES, fetchConfig.minBytes);
        assertEquals(IsolationLevel.READ_COMMITTED, fetchConfig.isolationLevel);
    }
}
