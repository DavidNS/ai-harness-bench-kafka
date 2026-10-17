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
package org.apache.kafka.server;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.IneligibleReplicaException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT}, brokers = 3, disksPerBroker = 2)
public class aih_probeCordonedTest {
    private final ClusterInstance cluster;

    public aih_probeCordonedTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    private static void alter(Admin admin, int id, AlterConfigOp op) throws Exception {
        ConfigResource r = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id));
        admin.incrementalAlterConfigs(Map.of(r, (Collection<AlterConfigOp>) List.of(op))).all().get();
    }

    private static void set(Admin admin, int id, String v) throws Exception {
        alter(admin, id, new AlterConfigOp(new ConfigEntry("cordoned.log.dirs", v), AlterConfigOp.OpType.SET));
    }

    private static void unset(Admin admin, int id) throws Exception {
        alter(admin, id, new AlterConfigOp(new ConfigEntry("cordoned.log.dirs", ""), AlterConfigOp.OpType.DELETE));
    }

    private static boolean failsWithRf(Admin admin, int rf) {
        try {
            admin.createTopics(List.of(new NewTopic("rf" + System.nanoTime(), 1, (short) rf))).all().get();
            return false;
        } catch (ExecutionException e) {
            return e.getCause() instanceof InvalidReplicationFactorException;
        } catch (Exception e) {
            return false;
        }
    }

    private static long gauge(String name, String dir) {
        for (var e : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            if (!e.getKey().getName().equals(name) || !e.getKey().getGroup().equals("kafka.log")) {
                continue;
            }
            if (dir == null || e.getKey().getMBeanName().contains(new File(dir).getName())) {
                return ((Number) ((Gauge<?>) e.getValue()).value()).longValue();
            }
        }
        throw new AssertionError("no metric " + name);
    }

    private static void expect(Class<? extends Throwable> cls, org.junit.jupiter.api.function.Executable ex) {
        ExecutionException t = assertThrows(ExecutionException.class, ex);
        assertInstanceOf(cls, t.getCause());
    }

    @ClusterTest
    public void probeMultiBroker() throws Exception {
        try (Admin admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic("r", Map.of(0, List.of(1, 2))))).all().get();
            cluster.waitTopicCreation("r", 1);
            set(admin, 0, "*");
            TestUtils.waitForCondition(() -> failsWithRf(admin, 3), 60_000, "rf3 should fail");
            assertTrue(admin.describeLogDirs(List.of(0)).allDescriptions().get().get(0).values().stream()
                .allMatch(LogDirDescription::isCordoned));
            assertTrue(admin.describeLogDirs(List.of(1)).allDescriptions().get().get(1).values().stream()
                .noneMatch(LogDirDescription::isCordoned));
            expect(InvalidReplicaAssignmentException.class, () ->
                admin.createTopics(List.of(new NewTopic("x", Map.of(0, List.of(0, 1))))).all().get());
            expect(IneligibleReplicaException.class, () -> admin.alterPartitionReassignments(Map.of(
                new TopicPartition("r", 0), Optional.of(new NewPartitionReassignment(List.of(1, 0))))).all().get());
            unset(admin, 0);
            TestUtils.waitForCondition(() -> !failsWithRf(admin, 3), 60_000, "rf3 should work again");
            assertTrue(admin.describeLogDirs(List.of(0)).allDescriptions().get().get(0).values().stream()
                .noneMatch(LogDirDescription::isCordoned));
        }
    }

    @ClusterTest(brokers = 1)
    public void probeSingleBroker() throws Exception {
        KafkaBroker broker = cluster.brokers().get(0);
        List<String> dirs = broker.config().logDirs().stream().map(d -> new File(d).getAbsolutePath()).toList();
        String cordoned = dirs.get(0);
        String other = dirs.get(1);
        try (Admin admin = cluster.admin()) {
            assertThrows(ExecutionException.class, () -> set(admin, 0, "/not/a/dir"));
            set(admin, 0, cordoned);
            TestUtils.waitForCondition(() -> broker.logManager().isLogDirCordoned(cordoned), 60_000, "cordon");
            assertEquals(1, gauge("LogDirectoryCordoned", cordoned));
            assertEquals(0, gauge("LogDirectoryCordoned", other));
            assertEquals(1, gauge("CordonedLogDirectoryCount", null));

            admin.createTopics(List.of(new NewTopic("p", 2, (short) 1))).all().get();
            cluster.waitTopicCreation("p", 2);
            TopicPartitionReplica replica = new TopicPartitionReplica("p", 0, 0);
            expect(InvalidReplicaAssignmentException.class,
                () -> admin.alterReplicaLogDirs(Map.of(replica, cordoned)).values().get(replica).get());

            Uuid id = broker.logManager().directoryId(cordoned).get();
            UnifiedLog log = broker.logManager().getOrCreateLog(new TopicPartition("sugg", 0), true, false,
                Optional.of(Uuid.randomUuid()), Option.apply(id));
            assertEquals(other, log.parentDir());

            unset(admin, 0);
            TestUtils.waitForCondition(() -> !broker.logManager().isLogDirCordoned(cordoned), 60_000, "uncordon");
            assertEquals(0, gauge("CordonedLogDirectoryCount", null));
        }
    }

    @ClusterTest(brokers = 1, metadataVersion = MetadataVersion.IBP_4_2_IV1, serverProperties = {
        @ClusterConfigProperty(key = "cordoned.log.dirs", value = "*")
    })
    public void probeOldMetadataVersion() throws Exception {
        try (Admin admin = cluster.admin()) {
            expect(InvalidConfigurationException.class, () -> set(admin, 0, "*"));
            assertTrue(admin.describeLogDirs(List.of(0)).allDescriptions().get().get(0).values().stream()
                .noneMatch(LogDirDescription::isCordoned));
            admin.createTopics(List.of(new NewTopic("t", 1, (short) 1))).all().get();
        }
    }
}
