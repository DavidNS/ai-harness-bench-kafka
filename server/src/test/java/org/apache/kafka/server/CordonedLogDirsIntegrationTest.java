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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.IneligibleReplicaException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(brokers = 3, disksPerBroker = 2, serverProperties = {
    @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
    @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1"),
    @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
    @ClusterConfigProperty(key = ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, value = "0")
})
public class CordonedLogDirsIntegrationTest {

    private final ClusterInstance cluster;

    CordonedLogDirsIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    private static ConfigResource broker(int brokerId) {
        return new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId));
    }

    private static void setCordoned(Admin admin, int brokerId, String value) throws Exception {
        admin.incrementalAlterConfigs(Map.of(broker(brokerId), List.of(new AlterConfigOp(
            new ConfigEntry(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, value), AlterConfigOp.OpType.SET)))).all().get();
    }

    private static void clearCordoned(Admin admin, int brokerId) throws Exception {
        admin.incrementalAlterConfigs(Map.of(broker(brokerId), List.of(new AlterConfigOp(
            new ConfigEntry(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, null), AlterConfigOp.OpType.DELETE)))).all().get();
    }

    private static Map<String, LogDirDescription> logDirs(Admin admin, int brokerId) throws Exception {
        return admin.describeLogDirs(List.of(brokerId)).allDescriptions().get().get(brokerId);
    }

    private static List<String> sortedLogDirPaths(Admin admin, int brokerId) throws Exception {
        return logDirs(admin, brokerId).keySet().stream().sorted().collect(Collectors.toList());
    }

    private static void waitForCordonedState(Admin admin, int brokerId, Set<String> expectedCordoned) throws Exception {
        TestUtils.waitForCondition(() -> {
            Map<String, LogDirDescription> dirs = logDirs(admin, brokerId);
            return dirs.entrySet().stream().allMatch(e -> e.getValue().isCordoned() == expectedCordoned.contains(e.getKey()));
        }, "Timed out waiting for the cordoned log directories of broker " + brokerId + " to be " + expectedCordoned);
    }

    /**
     * Wait until the controller rejects a topic of the given replication factor because of cordoned log directories,
     * or accepts it. The topic is only validated, never created.
     */
    private static void waitForPlacement(Admin admin, int replicationFactor, boolean expectAccepted) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                admin.createTopics(List.of(new NewTopic("probe", 1, (short) replicationFactor)),
                    new CreateTopicsOptions().validateOnly(true)).all().get();
                return expectAccepted;
            } catch (ExecutionException e) {
                assertInstanceOf(InvalidReplicationFactorException.class, e.getCause());
                return !expectAccepted;
            }
        }, "Timed out waiting for topics with replication factor " + replicationFactor +
            (expectAccepted ? " to be accepted" : " to be rejected"));
    }

    private static Set<Integer> replicaBrokers(Admin admin, String topic) throws Exception {
        return replicaBrokers(admin, topic, 1);
    }

    /**
     * The brokers hosting replicas of the topic, once the metadata shows at least the given number of partitions.
     */
    private static Set<Integer> replicaBrokers(Admin admin, String topic, int partitions) throws Exception {
        AtomicReference<Set<Integer>> result = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            try {
                var description = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
                if (description.partitions().size() < partitions) {
                    return false;
                }
                result.set(description.partitions().stream()
                    .flatMap(p -> p.replicas().stream())
                    .map(node -> node.id())
                    .collect(Collectors.toSet()));
                return true;
            } catch (ExecutionException e) {
                return false;
            }
        }, "Timed out waiting for the metadata of topic " + topic);
        return result.get();
    }

    private static long replicasInDir(Admin admin, int brokerId, String dir, String topic) throws Exception {
        return logDirs(admin, brokerId).get(dir).replicaInfos().keySet().stream()
            .filter(tp -> tp.topic().equals(topic))
            .count();
    }

    @ClusterTest
    public void testCordoningAllLogDirsOfABrokerExcludesItFromPlacement() throws Exception {
        try (Admin admin = cluster.admin()) {
            List<String> dirs = sortedLogDirPaths(admin, 2);
            assertEquals(2, dirs.size());
            logDirs(admin, 2).values().forEach(dir -> assertFalse(dir.isCordoned()));

            // A topic which exists before the broker is cordoned
            admin.createTopics(List.of(new NewTopic("pre-existing", 3, (short) 3))).all().get();
            TestUtils.waitForCondition(() -> dirs.stream().mapToLong(dir -> {
                try {
                    return replicasInDir(admin, 2, dir, "pre-existing");
                } catch (Exception ex) {
                    return 0;
                }
            }).sum() == 3, "Timed out waiting for the replicas of broker 2 to be created");

            setCordoned(admin, 2, ServerLogConfigs.CORDONED_LOG_DIRS_ALL);
            waitForCordonedState(admin, 2, Set.copyOf(dirs));
            // All the brokers are needed for a replication factor of three, so the request has to be rejected
            waitForPlacement(admin, 3, false);
            waitForPlacement(admin, 2, true);

            // New partitions are only placed on the other brokers
            admin.createTopics(List.of(new NewTopic("cordoned-all", 6, (short) 2))).all().get();
            assertEquals(Set.of(0, 1), replicaBrokers(admin, "cordoned-all"));

            // Explicit assignments onto the broker are invalid
            Map<Integer, List<Integer>> assignment = Map.of(0, List.of(0, 2));
            ExecutionException e = assertThrows(ExecutionException.class,
                () -> admin.createTopics(List.of(new NewTopic("explicit", assignment))).all().get());
            assertInstanceOf(InvalidReplicaAssignmentException.class, e.getCause());

            // So are partition reassignments
            TopicPartition tp = new TopicPartition("cordoned-all", 0);
            e = assertThrows(ExecutionException.class,
                () -> admin.alterPartitionReassignments(
                    Map.of(tp, Optional.of(new NewPartitionReassignment(List.of(0, 2))))).all().get());
            assertInstanceOf(IneligibleReplicaException.class, e.getCause());

            // Adding partitions follows the same rules
            admin.createPartitions(Map.of("cordoned-all", NewPartitions.increaseTo(8))).all().get();
            assertEquals(Set.of(0, 1), replicaBrokers(admin, "cordoned-all", 8));

            // The existing replicas on the broker keep being served
            assertEquals(3, dirs.stream().mapToLong(dir -> {
                try {
                    return replicasInDir(admin, 2, dir, "pre-existing");
                } catch (Exception ex) {
                    return 0;
                }
            }).sum());
            assertEquals(Set.of(0, 1, 2), replicaBrokers(admin, "pre-existing"));
        }
    }

    @ClusterTest
    public void testUncordoningMakesTheBrokerEligibleAgain() throws Exception {
        try (Admin admin = cluster.admin()) {
            setCordoned(admin, 2, ServerLogConfigs.CORDONED_LOG_DIRS_ALL);
            waitForPlacement(admin, 3, false);

            clearCordoned(admin, 2);
            waitForCordonedState(admin, 2, Set.of());
            waitForPlacement(admin, 3, true);

            admin.createTopics(List.of(new NewTopic("uncordoned", 3, (short) 3))).all().get();
            assertEquals(Set.of(0, 1, 2), replicaBrokers(admin, "uncordoned"));
        }
    }

    @ClusterTest
    public void testCordoningOneLogDirKeepsTheBrokerEligible() throws Exception {
        try (Admin admin = cluster.admin()) {
            List<String> dirs = sortedLogDirPaths(admin, 0);
            String cordonedDir = dirs.get(0);
            String openDir = dirs.get(1);

            setCordoned(admin, 0, cordonedDir);
            waitForCordonedState(admin, 0, Set.of(cordonedDir));
            // Broker 0 still has an uncordoned directory, so a replication factor of three works
            waitForPlacement(admin, 3, true);

            admin.createTopics(List.of(new NewTopic("one-dir", 6, (short) 3))).all().get();
            assertEquals(Set.of(0, 1, 2), replicaBrokers(admin, "one-dir"));
            TestUtils.waitForCondition(() -> replicasInDir(admin, 0, openDir, "one-dir") == 6,
                "Timed out waiting for the replicas to be created in the uncordoned log directory");
            assertEquals(0, replicasInDir(admin, 0, cordonedDir, "one-dir"));

            // A replica can't be moved onto the cordoned directory
            TopicPartitionReplica replica = new TopicPartitionReplica("one-dir", 0, 0);
            ExecutionException e = assertThrows(ExecutionException.class,
                () -> admin.alterReplicaLogDirs(Map.of(replica, cordonedDir)).all().get());
            assertInstanceOf(InvalidReplicaAssignmentException.class, e.getCause());

            // After uncordoning it can
            clearCordoned(admin, 0);
            waitForCordonedState(admin, 0, Set.of());
            admin.alterReplicaLogDirs(Map.of(replica, cordonedDir)).all().get();
            TestUtils.waitForCondition(() -> replicasInDir(admin, 0, cordonedDir, "one-dir") == 1,
                "Timed out waiting for the replica to be moved");
        }
    }

    @ClusterTest
    public void testInvalidCordonedLogDirIsRejected() throws Exception {
        try (Admin admin = cluster.admin()) {
            ExecutionException e = assertThrows(ExecutionException.class,
                () -> setCordoned(admin, 0, "/this/is/not/a/log/dir"));
            assertInstanceOf(InvalidConfigurationException.class, e.getCause());
            waitForCordonedState(admin, 0, Set.of());
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_2_IV1)
    public void testCordoningIsRejectedWithOldMetadataVersion() throws Exception {
        try (Admin admin = cluster.admin()) {
            ExecutionException e = assertThrows(ExecutionException.class,
                () -> setCordoned(admin, 0, ServerLogConfigs.CORDONED_LOG_DIRS_ALL));
            assertInstanceOf(InvalidConfigurationException.class, e.getCause());
            waitForCordonedState(admin, 0, Set.of());
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_2_IV1, serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, value = "*")
    })
    public void testStaticConfigHasNoEffectWithOldMetadataVersion() throws Exception {
        try (Admin admin = cluster.admin()) {
            waitForCordonedState(admin, 0, Set.of());
            admin.createTopics(List.of(new NewTopic("static", 3, (short) 3))).all().get();
            assertEquals(Set.of(0, 1, 2), replicaBrokers(admin, "static"));
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, value = "*")
    })
    public void testStaticConfigCordonsAllLogDirs() throws Exception {
        try (Admin admin = cluster.admin()) {
            for (int brokerId : cluster.brokerIds()) {
                waitForCordonedState(admin, brokerId, logDirs(admin, brokerId).keySet());
            }
            // Every broker is cordoned, so there is nowhere to place a new topic
            waitForPlacement(admin, 1, false);

            // Uncordoning one broker dynamically lets it receive partitions. The dynamic config overrides the static one.
            setCordoned(admin, 1, "");
            waitForCordonedState(admin, 1, Set.of());
            waitForPlacement(admin, 1, true);
            admin.createTopics(List.of(new NewTopic("static-all", 3, (short) 1))).all().get();
            assertEquals(Set.of(1), replicaBrokers(admin, "static-all"));
            assertTrue(logDirs(admin, 0).values().stream().allMatch(LogDirDescription::isCordoned));
        }
    }
}
