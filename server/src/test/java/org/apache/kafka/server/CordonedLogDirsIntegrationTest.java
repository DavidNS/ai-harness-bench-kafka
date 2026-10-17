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
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.IneligibleReplicaException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT}, brokers = 3, disksPerBroker = 2)
public class CordonedLogDirsIntegrationTest {
    private final ClusterInstance cluster;

    public CordonedLogDirsIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testNewPartitionsAvoidCordonedLogDir() throws Exception {
        try (Admin admin = cluster.admin()) {
            List<String> logDirs = logDirs(admin, 0);
            assertEquals(2, logDirs.size());
            String cordoned = logDirs.get(0);
            String uncordoned = logDirs.get(1);

            setCordoned(admin, 0, cordoned);
            waitForCordoned(admin, 0, Set.of(cordoned));

            admin.createTopics(List.of(new NewTopic("avoid", 8, (short) 3))).all().get();
            cluster.waitTopicCreation("avoid", 8);

            // Existing directories keep being served, but no new partition may be placed in the cordoned one.
            Map<String, LogDirDescription> descriptions = admin.describeLogDirs(List.of(0)).allDescriptions().get().get(0);
            assertTrue(descriptions.get(cordoned).isCordoned());
            assertFalse(descriptions.get(uncordoned).isCordoned());
            assertTrue(partitionsOf(descriptions.get(cordoned), "avoid").isEmpty());
            assertEquals(8, partitionsOf(descriptions.get(uncordoned), "avoid").size());

            // Uncordon: new partitions get spread over both directories again.
            removeCordoned(admin, 0);
            waitForCordoned(admin, 0, Set.of());
            assertFalse(admin.describeLogDirs(List.of(0)).allDescriptions().get().get(0).values().stream()
                .anyMatch(LogDirDescription::isCordoned));
            admin.createTopics(List.of(new NewTopic("spread", 8, (short) 3))).all().get();
            cluster.waitTopicCreation("spread", 8);
            descriptions = admin.describeLogDirs(List.of(0)).allDescriptions().get().get(0);
            assertFalse(partitionsOf(descriptions.get(cordoned), "spread").isEmpty());
        }
    }

    @ClusterTest
    public void testBrokerWithAllLogDirsCordonedReceivesNoPartitions() throws Exception {
        try (Admin admin = cluster.admin()) {
            setCordoned(admin, 0, ServerLogConfigs.CORDONED_LOG_DIRS_ALL);
            waitForCordoned(admin, 0, Set.copyOf(logDirs(admin, 0)));

            // Not enough brokers left to satisfy the replication factor.
            assertCreateTopicFails(admin, new NewTopic("rf3", 1, (short) 3), InvalidReplicationFactorException.class);

            // Enough brokers left: broker 0 is not used.
            admin.createTopics(List.of(new NewTopic("rf2", 6, (short) 2))).all().get();
            cluster.waitTopicCreation("rf2", 6);
            admin.describeTopics(List.of("rf2")).allTopicNames().get().get("rf2").partitions().forEach(p ->
                p.replicas().forEach(node -> assertTrue(node.id() != 0, "Broker 0 must not host new partitions")));

            // Explicit assignments onto the broker are rejected.
            assertCreateTopicFails(admin, new NewTopic("manual", Map.of(0, List.of(0, 1))),
                InvalidReplicaAssignmentException.class);
            admin.createTopics(List.of(new NewTopic("manual", Map.of(0, List.of(1, 2))))).all().get();
            cluster.waitTopicCreation("manual", 1);
            ExecutionException e = assertThrows(ExecutionException.class, () -> admin.createPartitions(
                Map.of("manual", NewPartitions.increaseTo(2, List.of(List.of(0, 1))))).all().get());
            assertInstanceOf(InvalidReplicaAssignmentException.class, e.getCause());
            // Without an assignment the broker is skipped
            admin.createPartitions(Map.of("manual", NewPartitions.increaseTo(3))).all().get();
            TestUtils.waitForCondition(() -> admin.describeTopics(List.of("manual")).allTopicNames().get()
                .get("manual").partitions().size() == 3, "Partitions were not created");
            admin.describeTopics(List.of("manual")).allTopicNames().get().get("manual").partitions().forEach(p ->
                p.replicas().forEach(node -> assertTrue(node.id() != 0, "Broker 0 must not host new partitions")));

            // Reassigning a partition onto the broker is rejected, but moving between the other ones works.
            TopicPartition tp = new TopicPartition("manual", 0);
            e = assertThrows(ExecutionException.class, () -> admin.alterPartitionReassignments(
                Map.of(tp, Optional.of(new NewPartitionReassignment(List.of(0, 1))))).all().get());
            assertInstanceOf(IneligibleReplicaException.class, e.getCause());

            // Once the broker is uncordoned, the same requests are accepted.
            removeCordoned(admin, 0);
            waitForCordoned(admin, 0, Set.of());
            admin.alterPartitionReassignments(
                Map.of(tp, Optional.of(new NewPartitionReassignment(List.of(0, 1))))).all().get();
            TestUtils.waitForCondition(() -> admin.listPartitionReassignments().reassignments().get().isEmpty(),
                "Reassignment did not complete");
            admin.createTopics(List.of(new NewTopic("rf3", 1, (short) 3))).all().get();
        }
    }

    @ClusterTest
    public void testAlterReplicaLogDirsToCordonedLogDir() throws Exception {
        try (Admin admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic("move", Map.of(0, List.of(1))))).all().get();
            cluster.waitTopicCreation("move", 1);

            List<String> logDirs = logDirs(admin, 1);
            String current = logDirs.stream()
                .filter(dir -> !partitionsOf(describe(admin, 1, dir), "move").isEmpty())
                .findFirst().orElseThrow();
            String other = logDirs.stream().filter(dir -> !dir.equals(current)).findFirst().orElseThrow();

            setCordoned(admin, 1, other);
            waitForCordoned(admin, 1, Set.of(other));

            TopicPartitionReplica replica = new TopicPartitionReplica("move", 0, 1);
            ExecutionException e = assertThrows(ExecutionException.class,
                () -> admin.alterReplicaLogDirs(Map.of(replica, other)).all().get());
            assertInstanceOf(InvalidReplicaAssignmentException.class, e.getCause());

            // The replica stays where it is
            assertEquals(1, partitionsOf(describe(admin, 1, current), "move").size());
            assertTrue(partitionsOf(describe(admin, 1, other), "move").isEmpty());

            // Moving into the directory works again after it is uncordoned.
            removeCordoned(admin, 1);
            waitForCordoned(admin, 1, Set.of());
            admin.alterReplicaLogDirs(Map.of(replica, other)).all().get();
            TestUtils.waitForCondition(() -> partitionsOf(describe(admin, 1, other), "move").size() == 1 &&
                    partitionsOf(describe(admin, 1, current), "move").isEmpty(),
                "The replica was not moved to the uncordoned log dir");
        }
    }

    @ClusterTest(brokers = 3, serverProperties = {
        @ClusterConfigProperty(id = 0, key = ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, value = ServerLogConfigs.CORDONED_LOG_DIRS_ALL)
    })
    public void testStaticCordonedLogDirs() throws Exception {
        try (Admin admin = cluster.admin()) {
            waitForCordoned(admin, 0, Set.copyOf(logDirs(admin, 0)));
            assertCreateTopicFails(admin, new NewTopic("static-rf3", 1, (short) 3), InvalidReplicationFactorException.class);
            admin.createTopics(List.of(new NewTopic("static-rf2", 4, (short) 2))).all().get();
            cluster.waitTopicCreation("static-rf2", 4);
            for (Map<String, LogDirDescription> descriptions : admin.describeLogDirs(List.of(1, 2)).allDescriptions().get().values()) {
                assertFalse(descriptions.values().stream().anyMatch(LogDirDescription::isCordoned));
            }
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_2_IV1, serverProperties = {
        @ClusterConfigProperty(id = 0, key = ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, value = ServerLogConfigs.CORDONED_LOG_DIRS_ALL)
    })
    public void testCordoningNeedsMetadataVersion() throws Exception {
        try (Admin admin = cluster.admin()) {
            // Setting the config dynamically is rejected
            String logDir = logDirs(admin, 1).get(0);
            ExecutionException e = assertThrows(ExecutionException.class, () -> setCordoned(admin, 1, logDir));
            assertInstanceOf(InvalidConfigurationException.class, e.getCause());

            // The static setting has no effect
            assertFalse(admin.describeLogDirs(List.of(0)).allDescriptions().get().get(0).values().stream()
                .anyMatch(LogDirDescription::isCordoned));
            admin.createTopics(List.of(new NewTopic("old-mv", 6, (short) 3))).all().get();
            cluster.waitTopicCreation("old-mv", 6);
            assertEquals(6, admin.describeLogDirs(List.of(0)).allDescriptions().get().get(0).values().stream()
                .mapToInt(description -> partitionsOf(description, "old-mv").size()).sum());
        }
    }

    @ClusterTest
    public void testInvalidCordonedLogDirIsRejected() throws Exception {
        try (Admin admin = cluster.admin()) {
            ExecutionException e = assertThrows(ExecutionException.class,
                () -> setCordoned(admin, 0, "/this/is/not/a/log/dir"));
            assertInstanceOf(ApiException.class, e.getCause());
            assertFalse(admin.describeLogDirs(List.of(0)).allDescriptions().get().get(0).values().stream()
                .anyMatch(LogDirDescription::isCordoned));
        }
    }

    private void assertCreateTopicFails(Admin admin, NewTopic topic, Class<? extends Throwable> expected) throws Exception {
        // The controller learns about a cordoned directory with the next heartbeat of the broker, so retry.
        TestUtils.waitForCondition(() -> {
            try {
                admin.createTopics(List.of(topic)).all().get();
                admin.deleteTopics(List.of(topic.name())).all().get();
                return false;
            } catch (ExecutionException e) {
                assertInstanceOf(expected, e.getCause());
                return true;
            }
        }, "Expected topic creation to fail with " + expected.getSimpleName());
    }

    private static List<String> logDirs(Admin admin, int brokerId) throws Exception {
        return admin.describeLogDirs(List.of(brokerId)).allDescriptions().get().get(brokerId).keySet().stream().sorted().toList();
    }

    private static LogDirDescription describe(Admin admin, int brokerId, String logDir) {
        try {
            return admin.describeLogDirs(List.of(brokerId)).allDescriptions().get().get(brokerId).get(logDir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Collection<TopicPartition> partitionsOf(LogDirDescription description, String topic) {
        return description.replicaInfos().keySet().stream()
            .filter(tp -> tp.topic().equals(topic))
            .collect(Collectors.toList());
    }

    private static void setCordoned(Admin admin, int brokerId, String value) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
            new ConfigEntry(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, value), AlterConfigOp.OpType.SET)))).all().get();
    }

    private static void removeCordoned(Admin admin, int brokerId) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
            new ConfigEntry(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, ""), AlterConfigOp.OpType.DELETE)))).all().get();
    }

    /**
     * Waits until the broker reports the given directories as cordoned, and until the controller
     * has seen that, which is visible in the registration which is replicated to all brokers.
     */
    private void waitForCordoned(Admin admin, int brokerId, Set<String> expected) throws Exception {
        TestUtils.waitForCondition(() -> {
            Map<String, LogDirDescription> descriptions = admin.describeLogDirs(List.of(brokerId)).allDescriptions().get().get(brokerId);
            return descriptions.entrySet().stream().filter(e -> e.getValue().isCordoned()).map(Map.Entry::getKey)
                .collect(Collectors.toSet()).equals(expected);
        }, "The broker did not report the expected cordoned log dirs " + expected);

        var broker = cluster.brokers().get(brokerId);
        Set<Uuid> expectedIds = expected.stream()
            .map(dir -> broker.logManager().directoryId(dir).get())
            .collect(Collectors.toSet());
        for (var other : cluster.brokers().values()) {
            TestUtils.waitForCondition(() -> {
                BrokerRegistration registration = ((KRaftMetadataCache) other.metadataCache()).getImage().cluster().broker(brokerId);
                return registration != null && Set.copyOf(registration.cordonedDirectories()).equals(expectedIds);
            }, "The controller did not register the cordoned log dirs of broker " + brokerId);
        }
    }
}
