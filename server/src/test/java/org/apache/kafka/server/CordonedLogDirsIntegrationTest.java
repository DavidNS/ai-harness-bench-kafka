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

import kafka.log.LogManager;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.IneligibleReplicaException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT}, brokers = 3, disksPerBroker = 2)
public class CordonedLogDirsIntegrationTest {

    private static final long WAIT_MS = 60_000;

    private final ClusterInstance cluster;
    private int topicCounter = 0;

    CordonedLogDirsIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    private String newTopicName() {
        return "topic-" + topicCounter++;
    }

    private static ConfigResource broker(int brokerId) {
        return new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    }

    private static void setCordoned(Admin admin, int brokerId, String value) throws Exception {
        admin.incrementalAlterConfigs(Map.of(broker(brokerId), List.of(new AlterConfigOp(
            new ConfigEntry(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, value), AlterConfigOp.OpType.SET)))).all().get();
    }

    private static void uncordon(Admin admin, int brokerId) throws Exception {
        admin.incrementalAlterConfigs(Map.of(broker(brokerId), List.of(new AlterConfigOp(
            new ConfigEntry(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, null), AlterConfigOp.OpType.DELETE)))).all().get();
    }

    private static Map<String, LogDirDescription> logDirs(Admin admin, int brokerId) throws Exception {
        return admin.describeLogDirs(List.of(brokerId)).allDescriptions().get().get(brokerId);
    }

    private static List<String> sortedLogDirs(Admin admin, int brokerId) throws Exception {
        return logDirs(admin, brokerId).keySet().stream().sorted().toList();
    }

    private static Set<String> cordonedLogDirs(Admin admin, int brokerId) throws Exception {
        return logDirs(admin, brokerId).entrySet().stream()
            .filter(e -> e.getValue().isCordoned())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    private static void waitForCordoned(Admin admin, int brokerId, Set<String> expected) throws Exception {
        TestUtils.waitForCondition(() -> expected.equals(cordonedLogDirs(admin, brokerId)), WAIT_MS,
            "Broker " + brokerId + " did not report the cordoned log directories " + expected);
    }

    private static Throwable createTopicError(Admin admin, NewTopic topic) throws Exception {
        try {
            admin.createTopics(List.of(topic)).all().get();
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private static Set<Integer> replicaBrokers(Admin admin, String topic) throws Exception {
        TopicDescription description = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
        Set<Integer> brokers = new HashSet<>();
        for (TopicPartitionInfo partition : description.partitions()) {
            partition.replicas().forEach(node -> brokers.add(node.id()));
        }
        return brokers;
    }

    /**
     * Waits until the controller refuses to place a replica on the given fully cordoned broker, which
     * means that it has seen the cordoned directories reported in the heartbeats of the broker.
     */
    private void waitForControllerToExcludeBroker(Admin admin, int brokerCount) throws Exception {
        TestUtils.waitForCondition(() ->
            createTopicError(admin, new NewTopic(newTopicName(), 1, (short) brokerCount)) instanceof InvalidReplicationFactorException,
            WAIT_MS, "The controller kept placing replicas on the cordoned broker");
    }

    private static <T extends Throwable> void assertCause(Class<T> expected, org.junit.jupiter.api.function.Executable executable) {
        ExecutionException e = assertThrows(ExecutionException.class, executable);
        assertInstanceOf(expected, e.getCause());
    }

    @ClusterTest
    public void testCordonedLogDirectoriesAreNotUsedForNewPartitions() throws Exception {
        try (Admin admin = cluster.admin()) {
            // Cordon the first directory of every broker.
            Map<Integer, String> cordoned = new java.util.HashMap<>();
            for (int brokerId : cluster.brokerIds()) {
                String dir = sortedLogDirs(admin, brokerId).get(0);
                cordoned.put(brokerId, dir);
                setCordoned(admin, brokerId, dir);
            }
            for (Map.Entry<Integer, String> entry : cordoned.entrySet()) {
                waitForCordoned(admin, entry.getKey(), Set.of(entry.getValue()));
            }

            String topic = newTopicName();
            admin.createTopics(List.of(new NewTopic(topic, 6, (short) 3))).all().get();
            cluster.waitTopicCreation(topic, 6);

            for (int brokerId : cluster.brokerIds()) {
                TestUtils.waitForCondition(() -> {
                    Map<String, LogDirDescription> descriptions = logDirs(admin, brokerId);
                    long replicasInCordonedDir = descriptions.get(cordoned.get(brokerId)).replicaInfos().keySet().stream()
                        .filter(tp -> tp.topic().equals(topic)).count();
                    long replicasInOtherDirs = descriptions.entrySet().stream()
                        .filter(e -> !e.getKey().equals(cordoned.get(brokerId)))
                        .flatMap(e -> e.getValue().replicaInfos().keySet().stream())
                        .filter(tp -> tp.topic().equals(topic)).count();
                    return replicasInCordonedDir == 0 && replicasInOtherDirs == 6;
                }, WAIT_MS, "Replicas of " + topic + " were not all placed in the uncordoned directory of broker " + brokerId);
            }

            // Removing the config uncordons the directories again.
            uncordon(admin, 0);
            waitForCordoned(admin, 0, Set.of());
            waitForCordoned(admin, 1, Set.of(cordoned.get(1)));

            // The cordoned state can also be changed by replacing the value.
            setCordoned(admin, 0, ServerLogConfigs.CORDONED_LOG_DIRS_ALL);
            waitForCordoned(admin, 0, new HashSet<>(sortedLogDirs(admin, 0)));
        }
    }

    @ClusterTest
    public void testBrokerWithAllLogDirectoriesCordonedReceivesNoNewPartitions() throws Exception {
        try (Admin admin = cluster.admin()) {
            setCordoned(admin, 2, ServerLogConfigs.CORDONED_LOG_DIRS_ALL);
            waitForCordoned(admin, 2, new HashSet<>(sortedLogDirs(admin, 2)));
            waitForControllerToExcludeBroker(admin, 3);

            // Auto placement only uses the other brokers.
            String topic = newTopicName();
            admin.createTopics(List.of(new NewTopic(topic, 6, (short) 2))).all().get();
            assertEquals(Set.of(0, 1), replicaBrokers(admin, topic));

            // Partitions added without assignment are not placed on the cordoned broker either.
            admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(12))).all().get();
            assertEquals(Set.of(0, 1), replicaBrokers(admin, topic));

            // Explicit assignments to the cordoned broker are invalid.
            assertCause(InvalidReplicaAssignmentException.class, () -> admin.createTopics(List.of(
                new NewTopic(newTopicName(), Map.of(0, List.of(0, 2))))).all().get());
            assertCause(InvalidReplicaAssignmentException.class, () -> admin.createPartitions(Map.of(
                topic, NewPartitions.increaseTo(13, List.of(List.of(0, 2))))).all().get());

            // Moving a replica to the cordoned broker is ineligible.
            String reassigned = newTopicName();
            admin.createTopics(List.of(new NewTopic(reassigned, Map.of(0, List.of(0))))).all().get();
            assertCause(IneligibleReplicaException.class, () -> admin.alterPartitionReassignments(Map.of(
                new TopicPartition(reassigned, 0), Optional.of(new NewPartitionReassignment(List.of(2))))).all().get());

            // Reassignments between the other brokers are still possible.
            admin.alterPartitionReassignments(Map.of(
                new TopicPartition(reassigned, 0), Optional.of(new NewPartitionReassignment(List.of(1))))).all().get();

            // Uncordoning makes the broker eligible again.
            uncordon(admin, 2);
            waitForCordoned(admin, 2, Set.of());
            TestUtils.waitForCondition(() -> {
                String name = newTopicName();
                return createTopicError(admin, new NewTopic(name, 1, (short) 3)) == null;
            }, WAIT_MS, "The controller kept excluding the uncordoned broker");
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(id = 1, key = ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, value = ServerLogConfigs.CORDONED_LOG_DIRS_ALL)
    })
    public void testStaticallyCordonedBroker() throws Exception {
        try (Admin admin = cluster.admin()) {
            waitForCordoned(admin, 1, new HashSet<>(sortedLogDirs(admin, 1)));
            assertEquals(Set.of(), cordonedLogDirs(admin, 0));
            waitForControllerToExcludeBroker(admin, 3);
        }
    }

    @ClusterTest
    public void testAlterReplicaLogDirsToCordonedDirectory() throws Exception {
        try (Admin admin = cluster.admin()) {
            String topic = newTopicName();
            admin.createTopics(List.of(new NewTopic(topic, Map.of(0, List.of(0))))).all().get();
            cluster.waitTopicCreation(topic, 1);
            TopicPartition tp = new TopicPartition(topic, 0);
            TopicPartitionReplica replica = new TopicPartitionReplica(topic, 0, 0);

            TestUtils.waitForCondition(() -> logDirs(admin, 0).values().stream()
                .anyMatch(d -> d.replicaInfos().containsKey(tp)), WAIT_MS, "The replica was not created");
            String currentDir = logDirs(admin, 0).entrySet().stream()
                .filter(e -> e.getValue().replicaInfos().containsKey(tp))
                .map(Map.Entry::getKey).findFirst().orElseThrow();
            String otherDir = sortedLogDirs(admin, 0).stream().filter(d -> !d.equals(currentDir)).findFirst().orElseThrow();

            setCordoned(admin, 0, otherDir);
            waitForCordoned(admin, 0, Set.of(otherDir));
            assertCause(InvalidReplicaAssignmentException.class,
                () -> admin.alterReplicaLogDirs(Map.of(replica, otherDir)).all().get());

            // Once the directory is uncordoned the replica can be moved to it.
            uncordon(admin, 0);
            waitForCordoned(admin, 0, Set.of());
            admin.alterReplicaLogDirs(Map.of(replica, otherDir)).all().get();
            TestUtils.waitForCondition(() -> logDirs(admin, 0).get(otherDir).replicaInfos().containsKey(tp)
                && !logDirs(admin, 0).get(currentDir).replicaInfos().containsKey(tp),
                WAIT_MS, "The replica was not moved to the uncordoned directory");
        }
    }

    @ClusterTest
    public void testInvalidCordonedLogDirsAreRejected() throws Exception {
        try (Admin admin = cluster.admin()) {
            // Not one of the log directories of the broker.
            assertCause(InvalidRequestException.class, () -> setCordoned(admin, 0, "/not/a/log/dir"));
            // A cordoned directory of another broker.
            String otherBrokerDir = sortedLogDirs(admin, 1).get(0);
            assertCause(InvalidRequestException.class, () -> setCordoned(admin, 0, otherBrokerDir));
            // The config is per broker, it can't be set cluster wide.
            assertCause(InvalidRequestException.class, () -> admin.incrementalAlterConfigs(Map.of(
                new ConfigResource(ConfigResource.Type.BROKER, ""), List.of(new AlterConfigOp(
                    new ConfigEntry(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, ServerLogConfigs.CORDONED_LOG_DIRS_ALL),
                    AlterConfigOp.OpType.SET)))).all().get());
            assertEquals(Set.of(), cordonedLogDirs(admin, 0));

            // Several valid directories can be cordoned at once.
            List<String> dirs = sortedLogDirs(admin, 0);
            setCordoned(admin, 0, String.join(",", dirs));
            waitForCordoned(admin, 0, new HashSet<>(dirs));
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_2_IV1, serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, value = ServerLogConfigs.CORDONED_LOG_DIRS_ALL)
    })
    public void testCordoningRequiresMetadataVersion() throws Exception {
        try (Admin admin = cluster.admin()) {
            // Setting the config dynamically is rejected.
            String dir = sortedLogDirs(admin, 0).get(0);
            assertCause(InvalidConfigurationException.class, () -> setCordoned(admin, 0, dir));

            // The statically configured value has no effect.
            for (int brokerId : cluster.brokerIds()) {
                assertEquals(Set.of(), cordonedLogDirs(admin, brokerId));
            }
            admin.createTopics(List.of(new NewTopic(newTopicName(), 3, (short) 3))).all().get();

            // Removing the config is not a problem.
            uncordon(admin, 0);
        }
    }

    @ClusterTest
    public void testBrokerDoesNotCreateReplicaInCordonedDirectory() throws Exception {
        try (Admin admin = cluster.admin()) {
            KafkaBroker broker = cluster.brokers().get(0);
            LogManager logManager = broker.logManager();
            List<String> dirs = sortedLogDirs(admin, 0);
            String cordonedDir = dirs.get(0);
            String uncordonedDir = dirs.get(1);

            setCordoned(admin, 0, cordonedDir);
            waitForCordoned(admin, 0, Set.of(cordonedDir));
            TestUtils.waitForCondition(() -> logManager.isCordoned(cordonedDir), WAIT_MS, "The log manager did not cordon the directory");

            // Even when the cordoned directory is the one suggested by the controller, a new replica is not created in it.
            Uuid cordonedDirId = logManager.directoryId(cordonedDir).get();
            for (int i = 0; i < 4; i++) {
                UnifiedLog log = logManager.getOrCreateLog(new TopicPartition("synthetic", i), true, false,
                    Optional.of(Uuid.randomUuid()), Option.apply(cordonedDirId));
                assertEquals(uncordonedDir, log.parentDir());
            }

            // When all the directories are cordoned the broker has to pick one of them anyway.
            setCordoned(admin, 0, ServerLogConfigs.CORDONED_LOG_DIRS_ALL);
            waitForCordoned(admin, 0, new HashSet<>(dirs));
            UnifiedLog log = logManager.getOrCreateLog(new TopicPartition("synthetic", 10), true, false,
                Optional.of(Uuid.randomUuid()), Option.empty());
            assertTrue(dirs.contains(log.parentDir()));
        }
    }

    @ClusterTest(brokers = 1)
    public void testCordonedLogDirectoryMetrics() throws Exception {
        try (Admin admin = cluster.admin()) {
            List<String> dirs = sortedLogDirs(admin, 0);
            assertEquals(2, dirs.size());
            assertEquals(0, cordonedCount());
            for (String dir : dirs) {
                assertEquals(0, cordonedGauge(dir));
            }

            setCordoned(admin, 0, dirs.get(0));
            TestUtils.waitForCondition(() -> cordonedCount() == 1 && cordonedGauge(dirs.get(0)) == 1 && cordonedGauge(dirs.get(1)) == 0,
                WAIT_MS, "The metrics did not reflect one cordoned directory");

            setCordoned(admin, 0, ServerLogConfigs.CORDONED_LOG_DIRS_ALL);
            TestUtils.waitForCondition(() -> cordonedCount() == 2 && cordonedGauge(dirs.get(0)) == 1 && cordonedGauge(dirs.get(1)) == 1,
                WAIT_MS, "The metrics did not reflect two cordoned directories");

            uncordon(admin, 0);
            TestUtils.waitForCondition(() -> cordonedCount() == 0 && cordonedGauge(dirs.get(0)) == 0 && cordonedGauge(dirs.get(1)) == 0,
                WAIT_MS, "The metrics did not reflect that no directory is cordoned");
        }
    }

    @ClusterTest(brokers = 1)
    public void testDescribeLogDirsReportsCordonedState() throws Exception {
        try (Admin admin = cluster.admin()) {
            List<String> dirs = sortedLogDirs(admin, 0);
            assertFalse(logDirs(admin, 0).get(dirs.get(0)).isCordoned());
            assertFalse(logDirs(admin, 0).get(dirs.get(1)).isCordoned());

            setCordoned(admin, 0, dirs.get(1));
            waitForCordoned(admin, 0, Set.of(dirs.get(1)));
            assertFalse(logDirs(admin, 0).get(dirs.get(0)).isCordoned());
            assertTrue(logDirs(admin, 0).get(dirs.get(1)).isCordoned());
            assertTrue(logDirs(admin, 0).get(dirs.get(1)).toString().contains("isCordoned=true"));
        }
    }

    private static Gauge<?> yammerGauge(String namePrefix, String suffix) {
        Collection<Gauge<?>> matches = new ArrayList<>();
        for (Map.Entry<MetricName, com.yammer.metrics.core.Metric> entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            String mbeanName = entry.getKey().getMBeanName();
            if (mbeanName.startsWith(namePrefix) && mbeanName.endsWith(suffix)) {
                matches.add((Gauge<?>) entry.getValue());
            }
        }
        assertEquals(1, matches.size(), "Expected exactly one metric " + namePrefix + "..." + suffix);
        return matches.iterator().next();
    }

    private static int cordonedCount() {
        return ((Number) yammerGauge("kafka.log:type=LogManager,name=CordonedLogDirectoryCount", "").value()).intValue();
    }

    private static int cordonedGauge(String dir) {
        return ((Number) yammerGauge("kafka.log:type=LogManager,name=LogDirectoryCordoned,", "logDirectory=" + Sanitizer.jmxSanitize(dir)).value()).intValue();
    }
}
