package org.apache.kafka.server;

import kafka.log.LogManager;

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
import org.apache.kafka.test.TestUtils;

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
public class aih_probeCordonTest {
    private final ClusterInstance cluster;

    aih_probeCordonTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    private static final String CFG = "cordoned.log.dirs";

    private static void set(Admin admin, int id, String v) throws Exception {
        ConfigEntry e = new ConfigEntry(CFG, v);
        admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.BROKER, "" + id),
            List.of(new AlterConfigOp(e, v == null ? AlterConfigOp.OpType.DELETE : AlterConfigOp.OpType.SET)))).all().get();
    }

    private static Map<String, LogDirDescription> dirs(Admin admin, int id) throws Exception {
        return admin.describeLogDirs(List.of(id)).allDescriptions().get().get(id);
    }

    private static Set<String> cordoned(Admin admin, int id) throws Exception {
        return dirs(admin, id).entrySet().stream().filter(e -> e.getValue().isCordoned())
            .map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    private static void waitCordoned(Admin admin, int id, Set<String> expected) throws Exception {
        TestUtils.waitForCondition(() -> expected.equals(cordoned(admin, id)), 60_000, "cordoned state");
    }

    private static Throwable err(Admin admin, NewTopic t) throws Exception {
        try {
            admin.createTopics(List.of(t)).all().get();
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private void fullyCordonBroker2(Admin admin) throws Exception {
        set(admin, 2, "*");
        waitCordoned(admin, 2, dirs(admin, 2).keySet());
        int[] n = {0};
        TestUtils.waitForCondition(() -> err(admin, new NewTopic("w" + n[0]++, 1, (short) 3))
            instanceof InvalidReplicationFactorException, 60_000, "controller excludes broker");
    }

    @ClusterTest
    public void testAcc23() throws Exception {
        try (Admin admin = cluster.admin()) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> set(admin, 0, "/no/such/dir"));
            assertTrue(e.getCause() != null);
            assertEquals(Set.of(), cordoned(admin, 0));
            String d = dirs(admin, 0).keySet().iterator().next();
            set(admin, 0, d);
            waitCordoned(admin, 0, Set.of(d));
            set(admin, 0, null);
            waitCordoned(admin, 0, Set.of());
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_2_IV1, serverProperties = {
        @ClusterConfigProperty(key = CFG, value = "*")})
    public void testAcc24() throws Exception {
        try (Admin admin = cluster.admin()) {
            String d = dirs(admin, 0).keySet().iterator().next();
            ExecutionException e = assertThrows(ExecutionException.class, () -> set(admin, 0, d));
            assertInstanceOf(InvalidConfigurationException.class, e.getCause());
            assertEquals(Set.of(), cordoned(admin, 1));
            assertEquals(null, err(admin, new NewTopic("t", 1, (short) 3)));
        }
    }

    @ClusterTest
    public void testAcc25to28() throws Exception {
        try (Admin admin = cluster.admin()) {
            fullyCordonBroker2(admin);
            assertEquals(null, err(admin, new NewTopic("ok", 4, (short) 2)));
            assertInstanceOf(InvalidReplicaAssignmentException.class,
                err(admin, new NewTopic("manual", Map.of(0, List.of(0, 2)))));
            admin.createTopics(List.of(new NewTopic("r", Map.of(0, List.of(0))))).all().get();
            ExecutionException e = assertThrows(ExecutionException.class, () ->
                admin.alterPartitionReassignments(Map.of(new TopicPartition("r", 0),
                    Optional.of(new NewPartitionReassignment(List.of(2))))).all().get());
            assertInstanceOf(IneligibleReplicaException.class, e.getCause());
            set(admin, 2, null);
            waitCordoned(admin, 2, Set.of());
            TestUtils.waitForCondition(() -> err(admin, new NewTopic("back" + System.nanoTime(), 1, (short) 3)) == null,
                60_000, "uncordoned");
        }
    }

    @ClusterTest
    public void testAcc29() throws Exception {
        try (Admin admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic("a", Map.of(0, List.of(0))))).all().get();
            TopicPartition tp = new TopicPartition("a", 0);
            TestUtils.waitForCondition(() -> dirs(admin, 0).values().stream().anyMatch(x -> x.replicaInfos().containsKey(tp)),
                60_000, "created");
            String cur = dirs(admin, 0).entrySet().stream().filter(x -> x.getValue().replicaInfos().containsKey(tp))
                .map(Map.Entry::getKey).findFirst().get();
            String other = dirs(admin, 0).keySet().stream().filter(x -> !x.equals(cur)).findFirst().get();
            set(admin, 0, other);
            waitCordoned(admin, 0, Set.of(other));
            ExecutionException e = assertThrows(ExecutionException.class, () ->
                admin.alterReplicaLogDirs(Map.of(new TopicPartitionReplica("a", 0, 0), other)).all().get());
            assertInstanceOf(InvalidReplicaAssignmentException.class, e.getCause());
        }
    }

    @ClusterTest
    public void testAcc30() throws Exception {
        try (Admin admin = cluster.admin()) {
            LogManager lm = cluster.brokers().get(0).logManager();
            List<String> ds = dirs(admin, 0).keySet().stream().sorted().toList();
            set(admin, 0, ds.get(0));
            waitCordoned(admin, 0, Set.of(ds.get(0)));
            TestUtils.waitForCondition(() -> lm.isCordoned(ds.get(0)), 60_000, "lm");
            Uuid id = lm.directoryId(ds.get(0)).get();
            for (int i = 0; i < 4; i++) {
                assertEquals(ds.get(1), lm.getOrCreateLog(new TopicPartition("s", i), true, false,
                    Optional.of(Uuid.randomUuid()), Option.apply(id)).parentDir());
            }
        }
    }

    @ClusterTest(brokers = 1)
    public void testAcc31and32() throws Exception {
        try (Admin admin = cluster.admin()) {
            List<String> ds = dirs(admin, 0).keySet().stream().sorted().toList();
            set(admin, 0, ds.get(1));
            waitCordoned(admin, 0, Set.of(ds.get(1)));
            assertFalse(dirs(admin, 0).get(ds.get(0)).isCordoned());
            TestUtils.waitForCondition(() -> {
                long cnt = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                    .filter(e -> e.getKey().getMBeanName().equals("kafka.log:type=LogManager,name=CordonedLogDirectoryCount"))
                    .map(e -> ((Number) ((com.yammer.metrics.core.Gauge<?>) e.getValue()).value()).intValue())
                    .filter(v -> v == 1).count();
                long perDir = KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                    .filter(n -> n.getMBeanName().startsWith("kafka.log:type=LogManager,name=LogDirectoryCordoned,")).count();
                return cnt == 1 && perDir == 2;
            }, 60_000, "metrics");
        }
    }
}
