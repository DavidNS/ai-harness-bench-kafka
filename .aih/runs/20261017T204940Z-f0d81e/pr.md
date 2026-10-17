## Implement KIP-1066, "Mechanism to cordon log directories", in the Kafka broker and controller.

### Request

Implement KIP-1066, "Mechanism to cordon log directories", in the Kafka broker and controller.

Operators need to stop new partitions from landing on a log directory (or on a whole broker)
without taking it out of service: a cordoned directory keeps serving its existing partitions, but
the controller must not place any new partition on it, and a broker whose directories are all
cordoned must not receive new partitions at all. This is the building block for decommissioning
brokers and for rebalancing uneven JBOD disks.

Behaviour to deliver:

- A new broker configuration `cordoned.log.dirs` (comma-separated list, default empty, low
  importance), settable in the properties file and dynamically per broker through the Admin
  client and `kafka-configs.sh`. Entries must be directories listed in `log.dirs`/`log.dir`;
  `*` cordons all of them. Removing an entry uncordons the directory.
- The feature is gated by the metadata version: it needs 4.3-IV0 (already defined, not yet
  production). Below that, setting the config dynamically is rejected with a configuration
  error, and a static setting has no effect.
- Brokers report their cordoned directories to the controller in their heartbeats once they have
  caught up with metadata, so dynamic changes take effect without a restart.
- Placement: topic and partition creation without explicit assignments only uses brokers that
  have at least one uncordoned directory; if that leaves too few brokers for the replication
  factor the request fails with INVALID_REPLICATION_FACTOR. Explicit assignments onto a broker
  with no uncordoned directory fail with INVALID_REPLICA_ASSIGNMENT. Partition reassignments onto
  such a broker fail with INELIGIBLE_REPLICA. Moving a replica onto a cordoned directory with
  AlterReplicaLogDirs fails with INVALID_REPLICA_ASSIGNMENT. The broker never picks a cordoned directory
  for a new replica, whatever directory the controller suggested.
- `DescribeLogDirs` shows whether each directory is cordoned: the Admin client's `LogDirDescription`
  gets an `isCordoned()` accessor, and the log dirs tool prints it.
- Metrics: `kafka.log:type=LogManager,name=LogDirectoryCordoned,logDirectory=<path>` (1 or 0)
  and `kafka.log:type=LogManager,name=CordonedLogDirectoryCount`.

Add an integration test against a real embedded cluster that cordons and uncordons directories
through the Admin client and checks where new partitions land and which errors are returned, plus
unit tests for the pieces you change. The existing test suites must stay green. Do not change
unrelated behaviour.

### Intents

- `INTENT1` Add broker config `cordoned.log.dirs` (list, default empty, low importance), settable statically and dynamically per broker via Admin/kafka-configs.sh, validated so entries are in log.dirs/log.dir or `*` (all), with removal uncordoning. (change)
- `INTENT2` Gate the feature on metadata version 4.3-IV0: below it, dynamic setting is rejected with a configuration error and a static setting has no effect. (change)
- `INTENT3` Brokers report their cordoned directories to the controller in broker heartbeats once caught up with metadata, so dynamic changes apply without restart. (change)
- `INTENT4` Topic and partition creation without explicit assignments uses only brokers with at least one uncordoned directory, failing with INVALID_REPLICATION_FACTOR if too few remain. (change)
- `INTENT5` Explicit assignments onto a broker with no uncordoned directory fail with INVALID_REPLICA_ASSIGNMENT. (change)
- `INTENT6` Partition reassignments onto a broker with no uncordoned directory fail with INELIGIBLE_REPLICA. (change)
- `INTENT7` AlterReplicaLogDirs moving a replica onto a cordoned directory fails with INVALID_REPLICA_ASSIGNMENT. (change)
- `INTENT8` The broker never selects a cordoned directory for a new replica, regardless of the directory the controller suggested. (change)
- `INTENT9` DescribeLogDirs reports cordoned state per directory: LogDirDescription gets an isCordoned() accessor and the log dirs tool prints it. (change)
- `INTENT10` Add metrics kafka.log:type=LogManager,name=LogDirectoryCordoned,logDirectory=<path> (1/0) and kafka.log:type=LogManager,name=CordonedLogDirectoryCount. (change)
- `INTENT11` Add an integration test on an embedded cluster that cordons/uncordons via Admin and checks partition placement and returned errors, plus unit tests for changed components. (change)

### Criteria, tests and results

| Criterion | Kind | Task | Test(s) | Result |
|---|---|---|---|---|
| ACC23 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testInvalidCordonedLogDirsAreRejected` (covered, probe verified / base does_not_build) | committed |
| ACC24 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testCordoningRequiresMetadataVersion` (covered, probe verified / base does_not_build) | committed |
| ACC25 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testBrokerWithAllLogDirectoriesCordonedReceivesNoNewPartitions` (covered, probe verified / base does_not_build) | committed |
| ACC26 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testBrokerWithAllLogDirectoriesCordonedReceivesNoNewPartitions` (covered, probe verified / base does_not_build) | committed |
| ACC27 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testBrokerWithAllLogDirectoriesCordonedReceivesNoNewPartitions` (covered, probe verified / base does_not_build) | committed |
| ACC28 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testBrokerWithAllLogDirectoriesCordonedReceivesNoNewPartitions` (covered, probe verified / base does_not_build) | committed |
| ACC29 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testAlterReplicaLogDirsToCordonedDirectory` (covered, probe verified / base does_not_build) | committed |
| ACC30 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testBrokerDoesNotCreateReplicaInCordonedDirectory` (covered, probe verified / base does_not_build) | committed |
| ACC31 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testDescribeLogDirsReportsCordonedState` (covered, probe verified / base does_not_build) | committed |
| ACC32 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testCordonedLogDirectoryMetrics` (covered, probe verified / base does_not_build) | committed |
| ACC33 | behavior | T3 | `server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java::testCordonedLogDirectoriesAreNotUsedForNewPartitions` (covered, probe verified / base does_not_build) | committed |

### Tasks

- `T3` Implement KIP-1066 as one PR. Order: config definition + dynamic registration + MV gating; protocol/record JSON changes and LogDirDescription; core LogManager cordon state, metrics, dir choice; broker heartbeat; controller storage and placement/validation; ReplicaManager/KafkaApis; tool output; then tests, fixing existing metadata/tools tests broken by schema/constructor changes and adding unit tests there. (1 attempt(s), commit `2ab8ef46f0`)
  - reviewer note [pass 1] `core/src/main/scala/kafka/log/LogManager.scala:1088`: When every live directory is cordoned, nextLogDirs falls back to a cordoned directory, so ACC30 ('never picks a cordoned directory') only holds while an uncordoned live directory exists. The integration test asserts the fallback, so it is deliberate, but it deserves a comment or a KIP check.
  - reviewer note [pass 2] `server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java:545`: No test checks that heartbeats omit CordonedLogDirs before the initial catch-up and include them afterwards. A BrokerLifecycleManager unit test, a KafkaConfig validation unit test for cordoned.log.dirs, and a LogManager unit test for nextLogDirs are also missing; only the integration tests exercise them.
  - reviewer note [pass 4] `core/src/main/scala/kafka/server/DynamicBrokerConfig.scala:608`: DynamicCordonedLogDirs has an empty validateReconfiguration, and BrokerRegistration.sorted() duplicates the sorting done in the constructor and in handleCordonedDirectories. Both could be trimmed.

### Gate failures along the way

- change.implemented (T1, attempt 1): change.replan_requested
- review.approved (T2, attempt 1): review.changes_requested
- change.implemented (T2, attempt 1): change.replan_requested
- change.implemented (T2, attempt 2): change.replan_requested
- plan (plan, attempt 1): plan.file_role_mismatch, plan.file_role_mismatch, plan.file_role_mismatch

### Cost (client-side estimate)

- sessions: 14; turns: 504; USD: 11.59; attempts: 4

_Generated by aih run `20261017T204940Z-f0d81e`._
