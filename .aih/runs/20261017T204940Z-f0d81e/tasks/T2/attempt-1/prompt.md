You are implementing one task of an approved plan inside an isolated worktree. You can read and
edit files. The only command you can run is `aih check`, as often as you need; every other shell
command is rejected. `aih check <test files...>` runs those tests; `aih check --build` only
compiles (when the project has a build step); `--race` runs them under the race detector and
`--run <pattern>` restricts them to matching tests, when the project supports it; and
`aih check --probe <file>` runs a scratch test file you wrote and deletes it afterwards, so you can
try a hypothesis without it becoming part of the change (name such files `aih_probe*`; they are
the only files you may create outside the task's list). When you try to finish, the harness runs the
task's tests again and gives you the result. Edits outside the files listed for this task are
rejected by a hook; do not work around it.

## Original request

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

## Plan summary



## Your task: T2 (T1)

Implement KIP-1066 as one PR. Order: config definition + dynamic registration + MV gating; protocol/record JSON changes and LogDirDescription; core LogManager cordon state, metrics, dir choice; broker heartbeat; controller storage and placement/validation; ReplicaManager/KafkaApis; tool output; then tests, fixing existing tests broken by schema/constructor changes.

Acceptance criteria this task must satisfy:
- ACC12 [behavior]: cordoned.log.dirs can be set dynamically per broker; values not in log.dirs (and not '*') are rejected; removing the entry uncordons.
- ACC13 [behavior]: Below 4.3-IV0, dynamic setting is rejected with a configuration error and static setting has no effect.
- ACC14 [behavior]: Heartbeats carry cordoned dirs after catch-up; controller reflects dynamic changes without restart.
- ACC15 [behavior]: Auto-placed creation avoids fully cordoned brokers and fails with INVALID_REPLICATION_FACTOR when too few remain.
- ACC16 [behavior]: Explicit assignment onto a fully cordoned broker returns INVALID_REPLICA_ASSIGNMENT.
- ACC17 [behavior]: Reassignment onto a fully cordoned broker returns INELIGIBLE_REPLICA.
- ACC18 [behavior]: AlterReplicaLogDirs to a cordoned directory returns INVALID_REPLICA_ASSIGNMENT.
- ACC19 [behavior]: Broker never creates a new replica in a cordoned directory even if the controller suggested it.
- ACC20 [behavior]: DescribeLogDirs returns per-directory cordoned state via LogDirDescription.isCordoned() and the tool prints it.
- ACC21 [behavior]: LogDirectoryCordoned and CordonedLogDirectoryCount metrics are registered and accurate.
- ACC22 [behavior]: Integration test cordons/uncordons via Admin on an embedded cluster and checks placement and errors.

Files you may edit (anything else is blocked):
- server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java (production, modify)
- server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java (production, modify)
- server/src/main/java/org/apache/kafka/server/config/DynamicBrokerConfig.java (production, modify)
- server/src/main/java/org/apache/kafka/server/config/DynamicConfig.java (production, modify)
- server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java (production, modify)
- core/src/main/scala/kafka/server/KafkaConfig.scala (production, modify)
- core/src/main/scala/kafka/server/DynamicBrokerConfig.scala (production, modify)
- core/src/main/scala/kafka/server/ControllerConfigurationValidator.scala (production, modify)
- core/src/main/scala/kafka/server/ConfigAdminManager.scala (production, modify)
- core/src/main/scala/kafka/server/BrokerServer.scala (production, modify)
- core/src/main/scala/kafka/server/KafkaApis.scala (production, modify)
- core/src/main/scala/kafka/server/ReplicaManager.scala (production, modify)
- core/src/main/scala/kafka/log/LogManager.scala (production, modify)
- clients/src/main/resources/common/message/BrokerHeartbeatRequest.json (production, modify)
- clients/src/main/resources/common/message/DescribeLogDirsResponse.json (production, modify)
- clients/src/main/java/org/apache/kafka/clients/admin/LogDirDescription.java (production, modify)
- clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java (production, modify)
- clients/src/main/java/org/apache/kafka/common/requests/DescribeLogDirsResponse.java (production, modify)
- tools/src/main/java/org/apache/kafka/tools/LogDirsCommand.java (production, modify)
- metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json (production, modify)
- metadata/src/main/java/org/apache/kafka/metadata/BrokerRegistration.java (production, modify)
- metadata/src/main/java/org/apache/kafka/image/ClusterDelta.java (production, modify)
- metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java (production, modify)
- metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java (production, modify)
- metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java (production, modify)
- metadata/src/main/java/org/apache/kafka/controller/ConfigurationValidator.java (production, modify)
- metadata/src/main/java/org/apache/kafka/controller/QuorumController.java (production, modify)
- metadata/src/main/java/org/apache/kafka/metadata/placement/ClusterDescriber.java (production, modify)
- server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java (test, create)

Tests that must pass when you finish: server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java

## Rules

- Write or update the tests listed above so that they fail without your production change and
  pass with it. The harness verifies this against the base commit: a test that also passes
  without the change does not count.
- Every branch you add (a new `if`/`else`, an `except`, an early return, a new error status)
  must be exercised by a test in this task's test files before you finish. An independent
  reviewer blocks the task for each new branch without a test; write them first.
- Do not modify existing tests other than the ones listed for this task.
- Do not add features, docs or refactors the task does not ask for.
- If the task cannot be done without touching a file that is not listed, or a criterion is
  impossible as written, do not improvise: stop and return `replan_request` with the reason and the
  files you need. That re-plans instead of failing you.

## Final answer

Return JSON matching the schema you were given: a short `summary`, one `criteria_notes` entry per
criterion with the test that proves it, and `replan_request` (`null` unless you need a re-plan).
