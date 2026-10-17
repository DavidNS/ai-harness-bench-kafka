You are reviewing one task of an approved plan, implemented by a different session whose
reasoning you have not seen. The change is already applied in this worktree. You can read the
repository, and you can run tests through `aih check`, the only command available:
`aih check <test files...>` runs those tests, `--race` under the race detector and
`--run <pattern>` only matching tests (when the project supports them), and
`aih check --probe <file>` runs a scratch test you wrote as a NEW file named `aih_probe*` and
deletes it afterwards. You cannot edit the change: every other file is locked for you. The harness
has already verified mechanically that the diff stays within the task's files, that the changed
tests fail without the production change and pass with it, and that the whole suite is green. Do
not re-check those. Your job is judgement, backed by tests you run.

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



## Task T3 (T1)

Implement KIP-1066 as one PR. Order: config definition + dynamic registration + MV gating; protocol/record JSON changes and LogDirDescription; core LogManager cordon state, metrics, dir choice; broker heartbeat; controller storage and placement/validation; ReplicaManager/KafkaApis; tool output; then tests, fixing existing metadata/tools tests broken by schema/constructor changes and adding unit tests there.

Acceptance criteria:
- ACC23 [behavior]: cordoned.log.dirs can be set dynamically per broker; values not in log.dirs (and not '*') are rejected; removing the entry uncordons.
- ACC24 [behavior]: Below 4.3-IV0, dynamic setting is rejected with a configuration error and static setting has no effect.
- ACC25 [behavior]: Heartbeats carry cordoned dirs after catch-up; controller reflects dynamic changes without restart.
- ACC26 [behavior]: Auto-placed creation avoids fully cordoned brokers and fails with INVALID_REPLICATION_FACTOR when too few remain.
- ACC27 [behavior]: Explicit assignment onto a fully cordoned broker returns INVALID_REPLICA_ASSIGNMENT.
- ACC28 [behavior]: Reassignment onto a fully cordoned broker returns INELIGIBLE_REPLICA.
- ACC29 [behavior]: AlterReplicaLogDirs to a cordoned directory returns INVALID_REPLICA_ASSIGNMENT.
- ACC30 [behavior]: Broker never creates a new replica in a cordoned directory even if the controller suggested it.
- ACC31 [behavior]: DescribeLogDirs returns per-directory cordoned state via LogDirDescription.isCordoned().
- ACC32 [behavior]: LogDirectoryCordoned and CordonedLogDirectoryCount metrics are registered and accurate.
- ACC33 [behavior]: Integration test cordons/uncordons via Admin on an embedded cluster and checks placement and errors.

Files declared for the task:
- server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java (production)
- server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java (production)
- server/src/main/java/org/apache/kafka/server/config/DynamicBrokerConfig.java (production)
- server/src/main/java/org/apache/kafka/server/config/DynamicConfig.java (production)
- server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java (production)
- core/src/main/scala/kafka/server/KafkaConfig.scala (production)
- core/src/main/scala/kafka/server/DynamicBrokerConfig.scala (production)
- core/src/main/scala/kafka/server/ControllerConfigurationValidator.scala (production)
- core/src/main/scala/kafka/server/ConfigAdminManager.scala (production)
- core/src/main/scala/kafka/server/BrokerServer.scala (production)
- core/src/main/scala/kafka/server/KafkaApis.scala (production)
- core/src/main/scala/kafka/server/ReplicaManager.scala (production)
- core/src/main/scala/kafka/log/LogManager.scala (production)
- clients/src/main/resources/common/message/BrokerHeartbeatRequest.json (production)
- clients/src/main/resources/common/message/DescribeLogDirsResponse.json (production)
- clients/src/main/java/org/apache/kafka/clients/admin/LogDirDescription.java (production)
- clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java (production)
- clients/src/main/java/org/apache/kafka/common/requests/DescribeLogDirsResponse.java (production)
- tools/src/main/java/org/apache/kafka/tools/LogDirsCommand.java (production)
- metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json (production)
- metadata/src/main/java/org/apache/kafka/metadata/BrokerRegistration.java (production)
- metadata/src/main/java/org/apache/kafka/image/ClusterDelta.java (production)
- metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java (production)
- metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java (production)
- metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java (production)
- metadata/src/main/java/org/apache/kafka/controller/ConfigurationValidator.java (production)
- metadata/src/main/java/org/apache/kafka/controller/QuorumController.java (production)
- metadata/src/main/java/org/apache/kafka/metadata/placement/ClusterDescriber.java (production)
- server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java (test)
- tools/src/test/java/org/apache/kafka/tools/LogDirsCommandTest.java (production)
- metadata/src/test/java/org/apache/kafka/controller/ReplicationControlManagerTest.java (production)
- metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java (production)

Test files of this task that the change modified (existing tests, declared by the plan):
- (none)

Test results: selected tests green; red proven; suite green

## The diff

```diff
diff --git a/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java b/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java
index a80c860de5..85f1e87459 100644
--- a/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java
+++ b/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java
@@ -3054,7 +3054,8 @@ public class KafkaAdminClient extends AdminClient {
                 Errors.forCode(logDirResult.errorCode()).exception(),
                 replicaInfoMap,
                 logDirResult.totalBytes(),
-                logDirResult.usableBytes()));
+                logDirResult.usableBytes(),
+                logDirResult.isCordoned()));
         }
         return result;
     }
diff --git a/clients/src/main/java/org/apache/kafka/clients/admin/LogDirDescription.java b/clients/src/main/java/org/apache/kafka/clients/admin/LogDirDescription.java
index 340e88db16..fa4699d790 100644
--- a/clients/src/main/java/org/apache/kafka/clients/admin/LogDirDescription.java
+++ b/clients/src/main/java/org/apache/kafka/clients/admin/LogDirDescription.java
@@ -33,12 +33,18 @@ public class LogDirDescription {
     private final ApiException error;
     private final OptionalLong totalBytes;
     private final OptionalLong usableBytes;
+    private final boolean isCordoned;
 
     public LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo> replicaInfos) {
         this(error, replicaInfos, UNKNOWN_VOLUME_BYTES, UNKNOWN_VOLUME_BYTES);
     }
 
     public LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo> replicaInfos, long totalBytes, long usableBytes) {
+        this(error, replicaInfos, totalBytes, usableBytes, false);
+    }
+
+    public LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo> replicaInfos, long totalBytes, long usableBytes, boolean isCordoned) {
+        this.isCordoned = isCordoned;
         this.error = error;
         this.replicaInfos = replicaInfos;
         this.totalBytes = (totalBytes == UNKNOWN_VOLUME_BYTES) ? OptionalLong.empty() : OptionalLong.of(totalBytes);
@@ -82,6 +88,13 @@ public class LogDirDescription {
         return usableBytes;
     }
 
+    /**
+     * Whether this log directory is cordoned, i.e. the cluster does not place new partitions on it.
+     */
+    public boolean isCordoned() {
+        return isCordoned;
+    }
+
     @Override
     public String toString() {
         return "LogDirDescription(" +
@@ -89,6 +102,7 @@ public class LogDirDescription {
                 ", error=" + error +
                 ", totalBytes=" + totalBytes +
                 ", usableBytes=" + usableBytes +
+                ", isCordoned=" + isCordoned +
                 ')';
     }
 }
diff --git a/clients/src/main/resources/common/message/BrokerHeartbeatRequest.json b/clients/src/main/resources/common/message/BrokerHeartbeatRequest.json
index 8f574b41fc..df612ee0ff 100644
--- a/clients/src/main/resources/common/message/BrokerHeartbeatRequest.json
+++ b/clients/src/main/resources/common/message/BrokerHeartbeatRequest.json
@@ -32,6 +32,9 @@
     { "name": "WantShutDown", "type": "bool", "versions": "0+",
       "about": "True if the broker wants to be shut down, false otherwise." },
     { "name": "OfflineLogDirs", "type":  "[]uuid", "versions": "1+", "taggedVersions": "1+", "tag": 0,
-      "about": "Log directories that failed and went offline." }
+      "about": "Log directories that failed and went offline." },
+    { "name": "CordonedLogDirs", "type": "[]uuid", "versions": "1+", "taggedVersions": "1+", "tag": 1,
+      "nullableVersions": "1+", "default": "null",
+      "about": "Log directories that are cordoned, or null if the broker did not report its cordoned log directories." }
   ]
 }
diff --git a/clients/src/main/resources/common/message/DescribeLogDirsResponse.json b/clients/src/main/resources/common/message/DescribeLogDirsResponse.json
index 725d1ad337..fbbc96632c 100644
--- a/clients/src/main/resources/common/message/DescribeLogDirsResponse.json
+++ b/clients/src/main/resources/common/message/DescribeLogDirsResponse.json
@@ -55,6 +55,9 @@
       },
       { "name": "UsableBytes", "type": "int64", "versions": "4+", "ignorable": true, "default": "-1",
         "about": "The usable size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage."
+      },
+      { "name": "IsCordoned", "type": "bool", "versions": "4+", "taggedVersions": "4+", "tag": 0, "ignorable": true, "default": "false",
+        "about": "True if this log directory is cordoned, so that no new partitions are placed on it."
       }
     ]}
   ]
diff --git a/core/src/main/scala/kafka/log/LogManager.scala b/core/src/main/scala/kafka/log/LogManager.scala
index efdab2a0f3..99da2e1989 100755
--- a/core/src/main/scala/kafka/log/LogManager.scala
+++ b/core/src/main/scala/kafka/log/LogManager.scala
@@ -38,6 +38,7 @@ import org.apache.kafka.metadata.ConfigRepository
 import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, PropertiesUtils}
 
 import java.util.{Collections, Optional, OptionalLong, Properties}
+import org.apache.kafka.server.config.ServerLogConfigs
 import org.apache.kafka.server.metrics.KafkaMetricsGroup
 import org.apache.kafka.server.util.{FileLock, Scheduler}
 import org.apache.kafka.storage.internals.log.{CleanerConfig, LogCleaner, LogConfig, LogDirFailureChannel, LogManager => JLogManager, LogOffsetsListener, ProducerStateManagerConfig, RemoteIndexCache, UnifiedLog}
@@ -153,12 +154,57 @@ class LogManager(logDirs: Seq[File],
   @volatile private var _cleaner: LogCleaner = _
   private[kafka] def cleaner: LogCleaner = _cleaner
 
+  // The value of cordoned.log.dirs: absolute paths of the cordoned directories, or "*" for all of them.
+  @volatile private var configuredCordonedLogDirs: Predef.Set[String] = Predef.Set.empty
+  // Cordoning is only in effect when this returns true (i.e. the metadata version supports it).
+  @volatile private var cordoningSupported: () => Boolean = () => true
+
   metricsGroup.newGauge("OfflineLogDirectoryCount", () => offlineLogDirs.size)
+  metricsGroup.newGauge("CordonedLogDirectoryCount", () => cordonedLogDirs.size)
 
   for (dir <- logDirs) {
     metricsGroup.newGauge("LogDirectoryOffline",
       () => if (_liveLogDirs.contains(dir)) 0 else 1,
       Map("logDirectory" -> dir.getAbsolutePath).asJava)
+    metricsGroup.newGauge("LogDirectoryCordoned",
+      () => if (cordonedLogDirs.contains(dir.getAbsolutePath)) 1 else 0,
+      Map("logDirectory" -> dir.getAbsolutePath).asJava)
+  }
+
+  /**
+   * Set the value of cordoned.log.dirs. Entries must be paths of directories managed by this LogManager, or "*".
+   */
+  def updateCordonedLogDirs(configured: util.Collection[String]): Unit = {
+    configuredCordonedLogDirs = configured.asScala.map { dir =>
+      if (dir == ServerLogConfigs.CORDONED_LOG_DIRS_ALL) dir else new File(dir).getAbsolutePath
+    }.toSet
+    info(s"Cordoned log directories are now: ${cordonedLogDirs.mkString(",")}")
+  }
+
+  def setCordoningSupported(supported: () => Boolean): Unit = {
+    cordoningSupported = supported
+  }
+
+  /**
+   * The absolute paths of the cordoned log directories.
+   */
+  def cordonedLogDirs: Predef.Set[String] = {
+    if (!cordoningSupported())
+      Predef.Set.empty
+    else if (configuredCordonedLogDirs.contains(ServerLogConfigs.CORDONED_LOG_DIRS_ALL))
+      logDirs.map(_.getAbsolutePath).toSet
+    else
+      logDirs.map(_.getAbsolutePath).filter(configuredCordonedLogDirs.contains).toSet
+  }
+
+  def isCordoned(logDir: String): Boolean = cordonedLogDirs.contains(new File(logDir).getAbsolutePath)
+
+  /**
+   * The IDs of the cordoned log directories that have one.
+   */
+  def cordonedLogDirIds: Predef.Set[Uuid] = {
+    val cordoned = cordonedLogDirs
+    directoryIds.collect { case (path, id) if cordoned.contains(path) => id }.toSet
   }
 
   /**
@@ -647,8 +693,10 @@ class LogManager(logDirs: Seq[File],
     info("Shutting down.")
 
     metricsGroup.removeMetric("OfflineLogDirectoryCount")
+    metricsGroup.removeMetric("CordonedLogDirectoryCount")
     for (dir <- logDirs) {
       metricsGroup.removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath).asJava)
+      metricsGroup.removeMetric("LogDirectoryCordoned", Map("logDirectory" -> dir.getAbsolutePath).asJava)
     }
 
     val threadPools = ArrayBuffer.empty[ExecutorService]
@@ -1037,10 +1085,13 @@ class LogManager(logDirs: Seq[File],
               throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
           }
 
-          if (preferredLogDir != null)
+          if (preferredLogDir != null && (isFuture || !isCordoned(preferredLogDir)))
             List(new File(preferredLogDir))
-          else
+          else {
+            if (preferredLogDir != null)
+              warn(s"Not creating the log for $topicPartition in the cordoned log directory $preferredLogDir")
             nextLogDirs()
+          }
         }
 
         val logDirName = {
@@ -1367,13 +1418,17 @@ class LogManager(logDirs: Seq[File],
    * data directories by fewest partitions.
    */
   private def nextLogDirs(): List[File] = {
-    if (_liveLogDirs.size == 1) {
-      List(_liveLogDirs.peek())
+    // Cordoned directories are avoided, unless every live directory is cordoned.
+    val cordoned = cordonedLogDirs
+    val uncordonedLiveDirs = _liveLogDirs.asScala.filterNot(dir => cordoned.contains(dir.getAbsolutePath))
+    val candidateDirs = if (uncordonedLiveDirs.isEmpty) _liveLogDirs.asScala else uncordonedLiveDirs
+    if (candidateDirs.size == 1) {
+      List(candidateDirs.head)
     } else {
       // count the number of logs in each parent directory (including 0 for empty directories
       val logCounts = allLogs.groupBy(_.parentDir).map { case (parent, logs) => parent -> logs.size }
-      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
-      val dirCounts = (zeros ++ logCounts).toBuffer
+      val zeros = candidateDirs.map(dir => (dir.getPath, 0)).toMap
+      val dirCounts = (zeros ++ logCounts.filter { case (parent, _) => zeros.contains(parent) }).toBuffer
 
       // choose the directory with the least logs in it
       dirCounts.sortBy(_._2).map {
@@ -1545,7 +1600,7 @@ object LogManager {
     val cleanerConfig = new CleanerConfig(config)
     val transactionLogConfig = new TransactionLogConfig(config)
 
-    new LogManager(logDirs = config.logDirs.asScala.map(new File(_).getAbsoluteFile),
+    val logManager = new LogManager(logDirs = config.logDirs.asScala.map(new File(_).getAbsoluteFile),
       initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
       configRepository = configRepository,
       initialDefaultConfig = defaultLogConfig,
@@ -1564,5 +1619,7 @@ object LogManager {
       time = time,
       remoteStorageSystemEnable = config.remoteLogManagerConfig.isRemoteStorageSystemEnabled,
       initialTaskDelayMs = config.logInitialTaskDelayMs)
+    logManager.updateCordonedLogDirs(config.cordonedLogDirs)
+    logManager
   }
 }
diff --git a/core/src/main/scala/kafka/server/BrokerServer.scala b/core/src/main/scala/kafka/server/BrokerServer.scala
index 977eca37a8..d88ed36311 100644
--- a/core/src/main/scala/kafka/server/BrokerServer.scala
+++ b/core/src/main/scala/kafka/server/BrokerServer.scala
@@ -45,7 +45,7 @@ import org.apache.kafka.metadata.publisher.{AclPublisher, DelegationTokenPublish
 import org.apache.kafka.security.{CredentialProvider, DelegationTokenManager}
 import org.apache.kafka.server.FetchSession.FetchSessionCache
 import org.apache.kafka.server.authorizer.Authorizer
-import org.apache.kafka.server.common.{ApiMessageAndVersion, DirectoryEventHandler, NodeToControllerChannelManager, TopicIdPartition}
+import org.apache.kafka.server.common.{ApiMessageAndVersion, DirectoryEventHandler, MetadataVersion, NodeToControllerChannelManager, TopicIdPartition}
 import org.apache.kafka.server.config.{ConfigType, DelegationTokenManagerConfigs}
 import org.apache.kafka.server.log.remote.metadata.storage.BrokerReadyCallback
 import org.apache.kafka.server.log.remote.storage.{RemoteLogManager, RemoteLogManagerConfig}
@@ -217,12 +217,15 @@ class BrokerServer(
         time,
         brokerTopicStats,
         logDirFailureChannel)
+      // Cordoning log directories requires metadata version 4.3-IV0 or higher.
+      logManager.setCordoningSupported(() => metadataCache.metadataVersion().isAtLeast(MetadataVersion.IBP_4_3_IV0))
 
       lifecycleManager = new BrokerLifecycleManager(config,
         time,
         s"broker-${config.nodeId}-",
         logManager.directoryIdsSet.asJava,
         () => new Thread(() => shutdown(), "kafka-shutdown-thread").start())
+      lifecycleManager.setCordonedLogDirsSupplier(() => logManager.cordonedLogDirIds.asJava)
 
       // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
       // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
diff --git a/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala b/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala
index a467f18ace..f3b4c59bd0 100755
--- a/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala
+++ b/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala
@@ -196,6 +196,7 @@ class DynamicBrokerConfig(private val kafkaConfig: KafkaConfig) extends Logging
 
     addBrokerReconfigurable(new BrokerDynamicThreadPool(kafkaServer))
     addBrokerReconfigurable(new DynamicLogConfig(kafkaServer.logManager))
+    addBrokerReconfigurable(new DynamicCordonedLogDirs(kafkaServer.logManager))
     addBrokerReconfigurable(new DynamicListenerConfig(kafkaServer))
     addBrokerReconfigurable(kafkaServer.socketServer)
     addBrokerReconfigurable(new DynamicProducerStateManagerConfig(kafkaServer.logManager.producerStateManagerConfig))
@@ -604,6 +605,20 @@ class DynamicLogConfig(logManager: LogManager) extends BrokerReconfigurable with
   }
 }
 
+class DynamicCordonedLogDirs(logManager: LogManager) extends BrokerReconfigurable {
+
+  override def reconfigurableConfigs: util.Set[String] = {
+    util.Set.of(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG)
+  }
+
+  // The entries are validated against log.dirs when the new KafkaConfig is created.
+  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {}
+
+  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
+    logManager.updateCordonedLogDirs(newConfig.cordonedLogDirs)
+  }
+}
+
 class ControllerDynamicThreadPool(controller: ControllerServer) extends BrokerReconfigurable {
 
   override def reconfigurableConfigs: util.Set[String] = {
diff --git a/core/src/main/scala/kafka/server/KafkaConfig.scala b/core/src/main/scala/kafka/server/KafkaConfig.scala
index d05ef90e6f..90001d11fb 100755
--- a/core/src/main/scala/kafka/server/KafkaConfig.scala
+++ b/core/src/main/scala/kafka/server/KafkaConfig.scala
@@ -494,6 +494,11 @@ class KafkaConfig private(doLog: Boolean, val props: util.Map[_, _])
     require(logRollTimeJitterMillis >= 0, "log.roll.jitter.ms must be greater than or equal to 0")
     require(logRetentionTimeMillis >= 1 || logRetentionTimeMillis == -1, "log.retention.ms must be unlimited (-1) or, greater than or equal to 1")
     require(logDirs.size > 0, "At least one log directory must be defined via log.dirs or log.dir.")
+    val absoluteLogDirs = logDirs.asScala.map(new java.io.File(_).getAbsolutePath).toSet
+    val unknownCordonedLogDirs = cordonedLogDirs.asScala.filter(dir =>
+      dir != ServerLogConfigs.CORDONED_LOG_DIRS_ALL && !absoluteLogDirs.contains(new java.io.File(dir).getAbsolutePath))
+    require(unknownCordonedLogDirs.isEmpty,
+      s"${ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG} contains entries that are not in log.dirs or log.dir: ${unknownCordonedLogDirs.mkString(",")}")
     require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.")
     require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
       " to prevent unnecessary socket timeouts")
diff --git a/core/src/main/scala/kafka/server/ReplicaManager.scala b/core/src/main/scala/kafka/server/ReplicaManager.scala
index d2471b2ecd..ac42094daa 100644
--- a/core/src/main/scala/kafka/server/ReplicaManager.scala
+++ b/core/src/main/scala/kafka/server/ReplicaManager.scala
@@ -1117,6 +1117,8 @@ class ReplicaManager(val config: KafkaConfig,
             throw new InvalidTopicException("The topic name is too long.")
           if (!logManager.isLogDirOnline(destinationDir))
             throw new KafkaStorageException(s"Log directory $destinationDir is offline")
+          if (logManager.isCordoned(destinationDir))
+            throw new InvalidReplicaAssignmentException(s"Log directory $destinationDir is cordoned")
 
           getPartition(topicPartition) match {
             case online: HostedPartition.Online[Partition] =>
@@ -1171,6 +1173,7 @@ class ReplicaManager(val config: KafkaConfig,
           case e@(_: InvalidTopicException |
                   _: LogDirNotFoundException |
                   _: ReplicaNotAvailableException |
+                  _: InvalidReplicaAssignmentException |
                   _: KafkaStorageException) =>
             warn(s"Unable to alter log dirs for $topicPartition", e)
             (topicPartition, Errors.forException(e))
@@ -1231,6 +1234,7 @@ class ReplicaManager(val config: KafkaConfig,
           .setErrorCode(Errors.NONE.code)
           .setTotalBytes(totalBytes)
           .setUsableBytes(usableBytes)
+          .setIsCordoned(logManager.isCordoned(absolutePath))
         describeLogDirsResult
 
       } catch {
diff --git a/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java b/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java
index 4a355c206b..db1f26a5b7 100644
--- a/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java
+++ b/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java
@@ -65,6 +65,7 @@ import java.util.Iterator;
 import java.util.List;
 import java.util.Map;
 import java.util.Map.Entry;
+import java.util.NoSuchElementException;
 import java.util.Optional;
 import java.util.OptionalLong;
 import java.util.Random;
@@ -617,6 +618,7 @@ public class ClusterControlManager {
             record.epoch(),
             BrokerRegistrationFencingChange.FENCE.asBoolean(),
             BrokerRegistrationInControlledShutdownChange.NONE.asBoolean(),
+            Optional.empty(),
             Optional.empty()
         );
     }
@@ -628,6 +630,7 @@ public class ClusterControlManager {
             record.epoch(),
             BrokerRegistrationFencingChange.UNFENCE.asBoolean(),
             BrokerRegistrationInControlledShutdownChange.NONE.asBoolean(),
+            Optional.empty(),
             Optional.empty()
         );
     }
@@ -648,7 +651,8 @@ public class ClusterControlManager {
             record.brokerEpoch(),
             fencingChange.asBoolean(),
             inControlledShutdownChange.asBoolean(),
-            directoriesChange
+            directoriesChange,
+            Optional.ofNullable(record.cordonedLogDirs())
         );
     }
 
@@ -658,7 +662,8 @@ public class ClusterControlManager {
         long brokerEpoch,
         Optional<Boolean> fencingChange,
         Optional<Boolean> inControlledShutdownChange,
-        Optional<List<Uuid>> directoriesChange
+        Optional<List<Uuid>> directoriesChange,
+        Optional<List<Uuid>> cordonedDirectoriesChange
     ) {
         BrokerRegistration curRegistration = brokerRegistrations.get(brokerId);
         if (curRegistration == null) {
@@ -671,7 +676,8 @@ public class ClusterControlManager {
             BrokerRegistration nextRegistration = curRegistration.cloneWith(
                 fencingChange,
                 inControlledShutdownChange,
-                directoriesChange
+                directoriesChange,
+                cordonedDirectoriesChange
             );
             if (!curRegistration.equals(nextRegistration)) {
                 log.info("Replayed {} modifying the registration for broker {}: {}",
@@ -704,8 +710,33 @@ public class ClusterControlManager {
         if (heartbeatManager == null) {
             throw new RuntimeException("ClusterControlManager is not active.");
         }
-        return heartbeatManager.usableBrokers(
+        Iterator<UsableBroker> usableBrokers = heartbeatManager.usableBrokers(
             id -> brokerRegistrations.get(id).rack());
+        // Brokers with all their log directories cordoned do not get new partitions.
+        return new Iterator<>() {
+            private UsableBroker next = null;
+
+            @Override
+            public boolean hasNext() {
+                while (next == null && usableBrokers.hasNext()) {
+                    UsableBroker candidate = usableBrokers.next();
+                    if (brokerRegistrations.get(candidate.id()).hasUncordonedDir()) {
+                        next = candidate;
+                    }
+                }
+                return next != null;
+            }
+
+            @Override
+            public UsableBroker next() {
+                if (!hasNext()) {
+                    throw new NoSuchElementException();
+                }
+                UsableBroker result = next;
+                next = null;
+                return result;
+            }
+        };
     }
 
     /**
diff --git a/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java b/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
index 4a0219d175..289665ae94 100644
--- a/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
+++ b/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
@@ -33,6 +33,8 @@ import org.apache.kafka.common.utils.LogContext;
 import org.apache.kafka.metadata.KafkaConfigSchema;
 import org.apache.kafka.server.common.ApiMessageAndVersion;
 import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
+import org.apache.kafka.server.common.MetadataVersion;
+import org.apache.kafka.server.config.ServerLogConfigs;
 import org.apache.kafka.server.mutable.BoundedList;
 import org.apache.kafka.server.policy.AlterConfigPolicy;
 import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
@@ -336,6 +338,8 @@ public class ConfigurationControlManager {
                 return DISALLOWED_BROKER_MIN_ISR_TRANSITION_ERROR;
             } else if (isDisallowedClusterMinIsrTransition(configRecord)) {
                 return DISALLOWED_CLUSTER_MIN_ISR_REMOVAL_ERROR;
+            } else if (isDisallowedCordonedLogDirsSetting(configRecord)) {
+                return DISALLOWED_CORDONED_LOG_DIRS_ERROR;
             } else if (configRecord.value() == null) {
                 allConfigs.remove(configRecord.name());
             } else if (configRecord.value().length() > Short.MAX_VALUE) {
@@ -390,6 +394,17 @@ public class ConfigurationControlManager {
         new ApiError(INVALID_CONFIG, "The configuration value cannot be added because " +
             "it exceeds the maximum value size of " + Short.MAX_VALUE + " bytes.");
 
+    private static final ApiError DISALLOWED_CORDONED_LOG_DIRS_ERROR =
+        new ApiError(INVALID_CONFIG, ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG +
+            " cannot be set before metadata version " + MetadataVersion.IBP_4_3_IV0 + ".");
+
+    boolean isDisallowedCordonedLogDirsSetting(ConfigRecord configRecord) {
+        return configRecord.name().equals(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG) &&
+            configRecord.resourceType() == BROKER.id() &&
+            configRecord.value() != null &&
+            !featureControl.metadataVersionOrThrow().isAtLeast(MetadataVersion.IBP_4_3_IV0);
+    }
+
     boolean isDisallowedBrokerMinIsrTransition(ConfigRecord configRecord) {
         if (configRecord.name().equals(MIN_IN_SYNC_REPLICAS_CONFIG) &&
                 configRecord.resourceType() == BROKER.id() &&
diff --git a/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java b/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java
index 316213bbc0..553a118dbf 100644
--- a/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java
+++ b/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java
@@ -25,6 +25,7 @@ import org.apache.kafka.common.Uuid;
 import org.apache.kafka.common.config.ConfigResource;
 import org.apache.kafka.common.errors.ApiException;
 import org.apache.kafka.common.errors.BrokerIdNotRegisteredException;
+import org.apache.kafka.common.errors.IneligibleReplicaException;
 import org.apache.kafka.common.errors.InvalidPartitionsException;
 import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
 import org.apache.kafka.common.errors.InvalidReplicationFactorException;
@@ -94,6 +95,7 @@ import org.apache.kafka.metadata.placement.PlacementSpec;
 import org.apache.kafka.metadata.placement.TopicAssignment;
 import org.apache.kafka.metadata.placement.UsableBroker;
 import org.apache.kafka.server.common.ApiMessageAndVersion;
+import org.apache.kafka.server.common.MetadataVersion;
 import org.apache.kafka.server.common.TopicIdPartition;
 import org.apache.kafka.server.mutable.BoundedList;
 import org.apache.kafka.server.policy.CreateTopicPolicy;
@@ -737,6 +739,7 @@ public class ReplicationControlManager {
                 }
                 PartitionAssignment partitionAssignment = new PartitionAssignment(assignment.brokerIds(), clusterDescriber);
                 validateManualPartitionAssignment(partitionAssignment, replicationFactor);
+                validateNoCordonedBrokers(partitionAssignment.replicas());
                 replicationFactor = OptionalInt.of(assignment.brokerIds().size());
                 List<Integer> isr = assignment.brokerIds().stream().
                     filter(clusterControl::isActive).toList();
@@ -1531,6 +1534,34 @@ public class ReplicationControlManager {
         }
     }
 
+    /**
+     * Update the cordoned directories of a broker if the broker reported a set of cordoned
+     * directories which differs from the one in its registration.
+     *
+     * @param brokerId        The broker ID.
+     * @param brokerEpoch     The broker epoch.
+     * @param cordonedDirs    The cordoned directories reported by the broker, or null if it did not report any.
+     * @param records         The record list to append to.
+     */
+    void handleCordonedDirectories(
+        int brokerId,
+        long brokerEpoch,
+        List<Uuid> cordonedDirs,
+        List<ApiMessageAndVersion> records
+    ) {
+        if (cordonedDirs == null) return;
+        BrokerRegistration registration = clusterControl.registration(brokerId);
+        List<Uuid> sortedCordonedDirs = new ArrayList<>(cordonedDirs);
+        sortedCordonedDirs.sort(Uuid::compareTo);
+        if (sortedCordonedDirs.equals(registration.cordonedDirectories())) return;
+        records.add(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
+                setBrokerId(brokerId).setBrokerEpoch(brokerEpoch).
+                setCordonedLogDirs(sortedCordonedDirs),
+                (short) 3));
+        log.info("Cordoned directories of broker {} changed from {} to {}",
+                brokerId, registration.cordonedDirectories(), sortedCordonedDirs);
+    }
+
     ControllerResult<ElectLeadersResponseData> electLeaders(ElectLeadersRequestData request) {
         ElectionType electionType = electionType(request.electionType());
         List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
@@ -1667,6 +1698,9 @@ public class ReplicationControlManager {
         if (featureControl.metadataVersionOrThrow().isDirectoryAssignmentSupported()) {
             handleDirectoriesOffline(brokerId, brokerEpoch, request.offlineLogDirs(), records);
         }
+        if (featureControl.metadataVersionOrThrow().isAtLeast(MetadataVersion.IBP_4_3_IV0)) {
+            handleCordonedDirectories(brokerId, brokerEpoch, request.cordonedLogDirs(), records);
+        }
         boolean isCaughtUp = request.currentMetadataOffset() >= registerBrokerRecordOffset;
         BrokerHeartbeatReply reply = new BrokerHeartbeatReply(isCaughtUp,
                 states.next().fenced(),
@@ -1928,6 +1962,7 @@ public class ReplicationControlManager {
                 List<Integer> replicas = topic.assignments().get(i).brokerIds();
                 PartitionAssignment partitionAssignment = new PartitionAssignment(replicas, clusterDescriber);
                 validateManualPartitionAssignment(partitionAssignment, OptionalInt.of(replicationFactor));
+                validateNoCordonedBrokers(partitionAssignment.replicas());
                 partitionAssignments.add(partitionAssignment);
                 List<Integer> isr = partitionAssignment.replicas().stream().
                     filter(clusterControl::isActive).toList();
@@ -1966,6 +2001,21 @@ public class ReplicationControlManager {
         }
     }
 
+    /**
+     * Throws InvalidReplicaAssignmentException if any of the given brokers has all its log
+     * directories cordoned.
+     */
+    private void validateNoCordonedBrokers(List<Integer> brokerIds) {
+        for (int brokerId : brokerIds) {
+            BrokerRegistration registration = clusterControl.registration(brokerId);
+            if (registration != null && !registration.hasUncordonedDir()) {
+                throw new InvalidReplicaAssignmentException("The manual partition " +
+                    "assignment includes broker " + brokerId + ", but all of its log " +
+                    "directories are cordoned.");
+            }
+        }
+    }
+
     void validateManualPartitionAssignment(
         PartitionAssignment assignment,
         OptionalInt replicationFactor
@@ -2234,6 +2284,14 @@ public class ReplicationControlManager {
         PartitionAssignment targetAssignment = new PartitionAssignment(target.replicas(), clusterDescriber);
 
         validateManualPartitionAssignment(targetAssignment, OptionalInt.empty());
+        for (int brokerId : targetAssignment.replicas()) {
+            BrokerRegistration registration = clusterControl.registration(brokerId);
+            if (!currentAssignment.replicas().contains(brokerId) &&
+                    registration != null && !registration.hasUncordonedDir()) {
+                throw new IneligibleReplicaException("Unable to reassign " + tp + " to broker " +
+                    brokerId + " because all of its log directories are cordoned.");
+            }
+        }
         if (!allowRFChange) {
             validatePartitionReplicationFactorUnchanged(part, target);
         }
diff --git a/metadata/src/main/java/org/apache/kafka/image/ClusterDelta.java b/metadata/src/main/java/org/apache/kafka/image/ClusterDelta.java
index 8f0556959b..c154eb74cb 100644
--- a/metadata/src/main/java/org/apache/kafka/image/ClusterDelta.java
+++ b/metadata/src/main/java/org/apache/kafka/image/ClusterDelta.java
@@ -143,7 +143,8 @@ public final class ClusterDelta {
         BrokerRegistration nextRegistration = curRegistration.cloneWith(
             fencingChange.asBoolean(),
             inControlledShutdownChange.asBoolean(),
-            directoriesChange
+            directoriesChange,
+            Optional.ofNullable(record.cordonedLogDirs())
         );
         if (!curRegistration.equals(nextRegistration)) {
             changedBrokers.put(record.brokerId(), Optional.of(nextRegistration));
diff --git a/metadata/src/main/java/org/apache/kafka/metadata/BrokerRegistration.java b/metadata/src/main/java/org/apache/kafka/metadata/BrokerRegistration.java
index 56b2bdf375..a789ffca7d 100644
--- a/metadata/src/main/java/org/apache/kafka/metadata/BrokerRegistration.java
+++ b/metadata/src/main/java/org/apache/kafka/metadata/BrokerRegistration.java
@@ -53,6 +53,7 @@ public class BrokerRegistration {
         private boolean inControlledShutdown;
         private boolean isMigratingZkBroker;
         private List<Uuid> directories;
+        private List<Uuid> cordonedDirectories;
 
         public Builder() {
             this.id = 0;
@@ -65,6 +66,7 @@ public class BrokerRegistration {
             this.inControlledShutdown = false;
             this.isMigratingZkBroker = false;
             this.directories = List.of();
+            this.cordonedDirectories = List.of();
         }
 
         public Builder setId(int id) {
@@ -127,6 +129,11 @@ public class BrokerRegistration {
             return this;
         }
 
+        public Builder setCordonedDirectories(List<Uuid> cordonedDirectories) {
+            this.cordonedDirectories = cordonedDirectories;
+            return this;
+        }
+
         public BrokerRegistration build() {
             return new BrokerRegistration(
                 id,
@@ -138,7 +145,8 @@ public class BrokerRegistration {
                 fenced,
                 inControlledShutdown,
                 isMigratingZkBroker,
-                directories);
+                directories,
+                cordonedDirectories);
         }
     }
 
@@ -152,6 +160,7 @@ public class BrokerRegistration {
     private final boolean inControlledShutdown;
     private final boolean isMigratingZkBroker;
     private final List<Uuid> directories;
+    private final List<Uuid> cordonedDirectories;
 
     private BrokerRegistration(
         int id,
@@ -163,7 +172,8 @@ public class BrokerRegistration {
         boolean fenced,
         boolean inControlledShutdown,
         boolean isMigratingZkBroker,
-        List<Uuid> directories
+        List<Uuid> directories,
+        List<Uuid> cordonedDirectories
     ) {
         this.id = id;
         this.epoch = epoch;
@@ -185,6 +195,9 @@ public class BrokerRegistration {
         directories = new ArrayList<>(directories);
         directories.sort(Uuid::compareTo);
         this.directories = Collections.unmodifiableList(directories);
+        cordonedDirectories = new ArrayList<>(cordonedDirectories);
+        cordonedDirectories.sort(Uuid::compareTo);
+        this.cordonedDirectories = Collections.unmodifiableList(cordonedDirectories);
     }
 
     public static BrokerRegistration fromRecord(RegisterBrokerRecord record) {
@@ -209,7 +222,8 @@ public class BrokerRegistration {
             record.fenced(),
             record.inControlledShutdown(),
             record.isMigratingZkBroker(),
-            record.logDirs());
+            record.logDirs(),
+            List.of());
     }
 
     public int id() {
@@ -260,10 +274,35 @@ public class BrokerRegistration {
         return directories;
     }
 
+    public List<Uuid> cordonedDirectories() {
+        return cordonedDirectories;
+    }
+
     public boolean hasOnlineDir(Uuid dir) {
         return DirectoryId.isOnline(dir, directories);
     }
 
+    /**
+     * Returns true if this broker has at least one online log directory that is not cordoned.
+     * Brokers that did not register any log directory are considered to have an uncordoned directory.
+     */
+    public boolean hasUncordonedDir() {
+        if (directories.isEmpty()) return true;
+        for (Uuid directory : directories) {
+            if (!cordonedDirectories.contains(directory) && DirectoryId.isOnline(directory, directories)) {
+                return true;
+            }
+        }
+        return false;
+    }
+
+    /**
+     * Returns true if the given directory is a known online directory of this broker that is not cordoned.
+     */
+    public boolean isUncordonedDir(Uuid dir) {
+        return hasOnlineDir(dir) && !cordonedDirectories.contains(dir);
+    }
+
     public List<Uuid> directoryIntersection(List<Uuid> otherDirectories) {
         List<Uuid> results = new ArrayList<>();
         for (Uuid directory : directories) {
@@ -330,7 +369,7 @@ public class BrokerRegistration {
     @Override
     public int hashCode() {
         return Objects.hash(id, epoch, incarnationId, listeners, supportedFeatures,
-            rack, fenced, inControlledShutdown, isMigratingZkBroker, directories);
+            rack, fenced, inControlledShutdown, isMigratingZkBroker, directories, cordonedDirectories);
     }
 
     @Override
@@ -345,7 +384,8 @@ public class BrokerRegistration {
             other.fenced == fenced &&
             other.inControlledShutdown == inControlledShutdown &&
             other.isMigratingZkBroker == isMigratingZkBroker &&
-            other.directories.equals(directories);
+            other.directories.equals(directories) &&
+            other.cordonedDirectories.equals(cordonedDirectories);
     }
 
     @Override
@@ -367,6 +407,7 @@ public class BrokerRegistration {
                 ", inControlledShutdown=" + inControlledShutdown +
                 ", isMigratingZkBroker=" + isMigratingZkBroker +
                 ", directories=" + directories +
+                (cordonedDirectories.isEmpty() ? "" : ", cordonedDirectories=" + cordonedDirectories) +
                 ")";
     }
 
@@ -374,12 +415,23 @@ public class BrokerRegistration {
         Optional<Boolean> fencingChange,
         Optional<Boolean> inControlledShutdownChange,
         Optional<List<Uuid>> directoriesChange
+    ) {
+        return cloneWith(fencingChange, inControlledShutdownChange, directoriesChange, Optional.empty());
+    }
+
+    public BrokerRegistration cloneWith(
+        Optional<Boolean> fencingChange,
+        Optional<Boolean> inControlledShutdownChange,
+        Optional<List<Uuid>> directoriesChange,
+        Optional<List<Uuid>> cordonedDirectoriesChange
     ) {
         boolean newFenced = fencingChange.orElse(fenced);
         boolean newInControlledShutdownChange = inControlledShutdownChange.orElse(inControlledShutdown);
         List<Uuid> newDirectories = directoriesChange.orElse(directories);
+        List<Uuid> newCordonedDirectories = cordonedDirectoriesChange.orElse(cordonedDirectories);
 
-        if (newFenced == fenced && newInControlledShutdownChange == inControlledShutdown && newDirectories.equals(directories))
+        if (newFenced == fenced && newInControlledShutdownChange == inControlledShutdown && newDirectories.equals(directories) &&
+            sorted(newCordonedDirectories).equals(cordonedDirectories))
             return this;
 
         return new BrokerRegistration(
@@ -392,7 +444,14 @@ public class BrokerRegistration {
             newFenced,
             newInControlledShutdownChange,
             isMigratingZkBroker,
-            newDirectories
+            newDirectories,
+            newCordonedDirectories
         );
     }
+
+    private static List<Uuid> sorted(List<Uuid> uuids) {
+        List<Uuid> result = new ArrayList<>(uuids);
+        result.sort(Uuid::compareTo);
+        return result;
+    }
 }
diff --git a/metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json b/metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json
index 7a484a6aeb..d498899b0d 100644
--- a/metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json
+++ b/metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json
@@ -15,11 +15,12 @@
 
 // Version 1 adds InControlledShutdown
 // Version 2 adds LogDirs
+// Version 3 adds CordonedLogDirs
 {
   "apiKey": 17,
   "type": "metadata",
   "name": "BrokerRegistrationChangeRecord",
-  "validVersions": "0-2",
+  "validVersions": "0-3",
   "flexibleVersions": "0+",
   "fields": [
    { "name": "BrokerId", "type": "int32", "versions": "0+", "entityType": "brokerId",
@@ -31,6 +32,9 @@
    { "name": "InControlledShutdown", "type": "int8", "versions": "1+", "taggedVersions": "1+", "tag": 1,
      "about": "0 if no change, 1 if the broker is in controlled shutdown." },
    { "name": "LogDirs", "type":  "[]uuid", "versions":  "2+", "taggedVersions": "2+", "tag": 2,
-     "about": "Log directories configured in this broker which are available." }
+     "about": "Log directories configured in this broker which are available." },
+   { "name": "CordonedLogDirs", "type":  "[]uuid", "versions":  "3+", "taggedVersions": "3+", "tag": 3,
+     "nullableVersions": "3+", "default": "null",
+     "about": "Log directories of this broker which are cordoned, or null if there is no change." }
   ]
 }
diff --git a/metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java b/metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java
index 502c3881e9..4c888def4b 100644
--- a/metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java
+++ b/metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java
@@ -68,6 +68,7 @@ import java.util.HashSet;
 import java.util.Iterator;
 import java.util.List;
 import java.util.Map;
+import java.util.NoSuchElementException;
 import java.util.Optional;
 import java.util.OptionalLong;
 import java.util.Set;
@@ -452,6 +453,96 @@ public class ClusterControlManagerTest {
         }
     }
 
+    private static void registerUnfencedBroker(ClusterControlManager clusterControl, int brokerId, List<Uuid> logDirs) {
+        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
+            setBrokerEpoch(100).
+            setBrokerId(brokerId).
+            setLogDirs(logDirs);
+        brokerRecord.endPoints().add(new BrokerEndpoint().
+            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
+            setPort((short) 9092).
+            setName("PLAINTEXT").
+            setHost("example.com"));
+        clusterControl.replay(brokerRecord, 100L);
+        clusterControl.replay(new UnfenceBrokerRecord().setId(brokerId).setEpoch(100));
+        clusterControl.heartbeatManager().touch(brokerId, false, 0);
+    }
+
+    private static Set<Integer> usableBrokerIds(ClusterControlManager clusterControl) {
+        Set<Integer> ids = new HashSet<>();
+        clusterControl.usableBrokers().forEachRemaining(broker -> ids.add(broker.id()));
+        return ids;
+    }
+
+    @Test
+    public void testCordonedDirectories() {
+        MockTime time = new MockTime(0, 0, 0);
+        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
+        FeatureControlManager featureControl = new FeatureControlManager.Builder().
+            setSnapshotRegistry(snapshotRegistry).
+            setQuorumFeatures(new QuorumFeatures(0,
+                QuorumFeatures.defaultSupportedFeatureMap(true),
+                List.of(0))).
+            build();
+        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
+            setTime(time).
+            setSnapshotRegistry(snapshotRegistry).
+            setSessionTimeoutNs(1000).
+            setFeatureControlManager(featureControl).
+            setBrokerShutdownHandler((brokerId, isCleanShutdown, records) -> { }).
+            build();
+        clusterControl.activate();
+        Uuid dir1 = Uuid.randomUuid();
+        Uuid dir2 = Uuid.randomUuid();
+        Uuid dir3 = Uuid.randomUuid();
+        registerUnfencedBroker(clusterControl, 0, List.of(dir1, dir2));
+        registerUnfencedBroker(clusterControl, 1, List.of(dir3));
+        // A broker which did not register any directory is never considered cordoned.
+        registerUnfencedBroker(clusterControl, 2, List.of());
+        assertEquals(Set.of(0, 1, 2), usableBrokerIds(clusterControl));
+        assertEquals(List.of(), clusterControl.registration(0).cordonedDirectories());
+        assertTrue(clusterControl.registration(0).hasUncordonedDir());
+        assertTrue(clusterControl.registration(2).hasUncordonedDir());
+
+        // Cordoning only some of the directories of a broker keeps it usable.
+        clusterControl.replay(new BrokerRegistrationChangeRecord().
+            setBrokerId(0).setBrokerEpoch(100).setCordonedLogDirs(List.of(dir2)));
+        assertEquals(List.of(dir2), clusterControl.registration(0).cordonedDirectories());
+        assertTrue(clusterControl.registration(0).hasUncordonedDir());
+        assertTrue(clusterControl.registration(0).isUncordonedDir(dir1));
+        assertFalse(clusterControl.registration(0).isUncordonedDir(dir2));
+        assertEquals(Set.of(0, 1, 2), usableBrokerIds(clusterControl));
+
+        // Cordoning all the directories of a broker excludes it from the usable brokers.
+        clusterControl.replay(new BrokerRegistrationChangeRecord().
+            setBrokerId(0).setBrokerEpoch(100).setCordonedLogDirs(List.of(dir2, dir1)));
+        clusterControl.replay(new BrokerRegistrationChangeRecord().
+            setBrokerId(1).setBrokerEpoch(100).setCordonedLogDirs(List.of(dir3)));
+        assertEquals(Stream.of(dir1, dir2).sorted().toList(), clusterControl.registration(0).cordonedDirectories());
+        assertFalse(clusterControl.registration(0).hasUncordonedDir());
+        assertFalse(clusterControl.registration(1).hasUncordonedDir());
+        assertEquals(Set.of(2), usableBrokerIds(clusterControl));
+        Iterator<UsableBroker> iterator = clusterControl.usableBrokers();
+        assertTrue(iterator.hasNext());
+        assertTrue(iterator.hasNext());
+        assertEquals(2, iterator.next().id());
+        assertFalse(iterator.hasNext());
+        assertThrows(NoSuchElementException.class, iterator::next);
+
+        // Records which don't have cordoned directories leave them unchanged.
+        clusterControl.replay(new BrokerRegistrationChangeRecord().
+            setBrokerId(1).setBrokerEpoch(100).
+            setFenced(BrokerRegistrationFencingChange.UNFENCE.value()));
+        assertEquals(List.of(dir3), clusterControl.registration(1).cordonedDirectories());
+        assertEquals(Set.of(2), usableBrokerIds(clusterControl));
+
+        // An empty list uncordons all the directories.
+        clusterControl.replay(new BrokerRegistrationChangeRecord().
+            setBrokerId(0).setBrokerEpoch(100).setCordonedLogDirs(List.of()));
+        assertEquals(List.of(), clusterControl.registration(0).cordonedDirectories());
+        assertEquals(Set.of(0, 2), usableBrokerIds(clusterControl));
+    }
+
     @Test
     public void testRegistrationsToRecords() {
         MetadataVersion metadataVersion = MetadataVersion.MINIMUM_VERSION;
diff --git a/metadata/src/test/java/org/apache/kafka/controller/ReplicationControlManagerTest.java b/metadata/src/test/java/org/apache/kafka/controller/ReplicationControlManagerTest.java
index 7024d0de3a..6e03f9d00f 100644
--- a/metadata/src/test/java/org/apache/kafka/controller/ReplicationControlManagerTest.java
+++ b/metadata/src/test/java/org/apache/kafka/controller/ReplicationControlManagerTest.java
@@ -126,6 +126,7 @@ import java.util.concurrent.TimeUnit;
 import java.util.concurrent.atomic.AtomicLong;
 import java.util.stream.Collectors;
 import java.util.stream.IntStream;
+import java.util.stream.Stream;
 
 import static org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG;
 import static org.apache.kafka.common.metadata.MetadataRecordType.CLEAR_ELR_RECORD;
@@ -3344,6 +3345,141 @@ public class ReplicationControlManagerTest {
             RecordTestUtils.iteratorToSet(ctx.replicationControl.brokersToIsrs().iterator(2, true)));
     }
 
+    private static List<ApiMessageAndVersion> heartbeatWithCordonedDirs(
+        ReplicationControlTestContext ctx,
+        int brokerId,
+        List<Uuid> cordonedLogDirs
+    ) {
+        ControllerResult<BrokerHeartbeatReply> result = ctx.replicationControl.
+            processBrokerHeartbeat(new BrokerHeartbeatRequestData().
+                setBrokerId(brokerId).setBrokerEpoch(defaultBrokerEpoch(brokerId)).
+                setCurrentMetadataOffset(1).
+                setWantFence(false).setWantShutDown(false).
+                setCordonedLogDirs(cordonedLogDirs), 0);
+        ctx.replay(result.records());
+        return result.records();
+    }
+
+    private static Set<Integer> replicaBrokers(ReplicationControlTestContext ctx, Uuid topicId, int numPartitions) {
+        Set<Integer> brokers = new HashSet<>();
+        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
+            brokers.addAll(Replicas.toList(ctx.replicationControl.getPartition(topicId, partitionId).replicas));
+        }
+        return brokers;
+    }
+
+    @Test
+    void testHandleCordonedDirectoriesInHeartbeat() {
+        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
+        Uuid dir1b1 = Uuid.randomUuid();
+        Uuid dir2b1 = Uuid.randomUuid();
+        ctx.registerBrokersWithDirs(1, List.of(dir1b1, dir2b1));
+        ctx.unfenceBrokers(1);
+        assertEquals(List.of(), ctx.clusterControl.registration(1).cordonedDirectories());
+
+        // A heartbeat which doesn't report cordoned directories doesn't change them.
+        assertEquals(List.of(), filter(heartbeatWithCordonedDirs(ctx, 1, null), BrokerRegistrationChangeRecord.class));
+
+        // Reporting cordoned directories changes the registration, the order of the directories doesn't matter.
+        List<Uuid> sortedDirs = Stream.of(dir1b1, dir2b1).sorted().toList();
+        List<ApiMessageAndVersion> records = heartbeatWithCordonedDirs(ctx, 1, List.of(dir2b1, dir1b1));
+        assertEquals(
+            List.of(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord()
+                .setBrokerId(1).setBrokerEpoch(defaultBrokerEpoch(1))
+                .setCordonedLogDirs(sortedDirs), (short) 3)),
+            filter(records, BrokerRegistrationChangeRecord.class));
+        assertEquals(sortedDirs, ctx.clusterControl.registration(1).cordonedDirectories());
+        assertFalse(ctx.clusterControl.registration(1).hasUncordonedDir());
+
+        // Reporting the same directories doesn't generate records.
+        assertEquals(List.of(), heartbeatWithCordonedDirs(ctx, 1, List.of(dir1b1, dir2b1)));
+
+        // Uncordoning.
+        records = heartbeatWithCordonedDirs(ctx, 1, List.of(dir1b1));
+        assertEquals(List.of(dir1b1), ctx.clusterControl.registration(1).cordonedDirectories());
+        assertTrue(ctx.clusterControl.registration(1).hasUncordonedDir());
+        assertEquals(1, filter(records, BrokerRegistrationChangeRecord.class).size());
+        heartbeatWithCordonedDirs(ctx, 1, List.of());
+        assertEquals(List.of(), ctx.clusterControl.registration(1).cordonedDirectories());
+    }
+
+    @Test
+    void testCordonedDirectoriesAreIgnoredBeforeMetadataVersion() {
+        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
+            setMetadataVersion(MetadataVersion.IBP_4_2_IV1).build();
+        Uuid dir1b1 = Uuid.randomUuid();
+        ctx.registerBrokersWithDirs(1, List.of(dir1b1));
+        ctx.unfenceBrokers(1);
+        assertEquals(List.of(), heartbeatWithCordonedDirs(ctx, 1, List.of(dir1b1)));
+        assertEquals(List.of(), ctx.clusterControl.registration(1).cordonedDirectories());
+    }
+
+    @Test
+    void testCordonedBrokersAreNotUsedForNewReplicas() {
+        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
+        Uuid dir0 = Uuid.randomUuid();
+        Uuid dir1 = Uuid.randomUuid();
+        Uuid dir2a = Uuid.randomUuid();
+        Uuid dir2b = Uuid.randomUuid();
+        Uuid dir3 = Uuid.randomUuid();
+        ctx.registerBrokersWithDirs(0, List.of(dir0), 1, List.of(dir1), 2, List.of(dir2a, dir2b), 3, List.of(dir3));
+        ctx.unfenceBrokers(0, 1, 2, 3);
+        Uuid stayId = ctx.createTestTopic("stay", new int[][] {new int[] {1, 2}}).topicId();
+        Uuid moveId = ctx.createTestTopic("move", new int[][] {new int[] {0, 1}}).topicId();
+
+        // A broker which has an uncordoned directory is still usable.
+        heartbeatWithCordonedDirs(ctx, 2, List.of(dir2a));
+        assertEquals(Set.of(0, 1, 2, 3), replicaBrokers(ctx, ctx.createTestTopic("four", 8, (short) 4, NONE.code()).topicId(), 8));
+
+        // Cordon all the directories of broker 2.
+        heartbeatWithCordonedDirs(ctx, 2, List.of(dir2a, dir2b));
+
+        // The brokers with all their directories cordoned are not used by the replica placer.
+        ctx.createTestTopic("too-many", 1, (short) 4, INVALID_REPLICATION_FACTOR.code());
+        Uuid threeId = ctx.createTestTopic("three", 8, (short) 3, NONE.code()).topicId();
+        assertEquals(Set.of(0, 1, 3), replicaBrokers(ctx, threeId, 8));
+
+        // Neither are they used by manual assignments.
+        ctx.createTestTopic("manual", new int[][] {new int[] {0, 2}}, INVALID_REPLICA_ASSIGNMENT.code());
+        ctx.createPartitions(9, "three", new int[][] {new int[] {0, 1, 2}}, INVALID_REPLICA_ASSIGNMENT.code());
+        assertEquals(8, ctx.replicationControl.getTopic(threeId).numPartitions(Long.MAX_VALUE));
+        ctx.createPartitions(9, "three", new int[][] {new int[] {0, 1, 3}}, NONE.code());
+
+        // New partitions that are placed automatically only use the other brokers.
+        ControllerResult<List<CreatePartitionsTopicResult>> result = ctx.replicationControl.createPartitions(
+            anonymousContextFor(ApiKeys.CREATE_PARTITIONS), List.of(new CreatePartitionsTopic().setName("three").setCount(20).setAssignments(null)));
+        assertEquals(NONE.code(), result.response().get(0).errorCode());
+        ctx.replay(result.records());
+        assertEquals(Set.of(0, 1, 3), replicaBrokers(ctx, threeId, 20));
+
+        // With another broker cordoned there are not enough brokers for the replication factor.
+        heartbeatWithCordonedDirs(ctx, 3, List.of(dir3));
+        result = ctx.replicationControl.createPartitions(
+            anonymousContextFor(ApiKeys.CREATE_PARTITIONS), List.of(new CreatePartitionsTopic().setName("three").setCount(30).setAssignments(null)));
+        assertEquals(INVALID_REPLICATION_FACTOR.code(), result.response().get(0).errorCode());
+        heartbeatWithCordonedDirs(ctx, 3, List.of());
+
+        // Reassignments can't add replicas on cordoned brokers, but can keep the existing ones.
+        AlterPartitionReassignmentsRequestData request = new AlterPartitionReassignmentsRequestData().setTopics(List.of(
+            new ReassignableTopic().setName("move").setPartitions(List.of(
+                new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(0, 2)))),
+            new ReassignableTopic().setName("stay").setPartitions(List.of(
+                new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(2, 3))))));
+        ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
+            ctx.replicationControl.alterPartitionReassignments(request);
+        assertEquals(INELIGIBLE_REPLICA.code(),
+            alterResult.response().responses().get(0).partitions().get(0).errorCode());
+        assertEquals(NONE.code(),
+            alterResult.response().responses().get(1).partitions().get(0).errorCode());
+        ctx.replay(alterResult.records());
+        assertEquals(List.of(1, 2, 3), Replicas.toList(ctx.replicationControl.getPartition(stayId, 0).replicas).stream().sorted().toList());
+        assertEquals(List.of(0, 1), Replicas.toList(ctx.replicationControl.getPartition(moveId, 0).replicas));
+
+        // Once the directories are uncordoned the broker can be used again.
+        heartbeatWithCordonedDirs(ctx, 2, List.of());
+        ctx.createTestTopic("all-four", 1, (short) 4, NONE.code());
+    }
+
     @Test
     void testHandleDirectoriesOffline() {
         ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
diff --git a/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java b/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java
index d0e1c60d6d..a39de733df 100644
--- a/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java
+++ b/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java
@@ -40,6 +40,13 @@ public class ServerLogConfigs {
     public static final String LOG_DIR_DOC = "A comma-separated list of the directories where the log data is stored. (supplemental to " + LOG_DIRS_CONFIG + " property)";
     public static final String LOG_DIRS_DOC = "A comma-separated list of the directories where the log data is stored. If not set, the value in " + LOG_DIR_CONFIG + " is used.";
 
+    public static final String CORDONED_LOG_DIRS_CONFIG = "cordoned.log.dirs";
+    public static final String CORDONED_LOG_DIRS_ALL = "*";
+    public static final String CORDONED_LOG_DIRS_DOC = "A comma-separated list of the directories that are cordoned. Entries in this list must be entries in " +
+        LOG_DIRS_CONFIG + " or " + LOG_DIR_CONFIG + " configuration. This can also be set to <code>*</code> to cordon all log directories. " +
+        "The cluster does not place new partitions on cordoned directories, and does not place new partitions on brokers whose log directories are all cordoned. " +
+        "Existing partitions on cordoned directories keep being served. This configuration requires metadata version 4.3-IV0 or higher.";
+
     public static final String LOG_SEGMENT_BYTES_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_BYTES_CONFIG);
     public static final String LOG_SEGMENT_BYTES_DOC = "The maximum size of a single log file";
 
diff --git a/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java b/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java
index e148fb1c0b..f7913817dd 100644
--- a/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java
+++ b/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java
@@ -206,6 +206,16 @@ public class BrokerLifecycleManager {
      */
     private volatile OptionalLong previousBrokerEpoch = OptionalLong.empty();
 
+    /**
+     * Provides the IDs of the cordoned log directories of this broker, which are sent to the
+     * controller in the heartbeats once the broker has caught up with the metadata.
+     */
+    private volatile Supplier<Set<Uuid>> cordonedLogDirsSupplier = Set::of;
+
+    public void setCordonedLogDirsSupplier(Supplier<Set<Uuid>> cordonedLogDirsSupplier) {
+        this.cordonedLogDirsSupplier = cordonedLogDirsSupplier;
+    }
+
     public BrokerLifecycleManager(
             AbstractKafkaConfig config,
             Time time,
@@ -532,6 +542,13 @@ public class BrokerLifecycleManager {
             .setWantFence(!readyToUnfence)
             .setWantShutDown(state == BrokerState.PENDING_CONTROLLED_SHUTDOWN)
             .setOfflineLogDirs(new ArrayList<>(offlineDirs.keySet()));
+        // The cordoned log directories are only reported once the broker has caught up with the metadata,
+        // since the metadata version that gates the feature is not known before that.
+        if (initialCatchUpFuture.isDone()) {
+            List<Uuid> cordonedLogDirs = new ArrayList<>(cordonedLogDirsSupplier.get());
+            cordonedLogDirs.sort(Uuid::compareTo);
+            data.setCordonedLogDirs(cordonedLogDirs);
+        }
         if (logger.isTraceEnabled()) {
             logger.trace("Sending broker heartbeat {}", data);
         }
diff --git a/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java b/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
index bfec3337ca..63308a841e 100644
--- a/server/src/main/
... (truncated; 28753 more characters; read the files)
```

## Answer these questions, one at a time, in this order

1. Which acceptance criterion of this task is not met, or is met only in appearance (hard-coded, special-cased, or untested path)?
2. Which relevant case (boundary, error, empty input, interaction with existing options or state) is not covered by any test of this task?
3. Which existing test was weakened: an assertion removed or loosened, a case deleted, a skip added? Look at the modified declared test files above.
4. Which part of the diff could be removed or simplified without losing behaviour the task requires (dead code, duplicated branches, unnecessary abstraction)?

For each question, report only concrete findings: `file`, `line` (in the new version; 0 if the
finding is not tied to a line), the question number as `pass`, a `severity` and a one-sentence
`text`. `blocking` means the task must not be committed as is: a criterion unmet, a relevant case
untested, an existing test weakened, or a change that does not belong to the task. `advisory`
means worth a comment but not worth another attempt. No finding is a valid answer to a question.

A `blocking` finding is a claim that some test would fail, so it carries that test as `proof`,
and you have run it with `aih check --probe` and seen it fail for the reason you claim before
reporting it (the harness runs it again; a proof that passes for the harness downgrades the
finding, whatever you saw). The proof is:
`path`, a NEW file in the directory of one of the task's test files, named like a test file of
this repository (same package for Go, `test_*.py` for pytest), and `content`, the whole file
(package clause, imports, one or more tests) asserting exactly what you claim. The harness writes
it, runs it with the task's test command and removes it. If it fails, your finding stands and the
implementer gets the test to make pass. If it passes, or does not compile, or you give none, the
finding is recorded as advisory with that reason, not as blocking. A claim you cannot turn into a
test here (behaviour on another platform, style, taste) is advisory from the start: say it, do not
block on it. The tests already pass, so a proof that merely repeats them will pass too.

## Coverage map and probes

Then fill `coverage` with one entry per acceptance criterion marked `[behavior]` above. For
each, name the single test of this task's test files whose assertion proves the criterion:
`test` as a pytest node id (`tests/path.py::test_name`), `line` of the decisive assertion,
`status` `covered` when that assertion would fail if the criterion were broken, `partial` when
a test touches the behaviour but would not fail (asserts only the happy path, or a weaker
property), `uncovered` when no test exercises it. Read the test body: do not infer coverage
from a test's name. `partial` and `uncovered` are blocking, so when you mark one, say in
`note` what test would prove it.

Each entry also carries a `probe`: a test YOU wrote for that criterion and ran with
`aih check --probe`. Approving is a claim that the behaviour holds, and the task's own tests
were written by the session that wrote the code, so they prove what it believed, not what the
criterion says. Write the probe from the criterion's text, through the surface the request names
(the program's configuration, its public API, a real client), exercising the case the criterion
is about rather than the happy path; do not copy an existing test. Run it until it runs (with
`--run <your test names>` where supported: the rest of the package is not what you are checking;
a run that executed zero tests, `[no tests to run]`, counts as not run);
a probe that passes verifies the criterion, a probe that fails is a blocking finding of pass 1 with
that probe as `proof`. The harness runs every probe again, and runs every passing probe once more
on the base commit without the production change: a probe that passes there too asserts nothing
this change adds and does not count (a test with no assertion, or one that only checks what
already held). A criterion whose probe is missing, does not run, or does not discriminate is
recorded as unprobed, visible in the pull request, and you will be asked for it once.
`probe.path` is a new file in a directory of the task's test files, named like a test file of the
repository; `probe.content` is the whole file.

`verdict` is `request_changes` if and only if there is at least one `blocking` finding or a
criterion that is not `covered`.
