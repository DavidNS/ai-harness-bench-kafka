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



## Task T2 (T1)

Implement KIP-1066 as one PR. Order: config definition + dynamic registration + MV gating; protocol/record JSON changes and LogDirDescription; core LogManager cordon state, metrics, dir choice; broker heartbeat; controller storage and placement/validation; ReplicaManager/KafkaApis; tool output; then tests, fixing existing tests broken by schema/constructor changes.

Acceptance criteria:
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
index 340e88db16..b4d4f6d728 100644
--- a/clients/src/main/java/org/apache/kafka/clients/admin/LogDirDescription.java
+++ b/clients/src/main/java/org/apache/kafka/clients/admin/LogDirDescription.java
@@ -33,16 +33,28 @@ public class LogDirDescription {
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
+    public LogDirDescription(
+        ApiException error,
+        Map<TopicPartition, ReplicaInfo> replicaInfos,
+        long totalBytes,
+        long usableBytes,
+        boolean isCordoned
+    ) {
         this.error = error;
         this.replicaInfos = replicaInfos;
         this.totalBytes = (totalBytes == UNKNOWN_VOLUME_BYTES) ? OptionalLong.empty() : OptionalLong.of(totalBytes);
         this.usableBytes = (usableBytes == UNKNOWN_VOLUME_BYTES) ? OptionalLong.empty() : OptionalLong.of(usableBytes);
+        this.isCordoned = isCordoned;
     }
 
     /**
@@ -82,6 +94,14 @@ public class LogDirDescription {
         return usableBytes;
     }
 
+    /**
+     * Whether this log directory is cordoned. New partitions are not placed on a cordoned log directory,
+     * while the partitions it already hosts keep being served. This is false if the broker did not return a value.
+     */
+    public boolean isCordoned() {
+        return isCordoned;
+    }
+
     @Override
     public String toString() {
         return "LogDirDescription(" +
@@ -89,6 +109,7 @@ public class LogDirDescription {
                 ", error=" + error +
                 ", totalBytes=" + totalBytes +
                 ", usableBytes=" + usableBytes +
+                ", isCordoned=" + isCordoned +
                 ')';
     }
 }
diff --git a/clients/src/main/resources/common/message/BrokerHeartbeatRequest.json b/clients/src/main/resources/common/message/BrokerHeartbeatRequest.json
index 8f574b41fc..0465d7b33e 100644
--- a/clients/src/main/resources/common/message/BrokerHeartbeatRequest.json
+++ b/clients/src/main/resources/common/message/BrokerHeartbeatRequest.json
@@ -18,7 +18,9 @@
   "type": "request",
   "listeners": ["controller"],
   "name": "BrokerHeartbeatRequest",
-  "validVersions": "0-1",
+  // Version 1 adds OfflineLogDirs
+  // Version 2 adds CordonedLogDirs
+  "validVersions": "0-2",
   "flexibleVersions": "0+",
   "fields": [
     { "name": "BrokerId", "type": "int32", "versions": "0+", "entityType": "brokerId",
@@ -32,6 +34,9 @@
     { "name": "WantShutDown", "type": "bool", "versions": "0+",
       "about": "True if the broker wants to be shut down, false otherwise." },
     { "name": "OfflineLogDirs", "type":  "[]uuid", "versions": "1+", "taggedVersions": "1+", "tag": 0,
-      "about": "Log directories that failed and went offline." }
+      "about": "Log directories that failed and went offline." },
+    { "name": "CordonedLogDirs", "type": "[]uuid", "versions": "2+", "nullableVersions": "2+", "default": "null",
+      "taggedVersions": "2+", "tag": 1, "ignorable": true,
+      "about": "Log directories that are cordoned, or null if the broker does not report its cordoned log directories yet." }
   ]
 }
diff --git a/clients/src/main/resources/common/message/DescribeLogDirsResponse.json b/clients/src/main/resources/common/message/DescribeLogDirsResponse.json
index 725d1ad337..28b842604c 100644
--- a/clients/src/main/resources/common/message/DescribeLogDirsResponse.json
+++ b/clients/src/main/resources/common/message/DescribeLogDirsResponse.json
@@ -22,6 +22,7 @@
   // Version 2 is the first flexible version.
   // Version 3 adds the top-level ErrorCode field
   // Version 4 adds the TotalBytes and UsableBytes fields
+  // The IsCordoned tagged field was added to version 4 without a version bump
   "validVersions": "1-4",
   "flexibleVersions": "2+",
   "fields": [
@@ -55,6 +56,9 @@
       },
       { "name": "UsableBytes", "type": "int64", "versions": "4+", "ignorable": true, "default": "-1",
         "about": "The usable size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage."
+      },
+      { "name": "IsCordoned", "type": "bool", "versions": "4+", "taggedVersions": "4+", "tag": 0, "default": "false",
+        "about": "True if this log directory is cordoned."
       }
     ]}
   ]
diff --git a/core/src/main/scala/kafka/log/LogManager.scala b/core/src/main/scala/kafka/log/LogManager.scala
index efdab2a0f3..d997281f52 100755
--- a/core/src/main/scala/kafka/log/LogManager.scala
+++ b/core/src/main/scala/kafka/log/LogManager.scala
@@ -38,6 +38,7 @@ import org.apache.kafka.metadata.ConfigRepository
 import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, PropertiesUtils}
 
 import java.util.{Collections, Optional, OptionalLong, Properties}
+import org.apache.kafka.server.config.ServerLogConfigs
 import org.apache.kafka.server.metrics.KafkaMetricsGroup
 import org.apache.kafka.server.util.{FileLock, Scheduler}
 import org.apache.kafka.storage.internals.log.{CleanerConfig, LogCleaner, LogConfig, LogDirFailureChannel, LogManager => JLogManager, LogOffsetsListener, ProducerStateManagerConfig, RemoteIndexCache, UnifiedLog}
@@ -153,14 +154,53 @@ class LogManager(logDirs: Seq[File],
   @volatile private var _cleaner: LogCleaner = _
   private[kafka] def cleaner: LogCleaner = _cleaner
 
+  // The absolute paths of the cordoned log directories, as configured by cordoned.log.dirs.
+  @volatile private var configuredCordonedLogDirs: Predef.Set[String] = Predef.Set.empty
+  // Cordoning is only effective once the metadata version supports it (KIP-1066).
+  @volatile private var cordoningSupported: () => Boolean = () => true
+
   metricsGroup.newGauge("OfflineLogDirectoryCount", () => offlineLogDirs.size)
+  metricsGroup.newGauge("CordonedLogDirectoryCount", () => logDirs.count(dir => isLogDirCordoned(dir.getAbsolutePath)))
 
   for (dir <- logDirs) {
     metricsGroup.newGauge("LogDirectoryOffline",
       () => if (_liveLogDirs.contains(dir)) 0 else 1,
       Map("logDirectory" -> dir.getAbsolutePath).asJava)
+    metricsGroup.newGauge("LogDirectoryCordoned",
+      () => if (isLogDirCordoned(dir.getAbsolutePath)) 1 else 0,
+      Map("logDirectory" -> dir.getAbsolutePath).asJava)
+  }
+
+  /**
+   * Sets the supplier telling whether cordoning log directories is supported by the current metadata version.
+   * While it returns false, no log directory is considered cordoned.
+   */
+  def setCordoningSupported(supported: () => Boolean): Unit = {
+    cordoningSupported = supported
+  }
+
+  /**
+   * Updates the cordoned log directories.
+   *
+   * @param cordonedDirs the value of cordoned.log.dirs: log directories, or "*" for all of them.
+   */
+  def updateCordonedLogDirs(cordonedDirs: util.List[String]): Unit = {
+    configuredCordonedLogDirs =
+      if (cordonedDirs.contains(ServerLogConfigs.CORDONED_LOG_DIRS_ALL)) logDirs.map(_.getAbsolutePath).toSet
+      else cordonedDirs.asScala.map(new File(_).getAbsolutePath).toSet
   }
 
+  /**
+   * Whether the log directory, given its absolute path, is cordoned.
+   */
+  def isLogDirCordoned(logDir: String): Boolean = cordoningSupported() && configuredCordonedLogDirs.contains(logDir)
+
+  /**
+   * The IDs of the online log directories that are cordoned.
+   */
+  def cordonedDirectoryIds: Predef.Set[Uuid] =
+    directoryIds.collect { case (path, id) if isLogDirCordoned(path) => id }.toSet
+
   /**
    * Create and check validity of the given directories that are not in the given offline directories, specifically:
    * <ol>
@@ -647,8 +687,10 @@ class LogManager(logDirs: Seq[File],
     info("Shutting down.")
 
     metricsGroup.removeMetric("OfflineLogDirectoryCount")
+    metricsGroup.removeMetric("CordonedLogDirectoryCount")
     for (dir <- logDirs) {
       metricsGroup.removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath).asJava)
+      metricsGroup.removeMetric("LogDirectoryCordoned", Map("logDirectory" -> dir.getAbsolutePath).asJava)
     }
 
     val threadPools = ArrayBuffer.empty[ExecutorService]
@@ -1021,7 +1063,7 @@ class LogManager(logDirs: Seq[File],
           throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")
 
         val logDirs: List[File] = {
-          val preferredLogDir = targetLogDirectoryId.filterNot(Seq(DirectoryId.UNASSIGNED,DirectoryId.LOST).contains) match {
+          val suggestedLogDir = targetLogDirectoryId.filterNot(Seq(DirectoryId.UNASSIGNED,DirectoryId.LOST).contains) match {
             case Some(targetId) if !preferredLogDirs.containsKey(topicPartition) =>
               // If partition is configured with both targetLogDirectoryId and preferredLogDirs, then
               // preferredLogDirs will be respected, otherwise targetLogDirectoryId will be respected
@@ -1029,6 +1071,13 @@ class LogManager(logDirs: Seq[File],
             case _ =>
               preferredLogDirs.get(topicPartition)
           }
+          // A cordoned log directory is never picked for a new log, whichever directory was suggested.
+          val preferredLogDir = if (suggestedLogDir != null && isLogDirCordoned(suggestedLogDir)) {
+            warn(s"Ignoring the cordoned log directory $suggestedLogDir suggested for $topicPartition")
+            null
+          } else {
+            suggestedLogDir
+          }
 
           if (isFuture) {
             if (preferredLogDir == null)
@@ -1367,13 +1416,14 @@ class LogManager(logDirs: Seq[File],
    * data directories by fewest partitions.
    */
   private def nextLogDirs(): List[File] = {
-    if (_liveLogDirs.size == 1) {
-      List(_liveLogDirs.peek())
+    val uncordonedLogDirs = _liveLogDirs.asScala.filterNot(dir => isLogDirCordoned(dir.getAbsolutePath))
+    if (uncordonedLogDirs.size == 1) {
+      List(uncordonedLogDirs.head)
     } else {
       // count the number of logs in each parent directory (including 0 for empty directories
       val logCounts = allLogs.groupBy(_.parentDir).map { case (parent, logs) => parent -> logs.size }
-      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
-      val dirCounts = (zeros ++ logCounts).toBuffer
+      val zeros = uncordonedLogDirs.map(dir => (dir.getPath, 0)).toMap
+      val dirCounts = (zeros ++ logCounts.filter { case (parent, _) => zeros.contains(parent) }).toBuffer
 
       // choose the directory with the least logs in it
       dirCounts.sortBy(_._2).map {
@@ -1545,7 +1595,7 @@ object LogManager {
     val cleanerConfig = new CleanerConfig(config)
     val transactionLogConfig = new TransactionLogConfig(config)
 
-    new LogManager(logDirs = config.logDirs.asScala.map(new File(_).getAbsoluteFile),
+    val logManager = new LogManager(logDirs = config.logDirs.asScala.map(new File(_).getAbsoluteFile),
       initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
       configRepository = configRepository,
       initialDefaultConfig = defaultLogConfig,
@@ -1564,5 +1614,7 @@ object LogManager {
       time = time,
       remoteStorageSystemEnable = config.remoteLogManagerConfig.isRemoteStorageSystemEnabled,
       initialTaskDelayMs = config.logInitialTaskDelayMs)
+    logManager.updateCordonedLogDirs(config.cordonedLogDirs)
+    logManager
   }
 }
diff --git a/core/src/main/scala/kafka/server/BrokerServer.scala b/core/src/main/scala/kafka/server/BrokerServer.scala
index 977eca37a8..79aa6e630a 100644
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
@@ -224,6 +224,10 @@ class BrokerServer(
         logManager.directoryIdsSet.asJava,
         () => new Thread(() => shutdown(), "kafka-shutdown-thread").start())
 
+      // Cordoning log directories requires a metadata version that supports it (KIP-1066).
+      logManager.setCordoningSupported(() => metadataCache.metadataVersion().isAtLeast(MetadataVersion.IBP_4_3_IV0))
+      lifecycleManager.setCordonedLogDirsSupplier(() => new util.ArrayList(logManager.cordonedDirectoryIds.asJava))
+
       // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
       // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
       tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
diff --git a/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala b/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala
index a467f18ace..20d64b86f5 100755
--- a/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala
+++ b/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala
@@ -196,6 +196,7 @@ class DynamicBrokerConfig(private val kafkaConfig: KafkaConfig) extends Logging
 
     addBrokerReconfigurable(new BrokerDynamicThreadPool(kafkaServer))
     addBrokerReconfigurable(new DynamicLogConfig(kafkaServer.logManager))
+    addBrokerReconfigurable(new DynamicCordonedLogDirsConfig(kafkaServer.logManager))
     addBrokerReconfigurable(new DynamicListenerConfig(kafkaServer))
     addBrokerReconfigurable(kafkaServer.socketServer)
     addBrokerReconfigurable(new DynamicProducerStateManagerConfig(kafkaServer.logManager.producerStateManagerConfig))
@@ -604,6 +605,21 @@ class DynamicLogConfig(logManager: LogManager) extends BrokerReconfigurable with
   }
 }
 
+class DynamicCordonedLogDirsConfig(logManager: LogManager) extends BrokerReconfigurable {
+
+  override def reconfigurableConfigs: util.Set[String] = {
+    JDynamicBrokerConfig.DynamicCordonedLogDirsConfig.RECONFIGURABLE_CONFIGS
+  }
+
+  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
+    // The cordoned log directories are validated against log.dirs when the new KafkaConfig is created.
+  }
+
+  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
+    logManager.updateCordonedLogDirs(newConfig.cordonedLogDirs)
+  }
+}
+
 class ControllerDynamicThreadPool(controller: ControllerServer) extends BrokerReconfigurable {
 
   override def reconfigurableConfigs: util.Set[String] = {
diff --git a/core/src/main/scala/kafka/server/KafkaConfig.scala b/core/src/main/scala/kafka/server/KafkaConfig.scala
index d05ef90e6f..2f11bc03ff 100755
--- a/core/src/main/scala/kafka/server/KafkaConfig.scala
+++ b/core/src/main/scala/kafka/server/KafkaConfig.scala
@@ -494,6 +494,12 @@ class KafkaConfig private(doLog: Boolean, val props: util.Map[_, _])
     require(logRollTimeJitterMillis >= 0, "log.roll.jitter.ms must be greater than or equal to 0")
     require(logRetentionTimeMillis >= 1 || logRetentionTimeMillis == -1, "log.retention.ms must be unlimited (-1) or, greater than or equal to 1")
     require(logDirs.size > 0, "At least one log directory must be defined via log.dirs or log.dir.")
+    val logDirPaths = logDirs.asScala.map(new java.io.File(_).getAbsolutePath).toSet
+    cordonedLogDirs.asScala.foreach { dir =>
+      require(dir == ServerLogConfigs.CORDONED_LOG_DIRS_ALL || logDirPaths.contains(new java.io.File(dir).getAbsolutePath),
+        s"${ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG} contains $dir which is not listed in log.dirs or log.dir, " +
+          s"it must be one of ${logDirs.asScala.mkString(",")} or '${ServerLogConfigs.CORDONED_LOG_DIRS_ALL}'.")
+    }
     require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.")
     require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
       " to prevent unnecessary socket timeouts")
diff --git a/core/src/main/scala/kafka/server/ReplicaManager.scala b/core/src/main/scala/kafka/server/ReplicaManager.scala
index d2471b2ecd..2ff801b7ff 100644
--- a/core/src/main/scala/kafka/server/ReplicaManager.scala
+++ b/core/src/main/scala/kafka/server/ReplicaManager.scala
@@ -1117,6 +1117,8 @@ class ReplicaManager(val config: KafkaConfig,
             throw new InvalidTopicException("The topic name is too long.")
           if (!logManager.isLogDirOnline(destinationDir))
             throw new KafkaStorageException(s"Log directory $destinationDir is offline")
+          if (logManager.isLogDirCordoned(destinationDir))
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
+          .setIsCordoned(logManager.isLogDirCordoned(absolutePath))
         describeLogDirsResult
 
       } catch {
diff --git a/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java b/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java
index 4a355c206b..d9606aa276 100644
--- a/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java
+++ b/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java
@@ -617,6 +617,7 @@ public class ClusterControlManager {
             record.epoch(),
             BrokerRegistrationFencingChange.FENCE.asBoolean(),
             BrokerRegistrationInControlledShutdownChange.NONE.asBoolean(),
+            Optional.empty(),
             Optional.empty()
         );
     }
@@ -628,6 +629,7 @@ public class ClusterControlManager {
             record.epoch(),
             BrokerRegistrationFencingChange.UNFENCE.asBoolean(),
             BrokerRegistrationInControlledShutdownChange.NONE.asBoolean(),
+            Optional.empty(),
             Optional.empty()
         );
     }
@@ -648,7 +650,8 @@ public class ClusterControlManager {
             record.brokerEpoch(),
             fencingChange.asBoolean(),
             inControlledShutdownChange.asBoolean(),
-            directoriesChange
+            directoriesChange,
+            Optional.ofNullable(record.cordonedLogDirs())
         );
     }
 
@@ -658,7 +661,8 @@ public class ClusterControlManager {
         long brokerEpoch,
         Optional<Boolean> fencingChange,
         Optional<Boolean> inControlledShutdownChange,
-        Optional<List<Uuid>> directoriesChange
+        Optional<List<Uuid>> directoriesChange,
+        Optional<List<Uuid>> cordonedDirectoriesChange
     ) {
         BrokerRegistration curRegistration = brokerRegistrations.get(brokerId);
         if (curRegistration == null) {
@@ -671,7 +675,8 @@ public class ClusterControlManager {
             BrokerRegistration nextRegistration = curRegistration.cloneWith(
                 fencingChange,
                 inControlledShutdownChange,
-                directoriesChange
+                directoriesChange,
+                cordonedDirectoriesChange
             );
             if (!curRegistration.equals(nextRegistration)) {
                 log.info("Replayed {} modifying the registration for broker {}: {}",
@@ -704,8 +709,21 @@ public class ClusterControlManager {
         if (heartbeatManager == null) {
             throw new RuntimeException("ClusterControlManager is not active.");
         }
-        return heartbeatManager.usableBrokers(
-            id -> brokerRegistrations.get(id).rack());
+        // Brokers whose directories are all cordoned must not receive new partitions.
+        List<UsableBroker> usableBrokers = new ArrayList<>();
+        heartbeatManager.usableBrokers(id -> brokerRegistrations.get(id).rack()).forEachRemaining(broker -> {
+            if (brokerRegistrations.get(broker.id()).hasUncordonedDir()) {
+                usableBrokers.add(broker);
+            }
+        });
+        return usableBrokers.iterator();
+    }
+
+    /**
+     * Returns true if the registered broker has at least one online directory which is not cordoned.
+     */
+    public boolean hasUncordonedDir(int brokerId) {
+        return brokerRegistrations.get(brokerId).hasUncordonedDir();
     }
 
     /**
diff --git a/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java b/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
index 4a0219d175..96fded2e22 100644
--- a/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
+++ b/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
@@ -33,6 +33,7 @@ import org.apache.kafka.common.utils.LogContext;
 import org.apache.kafka.metadata.KafkaConfigSchema;
 import org.apache.kafka.server.common.ApiMessageAndVersion;
 import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
+import org.apache.kafka.server.common.MetadataVersion;
 import org.apache.kafka.server.mutable.BoundedList;
 import org.apache.kafka.server.policy.AlterConfigPolicy;
 import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
@@ -60,6 +61,7 @@ import static org.apache.kafka.common.config.TopicConfig.UNCLEAN_LEADER_ELECTION
 import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;
 import static org.apache.kafka.common.protocol.Errors.INVALID_CONFIG;
 import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;
+import static org.apache.kafka.server.config.ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG;
 
 
 public class ConfigurationControlManager {
@@ -338,6 +340,8 @@ public class ConfigurationControlManager {
                 return DISALLOWED_CLUSTER_MIN_ISR_REMOVAL_ERROR;
             } else if (configRecord.value() == null) {
                 allConfigs.remove(configRecord.name());
+            } else if (isUnsupportedCordonedLogDirs(configRecord)) {
+                return CORDONED_LOG_DIRS_UNSUPPORTED_ERROR;
             } else if (configRecord.value().length() > Short.MAX_VALUE) {
                 // In KRaft mode, large config values cannot be created by appending.
                 // If the size exceeds Short.MAX_VALUE, this error will be thrown to notify the user.
@@ -390,6 +394,16 @@ public class ConfigurationControlManager {
         new ApiError(INVALID_CONFIG, "The configuration value cannot be added because " +
             "it exceeds the maximum value size of " + Short.MAX_VALUE + " bytes.");
 
+    private static final ApiError CORDONED_LOG_DIRS_UNSUPPORTED_ERROR =
+        new ApiError(INVALID_CONFIG, "The configuration " + CORDONED_LOG_DIRS_CONFIG +
+            " cannot be set because the metadata version is below " + MetadataVersion.IBP_4_3_IV0 + ".");
+
+    boolean isUnsupportedCordonedLogDirs(ConfigRecord configRecord) {
+        return configRecord.name().equals(CORDONED_LOG_DIRS_CONFIG) &&
+            configRecord.resourceType() == BROKER.id() &&
+            !featureControl.metadataVersionOrThrow().isAtLeast(MetadataVersion.IBP_4_3_IV0);
+    }
+
     boolean isDisallowedBrokerMinIsrTransition(ConfigRecord configRecord) {
         if (configRecord.name().equals(MIN_IN_SYNC_REPLICAS_CONFIG) &&
                 configRecord.resourceType() == BROKER.id() &&
diff --git a/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java b/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java
index 316213bbc0..10ae2a04c8 100644
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
@@ -737,6 +739,12 @@ public class ReplicationControlManager {
                 }
                 PartitionAssignment partitionAssignment = new PartitionAssignment(assignment.brokerIds(), clusterDescriber);
                 validateManualPartitionAssignment(partitionAssignment, replicationFactor);
+                List<Integer> cordonedBrokers = brokersWithoutUncordonedDir(assignment.brokerIds());
+                if (!cordonedBrokers.isEmpty()) {
+                    return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
+                        "The manual partition assignment for partition " + assignment.partitionIndex() +
+                            " includes brokers " + cordonedBrokers + " whose log directories are all cordoned.");
+                }
                 replicationFactor = OptionalInt.of(assignment.brokerIds().size());
                 List<Integer> isr = assignment.brokerIds().stream().
                     filter(clusterControl::isActive).toList();
@@ -1531,6 +1539,36 @@ public class ReplicationControlManager {
         }
     }
 
+    /**
+     * Generates a BrokerRegistrationChangeRecord if the cordoned directories reported by the broker
+     * differ from the ones in its registration.
+     *
+     * @param brokerId       The broker id.
+     * @param brokerEpoch    The broker epoch.
+     * @param cordonedDirs   The directories reported as cordoned, or null if the broker did not report them.
+     * @param records        The record list to append to.
+     */
+    void handleCordonedDirectories(
+        int brokerId,
+        long brokerEpoch,
+        List<Uuid> cordonedDirs,
+        List<ApiMessageAndVersion> records
+    ) {
+        if (cordonedDirs == null) {
+            return;
+        }
+        BrokerRegistration registration = clusterControl.registration(brokerId);
+        List<Uuid> newCordonedDirs = registration.directoryIntersection(cordonedDirs);
+        if (!newCordonedDirs.equals(registration.cordonedDirectories())) {
+            records.add(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
+                setBrokerId(brokerId).setBrokerEpoch(brokerEpoch).
+                setCordonedLogDirs(newCordonedDirs),
+                (short) 3));
+            log.info("Cordoned directories in broker {} changed from {} to {}",
+                brokerId, registration.cordonedDirectories(), newCordonedDirs);
+        }
+    }
+
     ControllerResult<ElectLeadersResponseData> electLeaders(ElectLeadersRequestData request) {
         ElectionType electionType = electionType(request.electionType());
         List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
@@ -1667,6 +1705,9 @@ public class ReplicationControlManager {
         if (featureControl.metadataVersionOrThrow().isDirectoryAssignmentSupported()) {
             handleDirectoriesOffline(brokerId, brokerEpoch, request.offlineLogDirs(), records);
         }
+        if (featureControl.metadataVersionOrThrow().isAtLeast(MetadataVersion.IBP_4_3_IV0)) {
+            handleCordonedDirectories(brokerId, brokerEpoch, request.cordonedLogDirs(), records);
+        }
         boolean isCaughtUp = request.currentMetadataOffset() >= registerBrokerRecordOffset;
         BrokerHeartbeatReply reply = new BrokerHeartbeatReply(isCaughtUp,
                 states.next().fenced(),
@@ -1928,6 +1969,12 @@ public class ReplicationControlManager {
                 List<Integer> replicas = topic.assignments().get(i).brokerIds();
                 PartitionAssignment partitionAssignment = new PartitionAssignment(replicas, clusterDescriber);
                 validateManualPartitionAssignment(partitionAssignment, OptionalInt.of(replicationFactor));
+                List<Integer> cordonedBrokers = brokersWithoutUncordonedDir(replicas);
+                if (!cordonedBrokers.isEmpty()) {
+                    throw new InvalidReplicaAssignmentException("The manual partition assignment for " +
+                        "partition " + (startPartitionId + i) + " includes brokers " + cordonedBrokers +
+                        " whose log directories are all cordoned.");
+                }
                 partitionAssignments.add(partitionAssignment);
                 List<Integer> isr = partitionAssignment.replicas().stream().
                     filter(clusterControl::isActive).toList();
@@ -1966,6 +2013,14 @@ public class ReplicationControlManager {
         }
     }
 
+    /**
+     * Returns the registered brokers among the given ones that cannot receive new replicas
+     * because all of their log directories are cordoned.
+     */
+    private List<Integer> brokersWithoutUncordonedDir(List<Integer> brokerIds) {
+        return brokerIds.stream().filter(id -> !clusterControl.hasUncordonedDir(id)).toList();
+    }
+
     void validateManualPartitionAssignment(
         PartitionAssignment assignment,
         OptionalInt replicationFactor
@@ -2234,6 +2289,13 @@ public class ReplicationControlManager {
         PartitionAssignment targetAssignment = new PartitionAssignment(target.replicas(), clusterDescriber);
 
         validateManualPartitionAssignment(targetAssignment, OptionalInt.empty());
+        // Replicas which are already assigned to the partition may stay on a broker with only cordoned directories.
+        List<Integer> addedReplicas = target.replicas().stream().filter(id -> !Replicas.contains(part.replicas, id)).toList();
+        List<Integer> cordonedBrokers = brokersWithoutUncordonedDir(addedReplicas);
+        if (!cordonedBrokers.isEmpty()) {
+            throw new IneligibleReplicaException("The target replicas include brokers " + cordonedBrokers +
+                " whose log directories are all cordoned.");
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
index 56b2bdf375..1a4251a2da 100644
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
@@ -260,6 +274,21 @@ public class BrokerRegistration {
         return directories;
     }
 
+    /**
+     * The directories of this broker that are cordoned: no new partition is placed on them.
+     */
+    public List<Uuid> cordonedDirectories() {
+        return cordonedDirectories;
+    }
+
+    /**
+     * Whether this broker has at least one online directory that is not cordoned, so it can receive new partitions.
+     * A broker that did not register any directory is considered to have an uncordoned directory.
+     */
+    public boolean hasUncordonedDir() {
+        return directories.isEmpty() || directories.stream().anyMatch(dir -> !cordonedDirectories.contains(dir));
+    }
+
     public boolean hasOnlineDir(Uuid dir) {
         return DirectoryId.isOnline(dir, directories);
     }
@@ -330,7 +359,7 @@ public class BrokerRegistration {
     @Override
     public int hashCode() {
         return Objects.hash(id, epoch, incarnationId, listeners, supportedFeatures,
-            rack, fenced, inControlledShutdown, isMigratingZkBroker, directories);
+            rack, fenced, inControlledShutdown, isMigratingZkBroker, directories, cordonedDirectories);
     }
 
     @Override
@@ -345,7 +374,8 @@ public class BrokerRegistration {
             other.fenced == fenced &&
             other.inControlledShutdown == inControlledShutdown &&
             other.isMigratingZkBroker == isMigratingZkBroker &&
-            other.directories.equals(directories);
+            other.directories.equals(directories) &&
+            other.cordonedDirectories.equals(cordonedDirectories);
     }
 
     @Override
@@ -374,12 +404,23 @@ public class BrokerRegistration {
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
+        if (newFenced == fenced && newInControlledShutdownChange == inControlledShutdown &&
+            newDirectories.equals(directories) && newCordonedDirectories.equals(cordonedDirectories))
             return this;
 
         return new BrokerRegistration(
@@ -392,7 +433,8 @@ public class BrokerRegistration {
             newFenced,
             newInControlledShutdownChange,
             isMigratingZkBroker,
-            newDirectories
+            newDirectories,
+            newCordonedDirectories
         );
     }
 }
diff --git a/metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json b/metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json
index 7a484a6aeb..7f434d0b27 100644
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
+   { "name": "CordonedLogDirs", "type": "[]uuid", "versions": "3+", "nullableVersions": "3+", "default": "null",
+     "taggedVersions": "3+", "tag": 3,
+     "about": "Log directories configured in this broker which are cordoned, or null if there is no change." }
   ]
 }
diff --git a/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java b/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java
index d0e1c60d6d..6a2917c6c1 100644
--- a/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java
+++ b/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java
@@ -40,6 +40,15 @@ public class ServerLogConfigs {
     public static final String LOG_DIR_DOC = "A comma-separated list of the directories where the log data is stored. (supplemental to " + LOG_DIRS_CONFIG + " property)";
     public static final String LOG_DIRS_DOC = "A comma-separated list of the directories where the log data is stored. If not set, the value in " + LOG_DIR_CONFIG + " is used.";
 
+    public static final String CORDONED_LOG_DIRS_CONFIG = "cordoned.log.dirs";
+    public static final String CORDONED_LOG_DIRS_ALL = "*";
+    public static final String CORDONED_LOG_DIRS_DEFAULT = "";
+    public static final String CORDONED_LOG_DIRS_DOC = "A comma-separated list of the directories that are cordoned. " +
+        "New partitions are not placed on cordoned directories, while the existing partitions they host keep being served. " +
+        "Entries must be directories listed in " + LOG_DIRS_CONFIG + " (or " + LOG_DIR_CONFIG + "). " +
+        "The special value '" + CORDONED_LOG_DIRS_ALL + "' cordons all of them. " +
+        "This configuration requires metadata version 4.3-IV0 or higher, it has no effect otherwise.";
+
     public static final String LOG_SEGMENT_BYTES_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_BYTES_CONFIG);
     public static final String LOG_SEGMENT_BYTES_DOC = "The maximum size of a single log file";
 
diff --git a/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java b/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java
index e148fb1c0b..101b7303c3 100644
--- a/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java
+++ b/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java
@@ -206,6 +206,12 @@ public class BrokerLifecycleManager {
      */
     private volatile OptionalLong previousBrokerEpoch = OptionalLong.empty();
 
+    /**
+     * Provides the cordoned log directories to report to the controller in heartbeats.
+     * It is evaluated on the event queue thread.
+     */
+    private volatile Supplier<List<Uuid>> cordonedLogDirsSupplier = List::of;
+
     public BrokerLifecycleManager(
             AbstractKafkaConfig config,
             Time time,
@@ -257,6 +263,14 @@ public class BrokerLifecycleManager {
                 channelManager, clusterId, advertisedListeners, supportedFeatures));
     }
 
+    /**
+     * Sets the provider of the cordoned log directories, which are reported to the controller
+     * in each heartbeat sent once the broker has caught up with the metadata log.
+     */
+    public void setCordonedLogDirsSupplier(Supplier<List<Uuid>> cordonedLogDirsSupplier) {
+        this.cordonedLogDirsSupplier = cordonedLogDirsSupplier;
+    }
+
     public CompletableFuture<Void> setReadyToUnfence() {
         eventQueue.append(new SetReadyToUnfenceEvent());
         return initialUnfenceFuture;
@@ -532,6 +546,11 @@ public class BrokerLifecycleManager {
             .setWantFence(!readyToUnfence)
             .setWantShutDown(state == BrokerState.PENDING_CONTROLLED_SHUTDOWN)
             .setOfflineLogDirs(new ArrayList<>(offlineDirs.keySet()));
+        // Until the broker has caught up with the metadata log, its dynamic configs may be stale,
+        // so the cordoned log directories are left unset, which means no change to the controller.
+        if (state != BrokerState.STARTING) {
+            data.setCordonedLogDirs(cordonedLogDirsSupplier.get());
+        }
         if (logger.isTraceEnabled()) {
             logger.trace("Sending broker heartbeat {}", data);
         }
diff --git a/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java b/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
index bfec3337ca..9e71b7714e 100644
--- a/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
+++ b/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
@@ -81,7 +81,10 @@ public abstract class AbstractKafkaConfig extends AbstractConfig {
         QuotaConfig.CONFIG_DEF,
         BrokerSecurityConfigs.CONFIG_DEF,
         DelegationTokenManagerConfigs.CONFIG_DEF,
-        AddPartitionsToTxnConfig.CONFIG_DEF
+        AddPartitionsToTxnConfig.CONFIG_DEF,
+        new ConfigDef().define(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, ConfigDef.Type.LIST,
+            ServerLogConfigs.CORDONED_LOG_DIRS_DEFAULT, ConfigDef.ValidList.anyNonDuplicateValues(true, false),
+            ConfigDef.Importance.LOW, ServerLogConfigs.CORDONED_LOG_DIRS_DOC)
     ));
 
     public AbstractKafkaConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps, boolean doLog) {
@@ -92,6 +95,10 @@ public abstract class AbstractKafkaConfig extends AbstractConfig {
         return Optional.ofNullable(getList(ServerLogConfigs.LOG_DIRS_CONFIG)).orElse(getList(ServerLogConfigs.LOG_DIR_CONFIG));
     }
 
+    public List<String> cordonedLogDirs() {
+        return getList(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG);
+    }
+
     public int numIoThreads() {
         return getInt(ServerConfigs.NUM_IO_THREADS_CONFIG);
     }
diff --git a/server/src/main/java/org/apache/kafka/server/config/DynamicBrokerConfig.java b/server/src/main/java/org/apache/kafka/server/config/DynamicBrokerConfig.java
index 4ccd3005b7..6416e23310 100644
--- a/server/src/main/java/org/apache/kafka/server/config/DynamicBrokerConfig.java
+++ b/server/src/main/java/org/apache/kafka/server/config/DynamicBrokerConfig.java
@@ -61,7 +61,8 @@ public class DynamicBrokerConfig {
 
     private static final Set<String> PER_BROKER_CONFIGS = Stream.of(
             DYNAMIC_SECURITY_CONFIGS,
-            DynamicListenerConfig.RECONFIGURABLE_CONFIGS)
+            DynamicListenerConfig.RECONFIGURABLE_CONFIGS,
+            DynamicCordonedLogDirsConfig.RECONFIGURABLE_CONFIGS)
         .flatMap(Collection::stream)
         .filter(c -> !CLUSTER_LEVEL_LISTENER_CONFIGS.contains(c))
         .collect(Collectors.toUnmodifiableSet());
@@ -73,6 +74,7 @@ public class DynamicBrokerConfig {
             DynamicThreadPool.RECONFIGURABLE_CONFIGS,
             List.of(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG),
             DynamicListenerConfig.RECONFIGURABLE_CONFIGS,
+            DynamicCordonedLogDirsConfig.RECONFIGURABLE_CONFIGS,
             SocketServer.RECONFIGURABLE_CONFIGS,
             DYNAMIC_PRODUCER_STATE_MANAGER_CONFIGS,
             DynamicRemoteLogConfig.RECONFIGURABLE_CONFIGS,
@@ -209,6 +211,11 @@ public class DynamicBrokerConfig {
                 ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.values());
     }
 
+    public static class DynamicCordonedLogDirsConfig {
+        public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
+                ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG);
+    }
+
     public static class DynamicListenerConfig {
         /**
          * The set of configurations which the DynamicListenerConfig object listens for. Many of
diff --git a/server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java b/server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java
new file mode 100644
index 0000000000..ed58ca5b61
--- /dev/null
+++ b/server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java
@@ -0,0 +1,260 @@
+/*
+ * Licensed to the Apache Software Foundation (ASF) under one or more
+ * contributor license agreements. See the NOTICE file distributed with
+ * this work for additional information regarding copyright ownership.
+ * The ASF licenses this file to You under the Apache License, Version 2.0
+ * (the "License"); you may not use this file except in compliance with
+ * the License. You may obtain a copy of the License at
+ *
+ *    http://www.apache.org/licenses/LICENSE-2.0
+ *
+ * Unless required by applicable law or agreed to in writing, software
+ * distributed under the License is distributed on an "AS IS" BASIS,
+ * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+ * See the License for the specific language governing permissions and
+ * limitations under the License.
+ */
+package org.apache.kafka.server;
+
+import kafka.server.KafkaBroker;
+
+import org.apache.kafka.clients.admin.Admin;
+import org.apache.kafka.clients.admin.AlterConfigOp;
+import org.apache.kafka.clients.admin.ConfigEntry;
+import org.apache.kafka.clients.admin.LogDirDescription;
+import org.apache.kafka.clients.admin.NewPartitionReassignment;
+import org.apache.kafka.clients.admin.NewPartitions;
+import org.apache.kafka.clients.admin.NewTopic;
+import org.apache.kafka.common.KafkaFuture;
+import org.apache.kafka.common.TopicPartition;
+import org.apache.kafka.common.TopicPartitionReplica;
+import org.apache.kafka.common.Uuid;
+import org.apache.kafka.common.config.ConfigResource;
+import org.apache.kafka.common.errors.IneligibleReplicaException;
+import org.apache.kafka.common.errors.InvalidConfigurationException;
+import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
+import org.apache.kafka.common.errors.InvalidReplicationFactorException;
+import org.apache.kafka.common.errors.InvalidRequestException;
+import org.apache.kafka.common.test.ClusterInstance;
+import org.apache.kafka.common.test.api.ClusterConfigProperty;
+import org.apache.kafka.common.test.api.ClusterTest;
+import org.apache.kafka.common.test.api.ClusterTestDefaults;
+import org.apache.kafka.common.test.api.Type;
+import org.apache.kafka.common.utils.Sanitizer;
+import org.apache.kafka.metadata.BrokerRegistration;
+import org.apache.kafka.metadata.KRaftMetadataCache;
+import org.apache.kafka.server.common.MetadataVersion;
+import org.apache.kafka.server.metrics.KafkaYammerMetrics;
+import org.apache.kafka.storage.internals.log.UnifiedLog;
+import org.apache.kafka.test.TestUtils;
+
+import com.yammer.metrics.core.Gauge;
+
+import java.io.File;
+import java.util.List;
+import java.util.Map;
+import java.util.Optional;
+import java.util.Set;
+import java.util.concurrent.ExecutionException;
+import java.util.stream.Collectors;
+
+import scala.Option;
+
+import static org.apache.kafka.server.config.ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG;
+import static org.junit.jupiter.api.Assertions.assertEquals;
+import static org.junit.jupiter.api.Assertions.assertFalse;
+import static org.junit.jupiter.api.Assertions.assertInstanceOf;
+import static org.junit.jupiter.api.Assertions.assertThrows;
+import static org.junit.jupiter.api.Assertions.assertTrue;
+
+@ClusterTestDefaults(types = {Type.KRAFT}, brokers = 3, disksPerBroker = 2)
+public class CordonedLogDirsIntegrationTest {
+    private static final long WAIT_MS = 60_000;
+
+    private final ClusterInstance cluster;
+
+    public CordonedLogDirsIntegrationTest(ClusterInstance cluster) {
+        this.cluster = cluster;
+    }
+
+    @ClusterTest
+    public void testCordonedBrokersAreNotUsedForNewPartitions() throws Exception {
+        try (Admin admin = cluster.admin()) {
+            // t0 is created while nothing is cordoned, with a replica on broker 0.
+            admin.createTopics(List.of(new NewTopic("t0", Map.of(0, List.of(0, 1))))).all().get();
+            cluster.waitTopicCreation("t0", 1);
+
+            // Cordon all the log directories of broker 0, which is then reported to the controller.
+            setCordonedLogDirs(admin, 0, "*");
+            waitForCordonedDirectoryCount(1, 0, 2);
+            assertTrue(describeLogDirs(admin, 0).values().stream().allMatch(LogDirDescription::isCordoned));
+            assertTrue(describeLogDirs(admin, 1).values().stream().noneMatch(LogDirDescription::isCordoned));
+
+            // Automatic placement does not use broker 0.
+            admin.createTopics(List.of(new NewTopic("t1", 6, (short) 2))).all().get();
+            cluster.waitTopicCreation("t1", 6);
+            assertNoReplicaOnBroker(admin, "t1", 0);
+
+            // A replication factor which needs broker 0 is rejected.
+            assertFailsWith(InvalidReplicationFactorException.class,
+                admin.createTopics(List.of(new NewTopic("t2", 1, (short) 3))).all());
+
+            // Explicit assignments onto broker 0 are rejected, for new topics and new partitions.
+            assertFailsWith(InvalidReplicaAssignmentException.class,
+                admin.createTopics(List.of(new NewTopic("t3", Map.of(0, List.of(0, 1))))).all());
+            assertFailsWith(InvalidReplicaAssignmentException.class,
+                admin.createPartitions(Map.of("t1", NewPartitions.increaseTo(7, List.of(List.of(0, 1))))).all());
+
+            // New partitions without explicit assignments do not use broker 0.
+            admin.createPartitions(Map.of("t1", NewPartitions.increaseTo(8))).all().get();
+            cluster.waitTopicCreation("t1", 8);
+            assertNoReplicaOnBroker(admin, "t1", 0);
+
+            // Moving a replica onto broker 0 is rejected.
+            assertFailsWith(IneligibleReplicaException.class, admin.alterPartitionReassignments(Map.of(
+                new TopicPartition("t1", 0), Optional.of(new NewPartitionReassignment(List.of(1, 0))))).all());
+
+            // A partition which already has a replica on broker 0 can be reassigned while keeping it there.
+            admin.alterPartitionReassignments(Map.of(
+                new TopicPartition("t0", 0), Optional.of(new NewPartitionReassignment(List.of(0, 2))))).all().get();
+
+            // Removing the entry uncordons the log directories.
+            removeCordonedLogDirs(admin, 0);
+            waitForCordonedDirectoryCount(1, 0, 0);
+            assertTrue(describeLogDirs(admin, 0).values().stream().noneMatch(LogDirDescription::isCordoned));
+            admin.createTopics(List.of(new NewTopic("t4", 1, (short) 3))).all().get();
+        }
+    }
+
+    @ClusterTest(brokers = 1)
+    public void testCordonedLogDirIsNotUsedForNewReplicas() throws Exception {
+        KafkaBroker broker = cluster.brokers().get(0);
+        List<String> dirs = broker.config().logDirs().stream().map(dir -> new File(dir).getAbsolutePath()).toList();
+        assertEquals(2, dirs.size());
+        String cordoned = dirs.get(0);
+        String uncordoned = dirs.get(1);
+
+        try (Admin admin = cluster.admin()) {
+            // A value which is not one of the log directories is rejected.
+            ExecutionException e = assertThrows(ExecutionException.class,
+                () -> setCordonedLogDirs(admin, 0, "/not/a/log/dir"));
+            assertInstanceOf(InvalidRequestException.class, e.getCause());
+            assertFalse(broker.logManager().isLogDirCordoned(cordoned));
+
+            setCordonedLogDirs(admin, 0, cordoned);
+            waitForCordonedDirectoryCount(0, 0, 1);
+            Map<String, LogDirDescription> descriptions = describeLogDirs(admin, 0);
+            assertTrue(descriptions.get(cordoned).isCordoned());
+            assertFalse(descriptions.get(uncordoned).isCordoned());
+
+            // The metrics reflect the state of each directory.
+            assertEquals(1, gaugeValue("LogDirectoryCordoned", cordoned));
+            assertEquals(0, gaugeValue("LogDirectoryCordoned", uncordoned));
+            assertEquals(1, gaugeValue("CordonedLogDirectoryCount", null));
+
+            // The new replicas are all placed in the uncordoned directory.
+            admin.createTopics(List.of(new NewTopic("p", 4, (short) 1))).all().get();
+            cluster.waitTopicCreation("p", 4);
+            descriptions = describeLogDirs(admin, 0);
+            assertEquals(0, partitionsOfTopic(descriptions.get(cordoned), "p"));
+            assertEquals(4, partitionsOfTopic(descriptions.get(uncordoned), "p"));
+
+            // Whichever directory is suggested for a new log, the cordoned directory is not picked.
+            Uuid cordonedId = broker.logManager().directoryId(cordoned).get();
+            UnifiedLog log = broker.logManager().getOrCreateLog(new TopicPartition("suggested", 0), true, false,
+                Optional.of(Uuid.randomUuid()), Option.apply(cordonedId));
+            assertEquals(uncordoned, log.parentDir());
+
+            // Moving a replica into the cordoned directory is rejected.
+            TopicPartitionReplica replica = new TopicPartitionReplica("p", 0, 0);
+            assertFailsWith(InvalidReplicaAssignmentException.class,
+                admin.alterReplicaLogDirs(Map.of(replica, cordoned)).values().get(replica));
+
+            // Once uncordoned, the directory can receive replicas again.
+            removeCordonedLogDirs(admin, 0);
+            waitForCordonedDirectoryCount(0, 0, 0);
+            assertEquals(0, gaugeValue("LogDirectoryCordoned", cordoned));
+            assertEquals(0, gaugeValue("CordonedLogDirectoryCount", null));
+            admin.alterReplicaLogDirs(Map.of(replica, cordoned)).all().get();
+        }
+    }
+
+    @ClusterTest(brokers = 1, metadataVersion = MetadataVersion.IBP_4_2_IV1, serverProperties = {
+        @ClusterConfigProperty(key = CORDONED_LOG_DIRS_CONFIG, value = "*")
+    })
+    public void testCordoningRequiresMetadataVersion() throws Exception {
+        try (Admin admin = cluster.admin()) {
+            // The dynamic configuration is rejected below 4.3-IV0.
+            ExecutionException e = assertThrows(ExecutionException.class,
+                () -> setCordonedLogDirs(admin, 0, "*"));
+            assertInstanceOf(InvalidConfigurationException.class, e.getCause());
+
+            // The static configuration has no effect.
+            assertTrue(describeLogDirs(admin, 0).values().stream().noneMatch(LogDirDescription::isCordoned));
+            admin.createTopics(List.of(new NewTopic("t", 2, (short) 1))).all().get();
+            cluster.waitTopicCreation("t", 2);
+        }
+    }
+
+    private static void setCordonedLogDirs(Admin admin, int brokerId, String value) throws Exception {
+        alterCordonedLogDirs(admin, brokerId, new AlterConfigOp(
+ 
... (truncated; 4313 more characters; read the files)
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
