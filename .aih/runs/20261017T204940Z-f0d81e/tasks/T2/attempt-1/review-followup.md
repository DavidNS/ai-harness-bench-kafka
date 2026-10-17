Your review is incomplete.

These behaviour criteria have no probe that counts: a probe that runs zero tests does not count (check the output names your test), and neither does one that passes without the change (the harness runs every passing probe on the base commit too; a test that asserts nothing, or only what already held, proves nothing about this change). For each one, write a NEW `aih_probe*` test that asserts the criterion from the outside and would fail if the change were reverted, run it with `aih check --probe <file> --run <its test names>`, fix it until it runs, and put it as `probe` (path named like a test file in a directory of the task's test files, full content) in that criterion's coverage entry. If a probe fails, that is a blocking finding with the probe as `proof`.
- ACC12: cordoned.log.dirs can be set dynamically per broker; values not in log.dirs (and not '*') are rejected; removing the entry uncordons. (no probe test was given)
- ACC13: Below 4.3-IV0, dynamic setting is rejected with a configuration error and static setting has no effect. (no probe test was given)
- ACC14: Heartbeats carry cordoned dirs after catch-up; controller reflects dynamic changes without restart. (no probe test was given)
- ACC15: Auto-placed creation avoids fully cordoned brokers and fails with INVALID_REPLICATION_FACTOR when too few remain. (no probe test was given)
- ACC16: Explicit assignment onto a fully cordoned broker returns INVALID_REPLICA_ASSIGNMENT. (no probe test was given)
- ACC17: Reassignment onto a fully cordoned broker returns INELIGIBLE_REPLICA. (no probe test was given)
- ACC18: AlterReplicaLogDirs to a cordoned directory returns INVALID_REPLICA_ASSIGNMENT. (no probe test was given)
- ACC19: Broker never creates a new replica in a cordoned directory even if the controller suggested it. (no probe test was given)
- ACC20: DescribeLogDirs returns per-directory cordoned state via LogDirDescription.isCordoned() and the tool prints it. (no probe test was given)
- ACC21: LogDirectoryCordoned and CordonedLogDirectoryCount metrics are registered and accurate. (no probe test was given)
- ACC22: Integration test cordons/uncordons via Admin on an embedded cluster and checks placement and errors. (no probe test was given)

Then return the whole review JSON again, with every entry.