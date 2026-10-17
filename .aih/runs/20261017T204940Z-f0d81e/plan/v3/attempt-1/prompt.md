You are planning a change in this repository. You can read files but not edit them or run
commands. Another session will implement your plan, one task at a time, each in a fresh context, and a
harness will check the result against it mechanically. Explore the repository as you see fit;
only your final JSON answer is used.

## Request

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

## Intents (use these ids exactly)

- `INTENT1` (config): Add broker config `cordoned.log.dirs` (list, default empty, low importance), settable statically and dynamically per broker via Admin/kafka-configs.sh, validated so entries are in log.dirs/log.dir or `*` (all), with removal uncordoning.
- `INTENT2` (mv-gating): Gate the feature on metadata version 4.3-IV0: below it, dynamic setting is rejected with a configuration error and a static setting has no effect.
- `INTENT3` (heartbeat): Brokers report their cordoned directories to the controller in broker heartbeats once caught up with metadata, so dynamic changes apply without restart.
- `INTENT4` (placement-auto): Topic and partition creation without explicit assignments uses only brokers with at least one uncordoned directory, failing with INVALID_REPLICATION_FACTOR if too few remain.
- `INTENT5` (placement-explicit): Explicit assignments onto a broker with no uncordoned directory fail with INVALID_REPLICA_ASSIGNMENT.
- `INTENT6` (reassignment): Partition reassignments onto a broker with no uncordoned directory fail with INELIGIBLE_REPLICA.
- `INTENT7` (alter-log-dirs): AlterReplicaLogDirs moving a replica onto a cordoned directory fails with INVALID_REPLICA_ASSIGNMENT.
- `INTENT8` (broker-dir-choice): The broker never selects a cordoned directory for a new replica, regardless of the directory the controller suggested.
- `INTENT9` (describe-log-dirs): DescribeLogDirs reports cordoned state per directory: LogDirDescription gets an isCordoned() accessor and the log dirs tool prints it.
- `INTENT10` (metrics): Add metrics kafka.log:type=LogManager,name=LogDirectoryCordoned,logDirectory=<path> (1/0) and kafka.log:type=LogManager,name=CordonedLogDirectoryCount.
- `INTENT11` (tests): Add an integration test on an embedded cluster that cordons/uncordons via Admin and checks partition placement and returned errors, plus unit tests for changed components.

## Constraints

- Existing test suites must stay green
- Do not change unrelated behaviour
- Feature requires metadata version 4.3-IV0 (already defined, not yet production)

## Repository facts

- Test files are those matching: server/src/test/java/**/*.java
- Tests are run by the harness, not by the implementer.

## Why the previous plan was rejected

ACC20 needs a test of LogDirsCommand's printed 'isCordoned' field. LogDirsCommand is in the tools module, not reachable from server-module tests, and the scope hook blocks creating or editing any tools test file. Add a tools test file (the existing LogDirsCommandTest.java, or a new one) to this task's files.. Files mentioned: tools/src/test/java/org/apache/kafka/tools/LogDirsCommandTest.java. Tasks already committed in the worktree (do not redo them): none.


## What the plan must contain

Return JSON matching the schema you were given.

- `intents`: every intent id above, exactly once, with a `resolution`:
  - `change`: something must change. `evidence` is optional but welcome.
  - `already_satisfied`: the repository already does it. Requires `evidence` that proves it.
  - `out_of_scope`: you propose not doing it. Requires a `rationale`; a human will decide.
- `evidence`: `path` relative to the repository root, `start`/`end` line numbers (1-based,
  inclusive) and a short `quote` that appears literally within those lines. The harness checks
  the file, the range and the quote; a wrong quote fails the plan.
- `criteria`: acceptance criteria, each tied to an `intent`. `kind` is `behavior` when a test
  can prove it, `other` otherwise (documentation, naming). Write them so that a test can be
  named for each `behavior` criterion.
- `tasks`: the units of work. **A task is an increment that could be released on its own: the
  change that would ship as one pull request.** A request that ships as one PR is ONE task, however
  many files it touches; splitting it into steps costs a fresh context per step, loses the design
  decisions between them and produces nothing releasable in between. Use several tasks only when
  the request contains pieces that could be merged and released independently of each other
  (then `depends_on` says which must land first). Never split by layer, by file or by phase.
  - `files`: every file the task may touch, with `role` (`test` if it matches the test globs,
    `production` otherwise) and `action`. The implementer is blocked from touching anything
    else. Declaring a file you will not need is cheap; forgetting one costs a replan.
  - `tests`: the test files (existing or to be created) that prove this task's criteria. Every
    `behavior` criterion must be covered by at least one task with at least one test. **Every
    path in `tests` must also appear in that task's `files`** (with `role: test`); a test path
    missing from `files` fails the plan.
  - `criteria`: the criteria keys this task satisfies.
  - `depends_on`: keys of tasks that must be committed first. No cycles. Independent tasks must
    not share a production file.
  - Existing tests that assert the old behaviour must be listed in the `files` (role `test`) and
    `tests` of the task that changes that behaviour; the implementer is blocked from touching any
    test file the task does not declare.
  - In the task `summary`, state the order of work the implementer should follow when it matters
    (add the new thing, migrate its callers and their tests, deprecate or remove the old thing
    last: many suites turn warnings into errors).
- `summary` on the plan and on each task: one or two sentences for a human reviewer.

Do not include anything the user did not ask for (docs, refactors, extra features) unless an
intent requires it; if you think it is needed, say so in the plan summary instead.
