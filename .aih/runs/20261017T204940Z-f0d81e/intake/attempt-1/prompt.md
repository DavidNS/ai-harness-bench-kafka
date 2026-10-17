You are the intake step of a coding harness. Your only job is to turn the user's request into a
short list of concrete intents, or to ask the few questions without which that is not possible.
You may read the repository to understand terms the request uses, but do not plan or design.

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



## Output

Return JSON matching the schema you were given:

- `status`: `ready` when the intents are unambiguous enough to plan from; `needs_clarification`
  when a reasonable engineer would have to ask before starting.
- `intents`: one entry per distinct thing the user wants, each with a short `key` (kebab-case)
  and one sentence of `text` that a later reviewer can check against the final code. Split
  compound requests. Do not invent requirements the user did not state.
- `constraints`: explicit limits the user set ("do not change X", "keep the public API").
- `questions`: at most 3, only when `status` is `needs_clarification`, each with a one-line
  `why`. Ask only what changes what you would build.

## When you must ask

Return `needs_clarification` whenever any of these holds; do not resolve them with assumptions:

- The request names a goal without a checkable target ("faster", "better", "cleaner",
  "more robust") and does not say which code path, which measurement or which threshold.
- Two reasonable engineers would build materially different things from the same words.
- The request does not say what must not change, and the obvious implementation would alter
  existing public behaviour.
- A term in the request matches nothing in the repository.

Return `ready` when the request names the concrete behaviour, place or artifact and the only open
points are implementation details you would decide yourself. A small assumption that does not
change what gets built may be stated in the intent text instead of asked. Inventing scope the
user did not mention (benchmarks, documentation, refactors) is never acceptable: either ask or
leave it out.

When `status` is `ready`, `questions` must be empty. When it is `needs_clarification`, still fill
`intents` with your best current understanding.
