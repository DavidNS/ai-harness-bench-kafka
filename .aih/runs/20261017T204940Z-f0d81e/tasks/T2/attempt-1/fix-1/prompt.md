An independent reviewer, who has not seen your reasoning, blocked task T2 (fix-up 1). Fix the findings below ON TOP of your current change: do not start over, do not revert what works, and touch only the task's files. Where a finding says a case is untested, add the test; where it says the code is wrong, fix the code and test it.

Findings:
- the reviewer requests 1 change(s): ACC20 is not proven by a test: partial: Admin isCordoned() is asserted but LogDirsCommand output is untested; add a LogDirsCommandTest that checks the printed field.
- [pass 5 coverage] server/src/test/java/org/apache/kafka/server/CordonedLogDirsIntegrationTest.java:130: ACC20 is not proven by a test: partial: Admin isCordoned() is asserted but LogDirsCommand output is untested; add a LogDirsCommandTest that checks the printed field.

When done, finish again with the same JSON answer (summary, criteria_notes, replan_request). The harness will run the task's tests and review the change again.