# Task protocol

The connector accepts protocol v2 envelopes containing `taskId`, `eventId`, an exact
UTF-8 `commandBody`, and its SHA-256 `commandFingerprint`. The body carries
`operationId`, `tenantId`, `agentId`, `action`, and `data`. The operation ID must
match the outer task ID and the destination must match the connector identity.
Only the validated inner body is executed. A conflicting inner tenant is refused.

SHA-256 binds content; it is not producer authentication. Broker access controls
and transport authentication remain required.

## Execution and acknowledgment

1. Atomically insert a majority-durable journal entry keyed by storage/operation
   identity, with an initial `unknown` result and sequence 1.
2. Only the successful inserter may invoke the Ceph workflow.
3. Persist its resulting `applied` or `unknown` outcome at sequence 2 before delivery.
4. Publish the stored result using persistent delivery, mandatory routing, and
   publisher confirmation. A timeout or returned message prevents acknowledgment.
5. Acknowledge the original command only after confirmed result publication.

Duplicates with the same fingerprint replay the current persisted result, including
its stable event ID and timestamp. A duplicate arriving during execution can see
sequence 1; a later sequence 2 supersedes it in the backend. A different fingerprint
for the same identity is quarantined. No journal TTL removes old identities.

Malformed, legacy, misrouted, and conflicting commands are confirmed into
`<TASK_QUEUE>.invalid` before acknowledgment. Other delivery failures are requeued.
Closed channels rely on broker redelivery; the operation journal prevents re-execution.

## Uncertain outcomes

An API error or process loss does not prove that Ceph was unchanged. Such operations
remain `unknown`, preserving backend locks and quota holds. The connector currently
makes no automatic `not_applied` claims and has no automatic observation reconciler.

Creating a volume uses an operation-derived name; unexpected existing images or
targets require investigation. Failed reads do not mean resource absence, and failed
target deletion prevents backing-image deletion. Multi-step workflows retain partial
resources on uncertainty instead of issuing potentially destructive rollback calls.

This design guarantees durable admission and deduplication while journal state is
preserved. It does not make multiple Ceph changes atomic or ensure every interrupted
operation automatically completes. Resolve uncertain resources and accounting through
a reviewed operational procedure; never erase identity state to force a retry.
