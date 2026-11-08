# Architecture

The connector adapts backend storage tasks to the Ceph Dashboard REST API. It also
publishes heartbeats and storage inventories for backend placement and discovery.

| Component | Responsibility |
| --- | --- |
| `src/app.ts` | Startup, probes, telemetry schedules, and shutdown |
| `src/services/amqp.ts` | Topology, exclusive task consumption, confirmed results, reconnect |
| `src/services/operations` | Protocol validation and durable operation journal |
| `src/controllers/tasks.controller.ts` | Storage workflows and mutation safeguards |
| `src/controllers/ceph.controller.ts` | Ceph operations and inventory normalization |
| `src/services/ceph-api.ts` | Authentication and versioned Dashboard API requests |

## Dependencies and identity

RabbitMQ carries commands on `jobs` (direct), results on `results` (topic), and
telemetry on `agent.telemetry` (topic). The defaults align with the backend.
The configured storage identity is used consistently for routing, journal keys,
results, inventories, and heartbeats.

The durable MongoDB journal uses a replica set, primary reads, majority read/write
concerns, and journaled writes. Each operation is a single atomic document keyed
by storage identity and operation ID. It is shared across connector replicas.

One consumer is active per task queue through RabbitMQ exclusive consumption,
with prefetch fixed at one. Additional replicas are not independent writers for
the same identity; failed exclusive acquisition can cause startup/reconnect retries.
The journal still deduplicates concurrent requests across processes.

## Known limitations

MongoDB, RabbitMQ, Ceph RBD, and iSCSI do not share an ACID transaction. A crash or
API timeout can leave partial resources. Unknown outcomes are retained and never
blindly retried or automatically compensated.

There is currently no automatic read-only Ceph reconciler or operator resolution
CLI. Investigation must verify the journal, actual image, target, and backend
reservation together. Do not edit journal results or delete state to force retries.

Storage deletion supports resources using the connector's tenant-prefixed image
naming and configured pool. Legacy resources outside that scope need a reviewed
migration. Shared multi-image targets are refused before mutation.

Ceph responses differ across releases. Automated tests simulate the Dashboard API
and host effects; real supported Ceph/iSCSI environments, session behavior, broker
failover, and network partitions still require deployment-level validation.
