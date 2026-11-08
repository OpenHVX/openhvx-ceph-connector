# API

The connector has no public HTTP task-submission API. Clients submit tasks through
the backend, which owns authorization, resource locks, quotas, and idempotency keys.
The connector receives the resulting protocol v2 commands through RabbitMQ.

## Supported actions

| Action | Input and behavior |
| --- | --- |
| `inventory.refresh` | Collect and publish the current storage inventory |
| `storage.create` | Positive integer `sizeMB`, `name`, and catalog `imageId`; clone its `base` snapshot into the configured pool and expose a dedicated iSCSI target |
| `storage.delete` | `target.refId` identifies the tenant-prefixed image; optionally `target.agentId` must match this connector; only the configured pool is supported |

The tenant comes from the immutable command identity. Gateway portals come from
connector configuration. Creation returns `disk.id`, `disk.refId`, `disk.name`,
`disk.iqn`, and `disk.tenantId`; image-based IDs agree with inventory references.
Resize and deletion completion are observed before returning success.
Creation refuses a requested size smaller than the cloned source and retains the
partial clone for investigation.

## HTTP probes

| Route | Meaning |
| --- | --- |
| `/healthz` | Process HTTP handler responds |
| `/readyz` | AMQP channel is available, journal MongoDB responds, and shutdown has not started |

Readiness does not prove that every Ceph operation will succeed. These probes carry
no task mutation endpoints and should be exposed only as operational endpoints.

## Ceph reference

The checked-in [Ceph API description](ceph-api.json) is a reference snapshot.
Validate actual endpoint behavior and permissions against the deployed Ceph release.
The connector uses the Dashboard API for authentication, health, RBD images and
snapshots, namespaces, and iSCSI targets. Unit tests do not establish compatibility
with every Ceph version.
