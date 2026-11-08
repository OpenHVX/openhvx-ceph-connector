# OpenHVX Ceph Connector

The connector executes storage operations against the Ceph Dashboard API and
exchanges commands, results, heartbeats, and storage inventory with the OpenHVX
backend through RabbitMQ.

**Status:** active development. Protocol, durable execution, and failure handling
are tested with simulated Ceph effects. Real Ceph/iSCSI and cluster failure tests
remain necessary before production.

## Documentation

| Guide | Contents |
| --- | --- |
| [Architecture](docs/architecture.md) | Components, message flow, and limitations |
| [Configuration](docs/configuration.md) | Runtime settings, identity, and dependencies |
| [Development](docs/development.md) | Setup, builds, tests, CI, and upgrades |
| [Task protocol](docs/task-protocol.md) | Deduplication, confirmations, and uncertain outcomes |
| [API](docs/api.md) | Supported tasks, probes, and Ceph API reference |

## Local development

Use Node.js 22 and a MongoDB replica set. Start with `.env.example` and the
[development guide](docs/development.md). Keep real credentials out of Git.

K3s lab and production infrastructure belongs to the separate deployment
repository. `compose.yaml` is an optional local connector process definition; it
does not provision Ceph, RabbitMQ, or MongoDB.
