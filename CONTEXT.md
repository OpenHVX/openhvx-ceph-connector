# Repository context

This repository contains the OpenHVX Ceph connector. The backend, Hyper-V agent,
and deployment automation live in separate repositories. Start with
[README.md](README.md). Keep current documentation in English under `docs/`, with
lowercase, topic-based filenames. `.env.example` describes runtime variables.

Core constraints:

- AMQP commands require protocol v2 and matching storage identity.
- Persist execution intent with majority acknowledgment before mutating Ceph.
- Redelivery replays persisted results; it never grants permission to execute again.
- Unknown outcomes preserve backend reservations and require operator reconciliation.
- MongoDB journaling does not make Ceph/iSCSI effects transactionally atomic.
- Do not infer resource absence from failed or partial API listings.
- Confirm result or invalid-message publication before acknowledging a command.
- Preserve journal data and storage identity across upgrades; there is no journal TTL.

See [checks](docs/development.md#checks) and
[known limitations](docs/architecture.md#known-limitations).
