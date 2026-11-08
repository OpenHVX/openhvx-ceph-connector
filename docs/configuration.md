# Configuration

Use [.env.example](../.env.example) as the runtime contract. The connector loads
`.env` through dotenv; already exported variables take precedence. Real profiles
are ignored by Git. Each deployed instance needs reachable dependencies and
credentials distributed by deployment automation.

## Required settings

| Variable | Purpose |
| --- | --- |
| `STORAGE_CONTROLLER_AGENT_ID` | Stable storage identity matching backend command routing |
| `RMQ_URL` | Authenticated RabbitMQ URI |
| `MONGO_URL` | Persistent replica-set journal database URI |
| `CEPH_API_URL` | Ceph Dashboard API base URL |
| `CEPH_API_USER`, `CEPH_API_PASSWORD` | Dedicated Dashboard credentials |
| `CEPH_GW_ISCSI_HOST`, `CEPH_GW_ISCSI_IP` | Gateway used for newly exposed targets |

The legacy `BROKER_URL` alias remains accepted when `RMQ_URL` is absent or identical.
Conflicting values fail startup. There are no fallback runtime credentials.
`SERVICE_NAME` no longer defines result identity, and task prefetch is fixed at one.

## Optional settings

- `TASK_QUEUE` defaults to `agent.<STORAGE_CONTROLLER_AGENT_ID>.tasks`.
- `JOBS_EXCHANGE`, `RESULTS_EXCHANGE`, and `TELEMETRY_EXCHANGE` default to `jobs`,
  `results`, and `agent.telemetry`.
- `TELEMETRY_ROUTING_KEY` defaults to `inventory.storage.full`.
- `AMQP_QUEUE_TYPE` accepts `classic` (default) or `quorum`; match the backend and
  existing broker topology. Queue types cannot be changed in place.
- `CEPH_DEFAULT_POOL` defaults to `rbd`; catalog clones use the `catalog` pool and
  its `base` snapshots.
- `CEPH_API_ACCEPT` defaults to `application/vnd.ceph.api.v1.0+json`; the client
  negotiates v1/v2 after explicit HTTP 415 responses.
- `CEPH_INSECURE_TLS` defaults to false. Use trusted certificates for deployment.
- `CEPH_TARGET_USER` and `CEPH_TARGET_PASSWORD` optionally configure CHAP and must
  be supplied together.
- `PORT` defaults to 3000 for probes. It is not a public task API.
- `INVENTORY_INTERVAL_MS`, `HEARTBEAT_INTERVAL_MS`, and
  `CREATE_READINESS_TIMEOUT_MS` default to 300000, 60000, and 180000 milliseconds.
  Timer values must be positive integers within the supported timer range.
- `LOG_ENABLED`, `LOG_LEVEL`, `LOG_JSON`, and `LOG_SERVICE_NAME` control logging.

`AMQP_INTEGRATION_URL` is for tests only. Set it and `RMQ_URL` to the same dedicated
disposable test broker when running messaging tests.

## Persistence and upgrades

Preserve the journal database and storage identity across rescheduling and upgrades.
Restrict database and broker permissions to the required collections, exchanges,
queues, and bindings. Restrict Dashboard credentials to intended storage operations.
Back up the journal and Ceph state; journal records have no automatic expiry.

Changing identity or deleting the journal is not a valid retry mechanism.
