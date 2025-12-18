# OpenHVX Ceph Connector

Agent that orchestrates Ceph volumes through RBD and iSCSI while exchanging tasks and telemetry over AMQP.
- Listens on a jobs exchange and creates/deletes/clones RBD images.
- Exposes images over iSCSI and publishes inventory telemetry.
- Relies on Ceph Dashboard and an AMQP broker configured via the environment variables in `.env.example`.
- Idempotent, if any errors, rollback to previous state.

## Environment variables

**AMQP**

| Variable | Example | Description |
| --- | --- | --- |
| `BROKER_URL` | `amqp://guest:guest@localhost:5672/` | AMQP broker URL (alias: `RMQ_URL`) |
| `JOBS_EXCHANGE` | `jobs` | Exchange to receive tasks |
| `RESULTS_EXCHANGE` | `results` | Exchange to publish task results |
| `TASK_QUEUE` | `openhvx.storage.tasks` | Queue name for tasks (bound to jobs exchange) |
| `TELEMETRY_EXCHANGE` | `agent.telemetry` | Exchange for telemetry/events |
| `TELEMETRY_ROUTING_KEY` | `inventory.storage.full` | Routing key for telemetry publishes |
| `SERVICE_NAME` | `storage-controller` | Service name advertised |
| `STORAGE_CONTROLLER_AGENT_ID` | `storage-controller-1` | Agent identifier/binding key |
| `PREFETCH_COUNT` | `10` | AMQP prefetch for task consumer |

**Ceph API**

| Variable | Example | Description |
| --- | --- | --- |
| `CEPH_API_URL` | `http://ceph-mgr:8003` | Ceph dashboard API base URL |
| `CEPH_API_USER` | `admin` | Ceph API username |
| `CEPH_API_PASSWORD` | `changeme` | Ceph API password |
| `CEPH_DEFAULT_POOL` | `rbd` | Default RBD pool |
| `CEPH_INSECURE_TLS` | `false` | Allow insecure TLS to Ceph API |
| `CEPH_API_ACCEPT` | `application/vnd.ceph.api.v1.0+json` | Accept header for Ceph API |
| `CEPH_GW_ISCSI_HOST` | `ceph-gw-01` | iSCSI gateway hostname |
| `CEPH_GW_ISCSI_IP` | `127.0.0.1` | iSCSI gateway IP |
| `CEPH_TARGET_USER` | *(optional)* | iSCSI target CHAP user (set with password) |
| `CEPH_TARGET_PASSWORD` | *(optional)* | iSCSI target CHAP password (set with user) |

**Agent behavior**

| Variable | Example | Description |
| --- | --- | --- |
| `INVENTORY_INTERVAL_MS` | `300000` | Interval between inventory publishes (ms) |
| `CREATE_READINESS_TIMEOUT_MS` | `180000` | Timeout for create readiness steps (ms) |

**Logging**

| Variable | Example | Description |
| --- | --- | --- |
| `LOG_ENABLED` | `1` | Enable logging (0 disables) |
| `LOG_LEVEL` | `info` | Log level: info\|debug\|warn\|error |
| `LOG_JSON` | `0` | Emit logs as JSON when `1` |
| `LOG_SERVICE_NAME` | `storage-controller` | Service name in logs |
