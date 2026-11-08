# Development

Use Node.js 22 and npm. Ceph, RabbitMQ, MongoDB, and deployment manifests are
managed outside this repository.

## Local setup

From the repository root:

```bash
npm ci
npm run build
```

Create a private `.env` from `.env.example` and configure reachable dependencies.
Use a MongoDB replica set for the durable journal, then run `npm start`.

`compose.yaml` optionally runs the connector against those external dependencies:

```bash
docker compose -f compose.yaml config --quiet
docker compose -f compose.yaml up --build -d
```

Its `.env` values must use addresses reachable from the container. The definition
does not start a storage cluster or publish the HTTP probe port to the host.

## Checks

```bash
npm run lint
npm run build
npm audit --omit=dev
npm test
```

Lint currently means TypeScript type checking. MongoDB tests start temporary
replica sets using MongoDB 7.0.24, downloaded on first use. They verify concurrent
admission, durable replay, conflicting identities, and real process loss after a
simulated fsynced host effect. Ceph controller tests use mocked API adapters.

To include the real RabbitMQ test, export both `RMQ_URL` and
`AMQP_INTEGRATION_URL` with the same disposable broker URI. The test publishes
commands and creates queues; never point it at a production broker.
Without that variable, the RabbitMQ test is explicitly skipped.

## Continuous integration

[Connector checks](../.github/workflows/connector.yml) runs on pushes and pull
requests with Node.js 22 and temporary RabbitMQ. It installs locked dependencies,
checks types, builds, audits production dependencies, runs tests including real
messaging, validates Compose, and builds the container image. It does not publish
images or deploy infrastructure.

Unlike the backend's Go compatibility test, this workflow does not check out other
repositories. Protocol tests include the shared backend command vector. Real Ceph
and complete backend/connector deployment tests remain separate validation.

## Upgrades

Drain legacy consumers and commands before switching to protocol v2. Default task
queue naming and telemetry defaults have changed to match the backend. Inspect and
migrate old queue bindings/backlogs explicitly; do not discard unprocessed tasks.

Provision the persistent journal replica-set database before startup. Coordinate
backend and connector versions, preserve storage identity, and review legacy image
names and pools before enabling deletion. See the
[task protocol](task-protocol.md) for ambiguous outcomes.
