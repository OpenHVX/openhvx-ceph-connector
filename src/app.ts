import { AmqpService } from './services/amqp';
import { config, positiveInteger } from './config';
import { CephApi } from './services/ceph-api';
import { CephController } from './controllers/ceph.controller';
import { TasksController } from './controllers/tasks.controller';
import logger from './lib/logger';
import os from 'os';
import http from 'node:http';
import { OperationJournal } from './services/operations/journal';

const amqp = new AmqpService();
const cephApi = new CephApi({
  baseUrl: config.ceph.url,
  username: config.ceph.user,
  password: config.ceph.password,
  allowInsecureTls: config.ceph.insecureTls,
  acceptHeader: config.ceph.apiAccept,
});

const cephController = new CephController(cephApi, config.ceph.defaultPool);
const tasksController = new TasksController(cephController, amqp, config.broker.service.agentId);
const INVENTORY_INTERVAL_MS = positiveInteger('INVENTORY_INTERVAL_MS', 300000);
const HEARTBEAT_INTERVAL_MS = positiveInteger('HEARTBEAT_INTERVAL_MS', 60000);
const pkg = require('../package.json') as { version?: string };

const journal = new OperationJournal();
let stopping = false;
const health = http.createServer(async (req, res) => {
  if (req.url !== '/healthz' && req.url !== '/readyz') { res.writeHead(404).end(); return; }
  res.writeHead(req.url === '/readyz' && (!amqp.ready || stopping || !await journal.healthy()) ? 503 : 200).end();
});

async function start(): Promise<void> {
  if (!process.env.MONGO_URL) throw new Error('MONGO_URL required for durable operations');
  await journal.open(process.env.MONGO_URL);
  await amqp.init();

  await amqp.consumeTasks(task => journal.execute(task, () => tasksController.handle(task)));
  health.listen(positiveInteger('PORT', 3000), '0.0.0.0');

  // Send a first inventory snapshot on startup
  await tasksController.refreshInventory().catch((err) => {
    logger.error('Failed to publish initial inventory', { err });
  });

  const heartbeatPayload = () => ({
    agentId: config.broker.service.agentId,
    version: pkg.version ?? 'unknown',
    capabilities: ['storage', 'task.protocol.v2'],
    host: os.hostname(),
    ts: new Date().toISOString(),
  });

  // Initial heartbeat
  amqp.publishHeartbeat(heartbeatPayload());

  // Serialize periodic inventory collection to avoid overlapping slow Ceph calls.
  let inventoryRunning = false;
  const inventoryTimer = setInterval(() => {
    if (inventoryRunning || stopping) return;
    inventoryRunning = true;
    void tasksController.refreshInventory()
      .catch(() => logger.error('Periodic inventory failed'))
      .finally(() => { inventoryRunning = false; });
  }, INVENTORY_INTERVAL_MS);

  // Periodic heartbeat
  const heartbeatTimer = setInterval(() => {
    try {
      amqp.publishHeartbeat(heartbeatPayload());
    } catch (err) {
      logger.error('Failed to publish heartbeat', { err });
    }
  }, HEARTBEAT_INTERVAL_MS);

  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    health.close();
    clearInterval(inventoryTimer);
    clearInterval(heartbeatTimer);
    await amqp.close();
    await journal.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

start().catch((err) => {
  logger.error('Connector startup failed', { error: err instanceof Error ? err.message : 'Unknown error' });
  process.exit(1);
});
