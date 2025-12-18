import { AmqpService, TaskMessage } from './services/amqp';
import { config } from './config';
import { CephApi } from './services/ceph-api';
import { CephController } from './controllers/ceph.controller';
import { TasksController } from './controllers/tasks.controller';
import logger from './lib/logger';
import os from 'os';

const amqp = new AmqpService();
const cephApi = new CephApi({
  baseUrl: config.ceph.url,
  username: config.ceph.user,
  password: config.ceph.password,
  allowInsecureTls: config.ceph.insecureTls,
  acceptHeader: config.ceph.apiAccept,
});

const cephController = new CephController(cephApi, config.ceph.defaultPool);
const tasksController = new TasksController(cephController, amqp, config.broker.service.name);
const INVENTORY_INTERVAL_MS = Number(process.env.INVENTORY_INTERVAL_MS || 300_000);
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 60_000);
const pkg = require('../package.json') as { version?: string };

async function start(): Promise<void> {
  await amqp.init();

  await amqp.consumeTasks(async (task: TaskMessage) => {
    logger.info('task received', { task });
    return tasksController.handle(task);
  });

  // Send a first inventory snapshot on startup
  await tasksController.refreshInventory().catch((err) => {
    logger.error('Failed to publish initial inventory', { err });
  });

  const heartbeatPayload = () => ({
    agentId: config.broker.service.agentId,
    version: pkg.version ?? 'unknown',
    capabilities: ['storage'],
    host: os.hostname(),
    ts: new Date().toISOString(),
  });

  // Initial heartbeat
  amqp.publishHeartbeat(heartbeatPayload());

  // Periodic inventory refresh
  const inventoryTimer = setInterval(() => {
    tasksController.refreshInventory().catch((err) => {
      logger.error('Failed to publish periodic inventory', { err });
    });
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
    clearInterval(inventoryTimer);
    clearInterval(heartbeatTimer);
    await amqp.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

start().catch((err) => {
  console.error(err);
  process.exit(1);
});
