import { connect, Channel, ChannelModel, ConsumeMessage } from "amqplib";
import { config } from "../config";
import { StorageTelemetryEnvelopeV1, StorageInventoryV1 } from "../types/inventory";
import logger from "../lib/logger";

const RMQ_URL = config.broker.url;
const JOBS_EX = config.broker.task.exchange;
const TELE_EX = config.broker.telemetry.exchange;
const RES_EX = config.broker.results.exchange;
const STORAGE_ID = config.broker.service.agentId;
const TELE_ROUTING_KEY = config.broker.telemetry.routingKey;
const TASK_QUEUE = config.broker.task.queue;
const PREFETCH_COUNT = config.broker.prefetchCount;
const RECONNECT_DELAY_MS = Number(process.env.BROKER_RECONNECT_DELAY_MS || 5000);

const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

export type TaskMessage = {
  taskId: string;
  action: string;
  data: Record<string, unknown>;
  tenantId?: string;
};

export type TaskResult = {
  taskId: string;
  agentId: string;
  ok: boolean;
  result?: unknown;
  error?: string;
  finishedAt: string;
};

export class AmqpService {
  private conn?: ChannelModel;
  private ch?: Channel;
  private consumer?: (task: TaskMessage) => Promise<TaskResult>;
  private connectPromise?: Promise<void>;
  private reconnectTimer?: NodeJS.Timeout;
  private shuttingDown = false;

  async init(): Promise<void> {
    await this.ensureConnected();
  }

  async consumeTasks(onTask: (task: TaskMessage) => Promise<TaskResult>): Promise<void> {
    this.consumer = onTask;
    if (!this.ch) {
      await this.ensureConnected();
    }
    await this.startConsumer(onTask);
  }

  publishResult(action: string, result: TaskResult): void {
    const ch = this.getChannel();
    if (!ch) {
      logger.warn("dropping result publish, channel unavailable", { action });
      return;
    }
    logger.debug("publishing result", { action, result });
    ch.publish(RES_EX, `task.${action}`, Buffer.from(JSON.stringify(result)), {
      contentType: "application/json",
      deliveryMode: 2,
      correlationId: result.taskId,
    });
  }

  publishTelemetry(inventory: StorageInventoryV1): void {
    const ch = this.getChannel();
    if (!ch) {
      logger.warn("dropping telemetry publish, channel unavailable");
      return;
    }
    const payload: StorageTelemetryEnvelopeV1 = {
      storageId: STORAGE_ID,
      ts: new Date().toISOString(),
      inventory,
    };

    logger.info("publishing telemetry", { storageId: payload.storageId });
    logger.debug("publishing telemetry payload", { payload });
    ch.publish(TELE_EX, TELE_ROUTING_KEY, Buffer.from(JSON.stringify(payload)), {
      contentType: "application/json",
      deliveryMode: 2,
    });
  }

  publishEvent(payload: unknown, routingKey = TELE_ROUTING_KEY): void {
    const ch = this.getChannel();
    if (!ch) {
      logger.warn("dropping event publish, channel unavailable", { routingKey });
      return;
    }
    logger.info("publishing event", { routingKey, payload });
    ch.publish(TELE_EX, routingKey, Buffer.from(JSON.stringify(payload)), {
      contentType: "application/json",
      deliveryMode: 2,
    });
  }

  publishHeartbeat(payload: { agentId: string; version: string; capabilities: string[]; host: string; ts: string }): void {
    const ch = this.getChannel();
    if (!ch) {
      logger.warn("dropping heartbeat, channel unavailable", { agentId: payload.agentId });
      return;
    }
    const routingKey = `heartbeat.${payload.agentId}`;
    logger.debug("publishing heartbeat", { routingKey, payload });
    ch.publish(TELE_EX, routingKey, Buffer.from(JSON.stringify(payload)), {
      contentType: "application/json",
      deliveryMode: 2,
    });
  }

  async close(): Promise<void> {
    this.shuttingDown = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    await this.ch?.close();
    await this.conn?.close();
  }

  private async ensureConnected(): Promise<void> {
    if (!this.connectPromise) {
      this.connectPromise = this.connectWithRetry();
    }
    await this.connectPromise;
  }

  private async connectWithRetry(): Promise<void> {
    while (!this.shuttingDown) {
      try {
        await this.createConnection();
        return;
      } catch (err) {
        logger.error("failed to connect to AMQP, retrying", { err, delayMs: RECONNECT_DELAY_MS });
        await wait(RECONNECT_DELAY_MS);
      }
    }
  }

  private async createConnection(): Promise<void> {
    logger.info("connecting to AMQP", { url: RMQ_URL });
    const conn = await connect(RMQ_URL);
    this.conn = conn;
    conn.on("error", (err) => {
      if (this.shuttingDown) return;
      logger.error("AMQP connection error", { err });
    });
    conn.on("close", () => {
      if (this.shuttingDown) return;
      logger.warn("AMQP connection closed, scheduling reconnect", { delayMs: RECONNECT_DELAY_MS });
      this.cleanupConnection();
      this.connectPromise = undefined;
      this.scheduleReconnect();
    });

    const ch = await conn.createChannel();
    this.ch = ch;

    await this.setupTopology(ch);

    if (this.consumer) {
      await this.startConsumer(this.consumer);
    }

    logger.info("AMQP ready", {
      exchanges: { jobs: JOBS_EX, telemetry: TELE_EX, results: RES_EX },
      queue: TASK_QUEUE || `agent.${STORAGE_ID}.tasks`,
      prefetch: PREFETCH_COUNT,
    });
  }

  private async setupTopology(ch: Channel): Promise<void> {
    await ch.prefetch(PREFETCH_COUNT);
    await ch.assertExchange(JOBS_EX, "direct", { durable: true });
    await ch.assertExchange(TELE_EX, "topic", { durable: true });
    await ch.assertExchange(RES_EX, "topic", { durable: true });
  }

  private async startConsumer(onTask: (task: TaskMessage) => Promise<TaskResult>): Promise<void> {
    const ch = this.getChannel();
    if (!ch) {
      throw new Error("Channel not initialized");
    }
    const queue = TASK_QUEUE || `agent.${STORAGE_ID}.tasks`;
    await ch.assertQueue(queue, { durable: true });
    await ch.bindQueue(queue, JOBS_EX, STORAGE_ID);

    ch.consume(queue, async (msg: ConsumeMessage | null) => {
      if (!msg) return;
      const parsed = JSON.parse(msg.content.toString()) as TaskMessage;
      try {
        const result = await onTask(parsed);
        this.publishResult(parsed.action, result);
        ch.ack(msg);
      } catch (error) {
        const fail: TaskResult = {
          taskId: parsed.taskId,
          agentId: STORAGE_ID,
          ok: false,
          error: (error as Error).message,
          finishedAt: new Date().toISOString(),
        };
        this.publishResult(parsed.action, fail);
        ch.ack(msg);
      }
    });
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer || this.shuttingDown) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.ensureConnected();
    }, RECONNECT_DELAY_MS);
  }

  private cleanupConnection(): void {
    this.ch = undefined;
    this.conn = undefined;
  }

  private getChannel(): Channel | undefined {
    if (this.ch) return this.ch;
    this.scheduleReconnect();
    return undefined;
  }
}
