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

  async init(): Promise<void> {
    logger.info("connecting to AMQP", { url: RMQ_URL });
    const conn = await connect(RMQ_URL);
    this.conn = conn;
    const ch = await conn.createChannel();
    this.ch = ch;

    await ch.prefetch(PREFETCH_COUNT);
    await ch.assertExchange(JOBS_EX, "direct", { durable: true });
    await ch.assertExchange(TELE_EX, "topic", { durable: true });
    await ch.assertExchange(RES_EX, "topic", { durable: true });
    logger.info("AMQP ready", {
      exchanges: { jobs: JOBS_EX, telemetry: TELE_EX, results: RES_EX },
      queue: TASK_QUEUE || `agent.${STORAGE_ID}.tasks`,
      prefetch: PREFETCH_COUNT,
    });
  }

  async consumeTasks(onTask: (task: TaskMessage) => Promise<TaskResult>): Promise<void> {
    if (!this.ch) throw new Error("Channel not initialized");
    const ch = this.ch;
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

  publishResult(action: string, result: TaskResult): void {
    if (!this.ch) throw new Error("Channel not initialized");
    logger.debug("publishing result", { action, result });
    this.ch.publish(RES_EX, `task.${action}`, Buffer.from(JSON.stringify(result)), {
      contentType: "application/json",
      deliveryMode: 2,
      correlationId: result.taskId,
    });
  }

  publishTelemetry(inventory: StorageInventoryV1): void {
    if (!this.ch) throw new Error("Channel not initialized");
    const payload: StorageTelemetryEnvelopeV1 = {
      storageId: STORAGE_ID,
      ts: new Date().toISOString(),
      inventory,
    };

    logger.info("publishing telemetry", { storageId: payload.storageId });
    logger.debug("publishing telemetry payload", { payload });
    this.ch.publish(TELE_EX, TELE_ROUTING_KEY, Buffer.from(JSON.stringify(payload)), {
      contentType: "application/json",
      deliveryMode: 2,
    });
  }

  publishEvent(payload: unknown, routingKey = TELE_ROUTING_KEY): void {
    if (!this.ch) throw new Error("Channel not initialized");
    logger.info("publishing event", { routingKey, payload });
    this.ch.publish(TELE_EX, routingKey, Buffer.from(JSON.stringify(payload)), {
      contentType: "application/json",
      deliveryMode: 2,
    });
  }

  async close(): Promise<void> {
    await this.ch?.close();
    await this.conn?.close();
  }
}
