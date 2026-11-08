import { connect, Channel, ChannelModel, ConsumeMessage } from 'amqplib';
import { config } from '../config';
import { StorageTelemetryEnvelopeV1, StorageInventoryV1 } from '../types/inventory';
import { Command, decodeCommand, InvalidCommand, Result } from './operations/protocol';
import logger from '../lib/logger';

export type TaskMessage = Pick<Command, 'taskId' | 'action' | 'data'> & Partial<Command>;
export type TaskResult = { taskId: string; agentId: string; ok: boolean; result?: unknown; error?: string; finishedAt: string };
const queueType = process.env.AMQP_QUEUE_TYPE || 'classic';
if (!['classic', 'quorum'].includes(queueType)) throw new Error('AMQP_QUEUE_TYPE must be classic or quorum');
const queueArgs = { 'x-queue-type': queueType };

export class AmqpService {
  private conn?: ChannelModel;
  private ch?: Channel;
  private consumer?: (task: Command) => Promise<Result>;
  private connecting?: Promise<void>;
  private timer?: NodeJS.Timeout;
  private stopping = false;
  private consumerTag?: string;
  private inFlight = new Set<Promise<void>>();
  get ready(): boolean { return !!this.ch && !this.stopping; }

  async init(): Promise<void> {
    if (!this.connecting) this.connecting = this.open().finally(() => { this.connecting = undefined; });
    await this.connecting;
  }
  private async open(): Promise<void> {
    const conn = await connect(config.broker.url);
    conn.on('error', () => logger.warn('AMQP connection error'));
    conn.on('close', () => {
      if (this.conn !== conn) return;
      this.conn = undefined; this.ch = undefined; this.consumerTag = undefined;
      this.reconnect();
    });
    this.conn = conn;
    try {
      const ch = await conn.createChannel();
      ch.on('error', () => logger.warn('AMQP channel error'));
      ch.on('close', () => { if (this.conn === conn) void conn.close().catch(() => {}); });
      await ch.assertExchange(config.broker.task.exchange, 'direct', { durable: true });
      await ch.assertExchange(config.broker.telemetry.exchange, 'topic', { durable: true });
      await ch.assertExchange(config.broker.results.exchange, 'topic', { durable: true });
      await ch.assertQueue(`${config.broker.task.queue}.invalid`, { durable: true, arguments: queueArgs });
      await ch.assertQueue(config.broker.task.queue, { durable: true, arguments: queueArgs });
      await ch.bindQueue(config.broker.task.queue, config.broker.task.exchange, config.broker.service.agentId);
      await ch.prefetch(1);
      this.ch = ch;
      if (this.consumer) await this.startConsumer();
    } catch (error) { await conn.close().catch(() => {}); throw error; }
  }
  private reconnect(): void {
    if (this.stopping || this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.init().catch(() => this.reconnect());
    }, 5000);
  }
  async consumeTasks(consumer: (task: Command) => Promise<Result>): Promise<void> {
    this.consumer = consumer;
    await this.startConsumer();
  }
  private async startConsumer(): Promise<void> {
    const ch = this.ch;
    if (!ch || !this.consumer || this.consumerTag) return;
    const result = await ch.consume(config.broker.task.queue, (msg) => {
      if (!msg) { void this.conn?.close().catch(() => {}); return; }
      const pending = this.handle(ch, msg).finally(() => this.inFlight.delete(pending));
      this.inFlight.add(pending);
    }, { noAck: false, exclusive: true });
    this.consumerTag = result.consumerTag;
  }
  private async handle(ch: Channel, msg: ConsumeMessage): Promise<void> {
    try {
      let command: Command;
      try { command = decodeCommand(JSON.parse(msg.content.toString('utf8')), config.broker.service.agentId); }
      catch { throw new InvalidCommand('Invalid command'); }
      const result = await this.consumer!(command);
      await this.confirm(config.broker.results.exchange, `task.${command.action}`, Buffer.from(JSON.stringify(result)), result.eventId);
      ch.ack(msg);
    } catch (error) {
      try {
        if (error instanceof InvalidCommand) {
          await this.confirm('', `${config.broker.task.queue}.invalid`, msg.content, msg.properties.messageId);
          ch.ack(msg);
        } else ch.nack(msg, false, true);
      } catch { /* Closed delivery channel: broker redelivers the persisted operation. */ }
      logger.warn('Task delivery deferred or quarantined', { invalid: error instanceof InvalidCommand });
    }
  }
  private async confirm(exchange: string, key: string, body: Buffer, eventId?: string): Promise<void> {
    const conn = this.conn;
    if (!conn) throw new Error('AMQP unavailable');
    const channel = await conn.createConfirmChannel();
    channel.on('error', () => {});
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Publisher confirm timeout')), 20000);
        const finish = (error?: Error | null) => { clearTimeout(timer); error ? reject(error) : resolve(); };
        channel.once('return', () => finish(new Error('Unroutable message')));
        channel.once('close', () => finish(new Error('Publisher closed')));
        channel.publish(exchange, key, body, { persistent: true, mandatory: true, contentType: 'application/json', messageId: eventId }, finish);
      });
    } finally { await channel.close().catch(() => {}); }
  }
  publishTelemetry(inventory: StorageInventoryV1): void {
    const payload: StorageTelemetryEnvelopeV1 = { storageId: config.broker.service.agentId, ts: new Date().toISOString(), inventory };
    this.publishEvent(payload, config.broker.telemetry.routingKey);
  }
  publishEvent(payload: unknown, routingKey = config.broker.telemetry.routingKey): void {
    if (!this.ch) throw new Error('AMQP unavailable');
    this.ch.publish(config.broker.telemetry.exchange, routingKey, Buffer.from(JSON.stringify(payload)), { persistent: true, contentType: 'application/json' });
  }
  publishHeartbeat(payload: { agentId: string; version: string; capabilities: string[]; host: string; ts: string }): void {
    this.publishEvent(payload, `heartbeat.${payload.agentId}`);
  }
  async close(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearTimeout(this.timer);
    if (this.ch && this.consumerTag) await this.ch.cancel(this.consumerTag).catch(() => {});
    let timer: NodeJS.Timeout | undefined;
    await Promise.race([Promise.allSettled([...this.inFlight]), new Promise(resolve => { timer = setTimeout(resolve, 30000); })]);
    if (timer) clearTimeout(timer);
    await this.conn?.close().catch(() => {});
    this.ch = undefined;
  }
}
