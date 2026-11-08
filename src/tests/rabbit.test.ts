import './setup';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { connect } from 'amqplib';
import { createHash } from 'node:crypto';
import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { OperationJournal } from '../services/operations/journal';
import { AmqpService } from '../services/amqp';

test('RabbitMQ confirms durable results, deduplicates commands and quarantines invalid payloads', { skip: !process.env.AMQP_INTEGRATION_URL, timeout: 60000 }, async () => {
  // Configuration is loaded by amqp.ts; CI sets RMQ_URL to this dedicated broker too.
  assert.equal(process.env.RMQ_URL, process.env.AMQP_INTEGRATION_URL, 'RMQ_URL must match the integration broker');
  const mongo = await MongoMemoryReplSet.create({ binary: { version: '7.0.24' }, replSet: { count: 1 } });
  const journal = new OperationJournal(); const service = new AmqpService();
  const conn = await connect(process.env.AMQP_INTEGRATION_URL!);
  conn.on('error', () => {});
  try {
    await journal.open(mongo.getUri()); await service.init();
    let effects = 0;
    await service.consumeTasks(command => journal.execute(command, async () => { effects++; return { ok: true, result: { id: 'disk' } }; }));
    const ch = await conn.createChannel();
    const results = await ch.assertQueue('', { exclusive: true });
    await ch.bindQueue(results.queue, 'results', 'task.storage.create');
    const received: any[] = [];
    await ch.consume(results.queue, msg => { if (msg) received.push(JSON.parse(msg.content.toString())); }, { noAck: true });
    const commandBody = JSON.stringify({ operationId: 'rabbit-op', tenantId: 'tenant-a', agentId: 'test-storage', action: 'storage.create', data: {} });
    const body = Buffer.from(JSON.stringify({ taskId: 'rabbit-op', contractVersion: 2, eventId: 'delivery', commandBody, commandFingerprint: createHash('sha256').update(commandBody).digest('hex') }));
    ch.publish('jobs', 'test-storage', body, { persistent: true });
    ch.publish('jobs', 'test-storage', body, { persistent: true });
    const until = Date.now() + 15000;
    while (received.length < 2 && Date.now() < until) await new Promise(r => setTimeout(r, 30));
    assert.equal(received.length, 2); assert.equal(effects, 1); assert.deepEqual(received[0], received[1]);
    ch.publish('jobs', 'test-storage', Buffer.from('{invalid'));
    let invalid = false;
    while (!invalid && Date.now() < until) { invalid = !!await ch.get('agent.test-storage.tasks.invalid', { noAck: true }); if (!invalid) await new Promise(r => setTimeout(r, 30)); }
    assert.equal(invalid, true);
  } finally { await service.close(); await conn.close(); await journal.close(); await mongo.stop(); }
});
