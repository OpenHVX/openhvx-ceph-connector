import './setup';
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { OperationJournal } from '../services/operations/journal';
import { decodeCommand, Command } from '../services/operations/protocol';

let mongo: MongoMemoryReplSet;
let journal: OperationJournal;
let uri: string;
before(async () => {
  mongo = await MongoMemoryReplSet.create({ binary: { version: '7.0.24' }, replSet: { count: 1 } });
  uri = mongo.getUri(); journal = new OperationJournal(); await journal.open(uri);
});
after(async () => { await journal.close(); await mongo?.stop(); });
export function command(id: string): Command {
  return { taskId: id, operationId: id, agentId: 'test-storage', tenantId: 'tenant-a', action: 'storage.create', data: {}, commandFingerprint: createHash('sha256').update(id).digest('hex') };
}
test('concurrent duplicate commands run one host effect and replay the persisted final result after restart', async () => {
  const c = command('concurrent'); let effects = 0;
  await Promise.all(Array.from({ length: 20 }, () => journal.execute(c, async () => { effects++; await new Promise(r => setTimeout(r, 25)); return { ok: true, result: { id: 'disk' } }; })));
  assert.equal(effects, 1);
  const expected = await journal.execute(c, async () => { throw new Error('must not run'); });
  assert.equal(expected.outcome, 'applied');
  await journal.close(); journal = new OperationJournal(); await journal.open(uri);
  assert.deepEqual(await journal.execute(c, async () => { throw new Error('must not run'); }), expected);
  await assert.rejects(journal.execute({ ...c, commandFingerprint: 'different' }, async () => ({ ok: true })), /identity reused/);
});
test('ambiguous host failures remain uncertain and never execute again', async () => {
  const c = command('uncertain'); let effects = 0;
  const first = await journal.execute(c, async () => { effects++; throw new Error('timeout after host effect'); });
  const replay = await journal.execute(c, async () => { effects++; return { ok: true }; });
  assert.equal(first.outcome, 'unknown'); assert.equal(effects, 1); assert.deepEqual(replay, first);
});
test('v2 command identity uses exact UTF-8 body and rejects tampering and foreign destinations', () => {
  const commandBody = JSON.stringify({ operationId: 'op', agentId: 'test-storage', tenantId: 'tenant-a', action: 'storage.create', data: { name: 'é' } });
  const envelope = { contractVersion: 2, taskId: 'op', eventId: 'event', commandBody, commandFingerprint: createHash('sha256').update(commandBody).digest('hex') };
  assert.equal(decodeCommand(envelope, 'test-storage').data.name, 'é');
  assert.throws(() => decodeCommand({ ...envelope, commandBody: commandBody + ' ' }, 'test-storage'));
  assert.throws(() => decodeCommand(envelope, 'foreign'));
  assert.throws(() => decodeCommand({ ...envelope, taskId: 'different' }, 'test-storage'));
});

test('process loss after a durable host effect replays uncertainty without a second effect', async () => {
  const { mkdtemp, readFile, rm } = await import('node:fs/promises');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');
  const { spawn } = await import('node:child_process');
  const dir = await mkdtemp(join(tmpdir(), 'ceph-crash-'));
  const path = join(dir, 'host-effect');
  const c = command('process-crash');
  try {
    const child = spawn(process.execPath, ['-r', 'ts-node/register', 'src/tests/crash-helper.ts'], {
      env: { ...process.env, CRASH_MONGO_URL: uri, CRASH_COMMAND: JSON.stringify(c), CRASH_EFFECT_PATH: path }, stdio: 'ignore',
    });
    const code = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); });
    assert.equal(code, 42);
    const result = await journal.execute(c, async () => { assert.fail('effect must never run again'); });
    assert.equal(result.outcome, 'unknown'); assert.equal(result.sequence, 1);
    assert.equal(await readFile(path, 'utf8'), 'effect\n');
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test('the shared backend UTF-8 command vector is accepted unchanged', () => {
  const fixture = require('./fixtures/task-v2.json');
  const decoded = decodeCommand(fixture, fixture.agentId);
  assert.equal(decoded.operationId, fixture.operationId);
  assert.deepEqual(decoded.data, fixture.data);
});
