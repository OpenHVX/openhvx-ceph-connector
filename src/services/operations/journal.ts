import { Collection, MongoClient } from 'mongodb';
import { Command, InvalidCommand, makeResult, Result } from './protocol';

interface Entry {
  _id: string;
  fingerprint: string;
  command: Omit<Command, 'data'>;
  result: Result;
}
export class OperationJournal {
  private client?: MongoClient;
  private entries?: Collection<Entry>;
  async open(uri: string): Promise<void> {
    this.client = new MongoClient(uri, { writeConcern: { w: 'majority', j: true }, readConcern: { level: 'majority' }, readPreference: 'primary' });
    await this.client.connect();
    const hello = await this.client.db().admin().command({ hello: 1 });
    if (!hello.setName && hello.msg !== 'isdbgrid') throw new Error('Journal requires MongoDB replica set');
    this.entries = this.client.db().collection<Entry>('cephoperations');
  }
  async healthy(): Promise<boolean> {
    try { await this.client?.db().command({ ping: 1 }, { timeoutMS: 3000 }); return !!this.client; } catch { return false; }
  }
  async close(): Promise<void> { await this.client?.close(); }
  async execute(command: Command, effect: () => Promise<{ ok: boolean; result?: unknown }>): Promise<Result> {
    if (!this.entries) throw new Error('Journal unavailable');
    const id = JSON.stringify([command.agentId, command.operationId]);
    const { data, ...identity } = command;
    // Persist uncertainty before any host effect. A crash can only replay this result,
    // never grant a second executor permission to mutate Ceph.
    const entry: Entry = { _id: id, fingerprint: command.commandFingerprint, command: identity,
      result: makeResult(command, 'unknown', 1) };
    try { await this.entries.insertOne(entry); }
    catch (error: any) {
      if (error.code !== 11000) throw error;
      const existing = await this.entries.findOne({ _id: id });
      if (!existing || existing.fingerprint !== command.commandFingerprint) throw new InvalidCommand('Operation identity reused with different command');
      return existing.result;
    }
    let result: Result;
    try {
      const response = await effect();
      result = makeResult(command, response.ok ? 'applied' : 'unknown', 2, response.result);
    } catch {
      result = makeResult(command, 'unknown', 2);
    }
    await this.entries.updateOne({ _id: id, fingerprint: command.commandFingerprint }, { $set: { result } });
    return result;
  }
}
