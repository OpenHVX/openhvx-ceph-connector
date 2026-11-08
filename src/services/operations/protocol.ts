import { createHash } from 'node:crypto';

export interface Command {
  taskId: string;
  operationId: string;
  tenantId: string;
  agentId: string;
  action: string;
  data: Record<string, unknown>;
  commandFingerprint: string;
}
export interface Result {
  contractVersion: 2;
  taskId: string;
  operationId: string;
  tenantId: string;
  agentId: string;
  action: string;
  commandFingerprint: string;
  outcome: 'applied' | 'not_applied' | 'unknown';
  sequence: number;
  eventId: string;
  ok: boolean;
  result?: unknown;
  error?: string;
  finishedAt: string;
}
export class InvalidCommand extends Error {}
export function decodeCommand(input: unknown, agentId: string): Command {
  const envelope = input as any;
  if (!envelope || envelope.contractVersion !== 2 || typeof envelope.commandBody !== 'string' ||
      typeof envelope.eventId !== 'string' || !envelope.eventId ||
      createHash('sha256').update(envelope.commandBody, 'utf8').digest('hex') !== envelope.commandFingerprint) {
    throw new InvalidCommand('Invalid protocol v2 envelope');
  }
  let body: any;
  try { body = JSON.parse(envelope.commandBody); } catch { throw new InvalidCommand('Invalid command body'); }
  for (const key of ['operationId', 'tenantId', 'agentId', 'action']) {
    if (typeof body?.[key] !== 'string' || !body[key].trim()) throw new InvalidCommand('Missing command identity');
  }
  if (body.agentId !== agentId || body.operationId !== envelope.taskId ||
      !body.data || typeof body.data !== 'object' || Array.isArray(body.data)) throw new InvalidCommand('Command identity mismatch');
  if (body.data.tenantId !== undefined && body.data.tenantId !== body.tenantId) throw new InvalidCommand('Tenant identity mismatch');
  return { ...body, taskId: body.operationId, commandFingerprint: envelope.commandFingerprint };
}
export function makeResult(command: Command, outcome: Result['outcome'], sequence: number, result?: unknown): Result {
  return {
    contractVersion: 2, taskId: command.taskId, operationId: command.operationId,
    tenantId: command.tenantId, agentId: command.agentId, action: command.action,
    commandFingerprint: command.commandFingerprint, outcome, sequence,
    eventId: createHash('sha256').update(`${command.agentId}:${command.operationId}:${sequence}`).digest('hex'),
    ok: outcome === 'applied', ...(result !== undefined ? { result } : {}),
    ...(outcome === 'unknown' ? { error: 'Host outcome uncertain; operator reconciliation required' } : {}),
    finishedAt: new Date().toISOString(),
  };
}
