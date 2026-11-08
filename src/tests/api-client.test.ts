import './setup';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { CephApi } from '../services/ceph-api';

test('a failed Ceph login does not poison all subsequent requests', async () => {
  let logins = 0;
  const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json');
    if (req.url === '/api/auth') {
      logins++;
      res.writeHead(logins === 1 ? 503 : 200).end(JSON.stringify(logins === 1 ? {} : { token: 'test-token' }));
    } else res.end(JSON.stringify({ health: 'HEALTH_OK' }));
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as any).port;
  const client = new CephApi({ baseUrl: `http://127.0.0.1:${port}`, username: 'test', password: 'test' });
  try {
    await assert.rejects(client.getHealthSummary());
    assert.deepEqual(await client.getHealthSummary(), { health: 'HEALTH_OK' });
    assert.equal(logins, 2);
  } finally { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); }
});

test('API negotiation never retries an ambiguous mutation after a non-415 failure', async () => {
  let mutations = 0;
  const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json');
    if (req.url === '/api/auth') { res.end(JSON.stringify({ token: 'test-token' })); return; }
    mutations++;
    res.writeHead(mutations === 1 ? 415 : 503).end(JSON.stringify({ detail: "endpoint is '2.0'" }));
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const client = new CephApi({ baseUrl: `http://127.0.0.1:${(server.address() as any).port}`, username: 'test', password: 'test' });
  try {
    await assert.rejects(client.createBlockImage({ pool: 'rbd', name: 'image', sizeBytes: 1 }));
    assert.equal(mutations, 2);
  } finally { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); }
});
