import './setup';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CephController } from '../controllers/ceph.controller';
import { TasksController } from '../controllers/tasks.controller';

test('failed or partial image listings cannot prove disk absence', async () => {
  const failed = new CephController({ getRBDs: async () => { throw new Error('unavailable'); } } as any);
  await assert.rejects(failed.diskExists('disk'), /unavailable/);
  let page = 0;
  const partial = new CephController({ getRBDs: async () => {
    if (page++) throw new Error('next page unavailable');
    return Array.from({ length: 200 }, (_, i) => ({ name: `image-${i}`, size: 1 }));
  } } as any);
  await assert.rejects(partial.diskExists('missing'), /next page unavailable/);
});
test('namespace filtering does not stop pagination before matching images', async () => {
  let page = 0;
  const ceph = new CephController({ getRBDs: async () => page++ ? [{ name: 'wanted', namespace: 'tenant', size: 1 }] : Array.from({ length: 200 }, (_, i) => ({ name: `other-${i}`, namespace: 'other', size: 1 })) } as any);
  assert.equal(await ceph.diskExists('wanted', 'rbd', 'tenant'), true);
});
test('shared target deletion is rejected before any mutation', async () => {
  let mutations = 0;
  const tasks = new TasksController({ findIscsiTargetsByDisk: async () => [{ iqn: 'shared', disks: [{ pool: 'rbd', image: 'tenant-a-disk' }, { pool: 'rbd', image: 'someone-else' }] }], deleteIscsiTarget: async () => { mutations++; } } as any, {} as any, 'test-storage');
  const result = await tasks.handle({ taskId: 'op', tenantId: 'tenant-a', action: 'storage.delete', data: { target: { refId: 'tenant-a-disk' } } });
  assert.equal(result.ok, false); assert.equal(mutations, 0);
});
test('failed target deletion retains the backing image', async () => {
  let deletedImages = 0;
  const tasks = new TasksController({ findIscsiTargetsByDisk: async () => [{ iqn: 'target', disks: [{ pool: 'rbd', image: 'tenant-a-disk' }] }], deleteIscsiTarget: async () => { throw new Error('timeout'); }, deleteDisk: async () => { deletedImages++; } } as any, {} as any, 'test-storage');
  const result = await tasks.handle({ taskId: 'op', tenantId: 'tenant-a', action: 'storage.delete', data: { target: { refId: 'tenant-a-disk' } } });
  assert.equal(result.ok, false); assert.equal(deletedImages, 0);
});

test('resize readiness observes requested size rather than mere image presence', async () => {
  const ceph = new CephController({} as any);
  let reads = 0;
  ceph.getDiskInfo = async () => ({ name: 'disk', pool: 'rbd', sizeBytes: ++reads === 1 ? 10 : 20 });
  await ceph.waitForImageSize('rbd', 'disk', 20, 1000, 1);
  assert.equal(reads, 2);
  ceph.getDiskInfo = async () => ({ name: 'disk', pool: 'rbd', sizeBytes: 10 });
  await assert.rejects(ceph.waitForImageSize('rbd', 'disk', 20, 10, 1), /completion could not be observed/);
});
