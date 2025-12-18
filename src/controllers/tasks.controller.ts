import type { CephController } from './ceph.controller';
import { randomUUID, randomBytes } from 'crypto';
import { AmqpService, TaskMessage, TaskResult } from '../services/amqp';
import { config } from '../config';
import type { TargetPortal } from '../types/iscsi/target';
import logger from '../lib/logger';

/**
 * Simple LIFO rollback helper to keep storage.create transactional.
 * Each registered step must undo a previously completed operation.
 */
class RollbackStack {
  private steps: Array<() => Promise<void>> = [];

  add(step: () => Promise<void>): void {
    this.steps.push(step);
  }

  async run(): Promise<void> {
    while (this.steps.length > 0) {
      const step = this.steps.pop();
      if (!step) continue;
      try {
        await step();
      } catch (err) {
        logger.warn('rollback step failed', { err });
      }
    }
  }
}

export class TasksController {
  constructor(
    private readonly ceph: CephController,
    private readonly amqp: AmqpService,
    private readonly storageId: string
  ) {}

  async handle(task: TaskMessage): Promise<TaskResult> {
    switch (task.action) {
      case 'inventory.refresh':
        return this.handleInventoryRefresh(task);
      case 'storage.create':
        return this.handleStorageCreate(task);
      case 'storage.delete':
        return this.handleStorageDelete(task);
      default:
        return this.fail(task, `Unknown action '${task.action}'`);
    }
  }

  async refreshInventory(): Promise<void> {
    await this.handleInventoryRefresh({
      taskId: 'inventory-refresh',
      action: 'inventory.refresh',
      data: {},
    });
  }

  private async handleInventoryRefresh(task: TaskMessage): Promise<TaskResult> {
    const inventory = await this.ceph.collectInventory(this.storageId);
    this.amqp.publishTelemetry(inventory);
    return this.ok(task, { inventory });
  }

  /**
   * storage.create flow:
   * - Validate payload and resolve the requested catalog snapshot
   * - Build destination image name using tenantId prefix and random suffix
   * - Clone catalog snapshot to destination image (or reuse existing)
   * - Wait for image readiness
   * - Expose iSCSI target (or reuse existing) then wait for readiness
   * - Refresh/publish inventory and return result
   * Any failure triggers rollback steps in reverse order.
   */
  private async handleStorageCreate(task: TaskMessage): Promise<TaskResult> {
    const data = task.data || {};
    const requestedNameRaw = (data as any).name || (data as any).refId || (data as any).image;
    const requestedName =
      requestedNameRaw !== undefined && requestedNameRaw !== null
        ? String(requestedNameRaw).trim()
        : '';
    const catalogImage = (data as any).imageId;
    const requestedSizeMbRaw = (data as any).sizeMB ?? (data as any).sizeMb ?? (data as any).size_mb;
    const requestedSizeMb = Number.isFinite(Number(requestedSizeMbRaw)) ? Number(requestedSizeMbRaw) : undefined;
    const requestedSizeBytes =
      requestedSizeMb !== undefined && requestedSizeMb > 0 ? requestedSizeMb * 1024 * 1024 : undefined;
    const tenantId = ((data as any).tenantId ?? task.tenantId ?? '').toString().trim() || undefined;
    const requestedSnapshot = 'base';
    const destPool = config.ceph.defaultPool;
    const destName = requestedName
      ? this.buildTenantImageName(tenantId ?? '', requestedName)
      : undefined;
    const iqn = destName ? `iqn.2001-07.org.openhvx:${destName}` : '';
    const lun = 0;
    const backstore = 'user:rbd';
    const rollback = new RollbackStack();
    const readinessTimeout = Number(process.env.CREATE_READINESS_TIMEOUT_MS || 180_000);

    if (!requestedName) {
      return this.fail(task, 'storage.create: missing required field "name"');
    }
    if (!catalogImage) {
      return this.fail(task, 'storage.create: missing required field "imageId" (catalog image id)');
    }
    if (!tenantId) {
      return this.fail(task, 'storage.create: missing required tenantId');
    }
    if (!destName) {
      return this.fail(task, 'storage.create: invalid tenantId or name');
    }

    const portals: TargetPortal[] = Array.isArray((data as any).portals)
      ? (data as any).portals
      : [
          {
            host: config.ceph.iscsi.gw.host,
            ip: config.ceph.iscsi.gw.ip,
          },
        ];

    const snapshot = await this.ceph.resolveCatalogSnapshotName(catalogImage, requestedSnapshot);
    if (!snapshot) {
      return this.fail(
        task,
        `storage.create: snapshot '${requestedSnapshot}' not found on catalog image '${catalogImage}'`
      );
    }

    try {
      const imageAlreadyPresent = await this.ceph.diskExists(destName, destPool);
      if (!imageAlreadyPresent) {
        await this.ceph.cloneFromCatalogSnapshot({
          destName,
          catalogImage,
          snapshot,
          destPool,
          destNamespace: undefined,
          objSize: undefined,
          features: undefined,
          stripeUnit: undefined,
          stripeCount: undefined,
          dataPool: undefined,
          configuration: undefined,
          metadata: undefined,
        });
        rollback.add(async () => {
          await this.ceph.deleteDisk(destName, destPool);
          logger.info('rollback: deleted cloned image', {
            pool: destPool,
            image: destName,
            tenantId,
          });
        });
      } else {
        logger.info('storage.create: image already exists, reusing as-is', {
          pool: destPool,
          image: destName,
          tenantId,
        });
      }

      await this.ceph.waitForImageReady(destPool, destName, readinessTimeout, undefined);

      if (requestedSizeBytes !== undefined) {
        const meta = await this.ceph.getDiskInfo(destName, destPool);
        if (!meta) {
          return this.fail(task, 'storage.create: failed to read destination image metadata for resize');
        }
        const currentSize = meta.sizeBytes;
        if (requestedSizeBytes > currentSize) {
          logger.info('storage.create: resizing image', {
            pool: destPool,
            image: destName,
            fromBytes: currentSize,
            toBytes: requestedSizeBytes,
          });
          await this.ceph.resizeDisk(destName, requestedSizeBytes, destPool);
          await this.ceph.waitForImageReady(destPool, destName, readinessTimeout, undefined);
        } else if (requestedSizeBytes < currentSize) {
          logger.warn('storage.create: requested size smaller than source, skipping shrink', {
            pool: destPool,
            image: destName,
            requestedSizeBytes,
            currentSize,
          });
        }
      }

      const existingTarget = await this.ceph.findIscsiTarget(iqn);
      const targetExisted = Boolean(existingTarget);
      if (!targetExisted) {
        await this.ceph.exposeIscsiTarget({
          iqn,
          pool: destPool,
          image: destName,
          namespace: undefined,
          portals,
          lun,
          backstore,
        });
        rollback.add(async () => {
          await this.ceph.deleteIscsiTarget(iqn);
          logger.info('rollback: deleted iscsi target', { iqn });
        });
      } else {
        logger.info('storage.create: target already exists, reusing', { iqn });
      }

      await this.ceph.waitForIscsiTarget(iqn, readinessTimeout);

      const inventory = await this.ceph.collectInventory(this.storageId);
      this.amqp.publishTelemetry(inventory);

      return this.ok(task, {
        disk: {
          refId: iqn,
          name: destName,
          id: destName,
          tenantId,
        },
        imageExisted: imageAlreadyPresent,
        targetExisted,
      });
    } catch (err: any) {
      const status = err?.response?.status;
      const data = err?.response?.data || {};
      const detail = data?.detail ?? data?.message;
      const cephCode = data?.code;
      const cephComponent = data?.component;
      const msg = detail || err?.message || String(err);

      logger.error('storage.create failed (ceph)', {
        step: 'clone',
        message: msg,
        status,
        code: cephCode,
        component: cephComponent,
        pool: destPool,
        image: destName,
        tenantId,
      });
      logger.debug('storage.create failed (raw)', { status, data, source: err?.config?.url, err });
      await rollback.run();
      return this.fail(task, msg);
    }
  }

  private ok(task: TaskMessage, result: unknown): TaskResult {
    return {
      taskId: task.taskId,
      agentId: this.storageId,
      ok: true,
      result,
      finishedAt: new Date().toISOString(),
    };
  }

  private fail(task: TaskMessage, error: string): TaskResult {
    return {
      taskId: task.taskId,
      agentId: this.storageId,
      ok: false,
      error,
      finishedAt: new Date().toISOString(),
    };
  }

  private async handleStorageDelete(task: TaskMessage): Promise<TaskResult> {
    const data = task.data || {};
    const refId = (data as any).refId || (data as any).iqn;
    const sizeMB = (data as any).sizeMB ?? (data as any).sizeMb;
    const pool = (data as any).pool || config.ceph.defaultPool;
    if (!refId) {
      return this.fail(task, 'storage.delete: missing required field "refId"');
    }

    const disks: Array<{ pool: string; image: string; namespace?: string }> = [];
    const iqnsToDelete: string[] = [];
    const refIsIqn = this.isIqn(refId);

    if (refIsIqn) {
      const target = await this.ceph.findIscsiTarget(refId);
      if (target) {
        iqnsToDelete.push(target.iqn);
        disks.push(...target.disks);
      }
      const derivedImage = this.deriveImageNameFromRef(refId);
      if (!disks.length && derivedImage) {
        disks.push({ pool, image: derivedImage });
      }
    } else {
      const matches = await this.ceph.findIscsiTargetsByDisk(refId, pool);
      matches.forEach((match) => {
        iqnsToDelete.push(match.iqn);
        match.disks
          .filter((d) => d.image === refId && (!pool || d.pool === pool))
          .forEach((d) => disks.push({ pool: d.pool, image: d.image, namespace: d.namespace }));
      });
      if (!disks.length) {
        disks.push({ pool, image: refId });
      }
    }

    const errors: string[] = [];
    let targetDeleted = false;

    if (iqnsToDelete.length > 0) {
      const uniqueIqns = Array.from(new Set(iqnsToDelete));
      for (const iqn of uniqueIqns) {
        try {
          await this.ceph.deleteIscsiTarget(iqn);
          targetDeleted = true;
        } catch (err: any) {
          const status = err?.response?.status ?? err?.status;
          if (status === 404) {
            logger.info('storage.delete: iscsi target already absent', { iqn });
            targetDeleted = true;
          } else {
            const msg = err?.message ?? String(err);
            errors.push(`failed to delete iscsi target ${iqn}: ${msg}`);
            logger.error('storage.delete: failed to delete iscsi target', { iqn, err });
          }
        }
      }
    } else {
      logger.warn('storage.delete: iscsi target not found', { refId });
    }

    const seen = new Set<string>();
    const deletedDisks: Array<{ pool: string; image: string; namespace?: string }> = [];
    for (const disk of disks) {
      const key = `${disk.pool}/${disk.image}${disk.namespace ? `@${disk.namespace}` : ''}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const exists = await this.ceph.diskExists(disk.image, disk.pool, (disk as any).namespace);
      if (!exists) {
        logger.info('storage.delete: disk already absent before delete', {
          pool: disk.pool,
          image: disk.image,
          namespace: (disk as any).namespace,
        });
        deletedDisks.push({ pool: disk.pool, image: disk.image, namespace: (disk as any).namespace });
        continue;
      }
      try {
        await this.ceph.deleteDisk(disk.image, disk.pool, (disk as any).namespace);
        deletedDisks.push({ pool: disk.pool, image: disk.image, namespace: (disk as any).namespace });
      } catch (err: any) {
        const status = err?.response?.status ?? err?.status;
        if (status === 404) {
          logger.info('storage.delete: disk already absent', {
            pool: disk.pool,
            image: disk.image,
            namespace: (disk as any).namespace,
          });
          deletedDisks.push({ pool: disk.pool, image: disk.image, namespace: (disk as any).namespace });
        } else {
          const msg = err?.message ?? String(err);
          errors.push(`failed to delete disk ${key}: ${msg}`);
          logger.error('storage.delete: failed to delete disk', {
            pool: disk.pool,
            image: disk.image,
            namespace: (disk as any).namespace,
            err,
          });
        }
      }
    }

    try {
      const inventory = await this.ceph.collectInventory(this.storageId);
      this.amqp.publishTelemetry(inventory);
    } catch (err) {
      logger.warn('storage.delete: failed to publish refreshed inventory', { err });
    }

    if (errors.length > 0) {
      return this.fail(task, errors.join('; '));
    }

    return this.ok(task, {
      refId,
      targetDeleted,
      deletedDisks,
      sizeMB,
    });
  }

  private deriveImageNameFromRef(refId: string): string | null {
    // For IQN like "iqn.2001-07.org.openhvx:my-image", keep suffix after last colon
    const parts = refId.split(':');
    const maybe = parts[parts.length - 1];
    return maybe ? maybe.trim() || null : null;
  }

  private isIqn(value: string): boolean {
    return /^iqn\.\d{4}-\d{2}\./i.test(value);
  }

  private buildTenantImageName(tenantId: string, name: string): string {
    const cleanTenant = this.normalizeSegment(tenantId);
    const cleanName = this.normalizeSegment(name);
    if (!cleanTenant || !cleanName) return '';
    const base = cleanName.startsWith(`${cleanTenant}-`) ? cleanName : `${cleanTenant}-${cleanName}`;
    return `${base}-${this.generateUid()}`;
  }

  private normalizeSegment(value: string): string {
    return value
      .trim()
      .replace(/[^A-Za-z0-9._-]+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-+|-+$/g, '');
  }

  private generateUid(): string {
    if (typeof randomUUID === 'function') {
      return randomUUID().split('-')[0];
    }
    return randomBytes(4).toString('hex');
  }
}
