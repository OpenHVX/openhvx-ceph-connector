import type { CephController } from './ceph.controller';
import { createHash } from 'node:crypto';
import { AmqpService, TaskMessage, TaskResult } from '../services/amqp';
import { config, positiveInteger } from '../config';
import type { TargetPortal } from '../types/iscsi/target';
import logger from '../lib/logger';

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
   * - Build destination image name using tenantId prefix and operation-derived suffix
   * - Clone catalog snapshot to a dedicated destination image
   * - Wait for image readiness
   * - Expose a dedicated iSCSI target and verify its disk mapping
   * - Refresh/publish inventory and return result
   * Failures preserve partial resources for operator reconciliation.
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
    const tenantId = task.tenantId;
    const requestedSnapshot = 'base';
    const destPool = config.ceph.defaultPool;
    const destName = requestedName
      ? this.buildTenantImageName(tenantId ?? '', requestedName, task.taskId)
      : undefined;
    const iqn = destName ? `iqn.2001-07.org.openhvx:${destName}` : '';
    const lun = 0;
    const backstore = 'user:rbd';

    const readinessTimeout = positiveInteger('CREATE_READINESS_TIMEOUT_MS', 180000);

    if (!Number.isSafeInteger(requestedSizeMb) || !requestedSizeMb || requestedSizeMb <= 0) return this.fail(task, 'sizeMB must be a positive integer');
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

    const portals: TargetPortal[] = [{ host: config.ceph.iscsi.gw.host, ip: config.ceph.iscsi.gw.ip }];

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

      } else {
        return this.fail(task, 'Destination image already exists; ownership requires reconciliation');

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
          await this.ceph.waitForImageSize(destPool, destName, requestedSizeBytes, readinessTimeout);
        } else if (requestedSizeBytes < currentSize) {
          return this.fail(task, 'Requested size is smaller than the source image; partial clone retained');
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

      } else {
        return this.fail(task, 'Destination target already exists; ownership requires reconciliation');
      }

      await this.ceph.waitForIscsiTarget(iqn, readinessTimeout);
      const readyTarget = await this.ceph.findIscsiTarget(iqn);
      if (!readyTarget || readyTarget.disks.length !== 1 || readyTarget.disks[0].pool !== destPool || readyTarget.disks[0].image !== destName) {
        return this.fail(task, 'Target mapping could not be verified');
      }

      await this.refreshInventory().catch(() => logger.warn('Post-operation inventory deferred'));

      return this.ok(task, {
        disk: {
          refId: destName,
          iqn,
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
    const target = (data as any).target;
    const refId = target?.refId;
    if (target?.agentId && target.agentId !== this.storageId) return this.fail(task, 'Target agent mismatch');
    if ((data as any).refId && (data as any).refId !== refId) return this.fail(task, 'Conflicting target identity');
    const sizeMB = (data as any).sizeMB ?? (data as any).sizeMb;
    const pool = (data as any).pool || config.ceph.defaultPool;
    if (!refId) {
      return this.fail(task, 'storage.delete: missing required field "refId"');
    }

    if (pool !== config.ceph.defaultPool) return this.fail(task, 'Unsupported pool');
    if (typeof refId !== 'string' || !refId.startsWith(`${this.normalizeSegment(task.tenantId || '')}-`)) return this.fail(task, 'Target does not belong to tenant naming scope');

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
      if (matches.some(match => match.disks.some(d => d.image !== refId || d.pool !== pool))) return this.fail(task, 'Refusing to delete a shared iSCSI target');
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
          await this.ceph.waitForTargetAbsent(iqn, positiveInteger('CREATE_READINESS_TIMEOUT_MS', 180000));
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

    if (errors.length) return this.fail(task, 'iSCSI target deletion uncertain; disk retained');
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
        await this.ceph.deleteDisk(disk.image, disk.pool, disk.namespace);
        await this.ceph.waitForDiskAbsent(disk.pool, disk.image, disk.namespace, positiveInteger('CREATE_READINESS_TIMEOUT_MS', 180000));
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

  private buildTenantImageName(tenantId: string, name: string, operationId: string): string {
    const cleanTenant = this.normalizeSegment(tenantId);
    const cleanName = this.normalizeSegment(name);
    if (!cleanTenant || !cleanName) return '';
    const base = cleanName.startsWith(`${cleanTenant}-`) ? cleanName : `${cleanTenant}-${cleanName}`;
    return `${base.slice(0, 120)}-${createHash('sha256').update(operationId).digest('hex').slice(0, 32)}`;
  }

  private normalizeSegment(value: string): string {
    return value
      .trim()
      .replace(/[^A-Za-z0-9._-]+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-+|-+$/g, '');
  }

}
