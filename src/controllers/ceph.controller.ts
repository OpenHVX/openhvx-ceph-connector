import { config } from '../config';
import { CephApi, CloneImageParams } from '../services/ceph-api';
import { StorageInventoryV1 } from '../types/inventory';
import type { TargetPortal } from '../types/iscsi/target';
import logger from '../lib/logger';

const BYTES_PER_GB = 1024 ** 3;

type DiskParams = {
  name: string;
  sizeGb: number;
  pool?: string;
  namespace?: string;
};

type RbdSnapshot = {
  id?: number | string;
  name: string;
  sizeBytes?: number;
  namespace?: string;
  timestamp?: string;
  isProtected?: boolean;
  usedBytes?: number | null;
  diskUsage?: number;
};

type RbdEntry = {
  name: string;
  sizeBytes: number;
  usedBytes?: number;
  pool: string;
  namespace?: string;
  snapshots?: RbdSnapshot[];
};

type IscsiDiskRef = { pool: string; image: string; namespace?: string };

export class CephController {
  constructor(
    private readonly api: CephApi,
    private readonly defaultPool = config.ceph.defaultPool
  ) {}

  private isImageMissingError(err: any): boolean {
    const code = err?.response?.data?.code ?? err?.code;
    const detail = err?.response?.data?.detail ?? err?.message ?? '';
    return code === 'image_does_not_exist' || /image.+does not exist/i.test(String(detail));
  }

  /**
   * Create a new RBD image, ensuring the namespace exists when provided.
   */
  async createDisk(params: DiskParams): Promise<unknown> {
    const pool = this.resolvePool(params.pool);
    if (params.namespace) {
      await this.ensureNamespace(pool, params.namespace);
    }
    const sizeBytes = params.sizeGb * BYTES_PER_GB;
    return this.api.createBlockImage({
      pool,
      name: params.name,
      sizeBytes,
      namespace: params.namespace,
    });
  }

  /**
   * Delete an RBD image from the given pool/namespace.
   */
  async deleteDisk(name: string, pool?: string, namespace?: string): Promise<unknown> {
    const selectedPool = this.resolvePool(pool);
    return this.api.deleteBlockImage(selectedPool, name, namespace);
  }

  /**
   * Remove an RBD image that was previously moved to the trash.
   */
  async deleteDiskFromTrash(name: string, pool?: string, namespace?: string): Promise<unknown> {
    const selectedPool = this.resolvePool(pool);
    return this.api.deleteBlockImageFromTrash(selectedPool, name, namespace);
  }

  /**
   * Move an RBD image to the trash with an optional delayed deletion.
   */
  async moveDiskToTrash(name: string, pool?: string, delay = 0, namespace?: string): Promise<unknown> {
    const selectedPool = this.resolvePool(pool);
    return this.api.moveBlockImageToTrash(selectedPool, name, delay, namespace);
  }

  /**
   * Check if a disk exists in the given pool/namespace.
   */
  async diskExists(name: string, pool?: string, namespace?: string): Promise<boolean> {
    const selectedPool = this.resolvePool(pool);
    try {
      const entries = await this.listRbdEntries(selectedPool, 200, namespace);
      return entries.some((e) => e.name === name && (namespace === undefined || e.namespace === namespace));
    } catch {
      return false;
    }
  }

  /**
   * Clone an image from the catalog pool snapshot into the destination pool/namespace.
   */
  async cloneFromCatalogSnapshot(params: {
    destName: string;
    catalogImage: string;
    snapshot: string;
    destPool?: string;
    destNamespace?: string;
    catalogPool?: string;
    objSize?: number | null;
    features?: number | null;
    stripeUnit?: number | null;
    stripeCount?: number | null;
    dataPool?: string | null;
    configuration?: string | Record<string, unknown> | Array<Record<string, unknown>> | null;
    metadata?: Record<string, unknown> | null;
  }): Promise<unknown> {
    if (params.destNamespace) {
      await this.ensureNamespace(params.destPool ?? this.defaultPool, params.destNamespace);
    }

    // Ensure the source snapshot is protected before clone
    try {
      await this.api.protectSnapshot(params.catalogPool ?? 'catalog', params.catalogImage, params.snapshot);
    } catch (err) {
      logger.warn('cloneFromCatalogSnapshot: failed to protect snapshot (may already be protected)', {
        pool: params.catalogPool ?? 'catalog',
        image: params.catalogImage,
        snapshot: params.snapshot,
        err,
      });
    }

    const payload: CloneImageParams = {
      srcPool: params.catalogPool ?? 'catalog',
      srcImage: params.catalogImage,
      snapshot: params.snapshot,
      destPool: params.destPool ?? this.defaultPool,
      destImage: params.destName,
      destNamespace: params.destNamespace,
      objSize: params.objSize,
      features: params.features,
      stripeUnit: params.stripeUnit,
      stripeCount: params.stripeCount,
      dataPool: params.dataPool,
      configuration: params.configuration,
      metadata: params.metadata,
    };

    try {
      logger.info('cloneFromCatalogSnapshot: sending payload', {
        path: `${payload.srcPool}/${payload.srcImage}@${payload.snapshot}`,
        destPool: payload.destPool,
        destImage: payload.destImage,
        destNamespace: payload.destNamespace ?? '',
      });
      return await this.api.cloneBlockImageFromSnapshot(payload);
    } catch (err: any) {
      const status = err?.response?.status ?? err?.status;
      const data = err?.response?.data;
      const cephDetail = data?.detail || data?.message || '';
      const cephCode = data?.code ?? err?.code ?? err?.errno;
      const compact = {
        status,
        detail: data?.detail,
        code: data?.code,
        component: data?.component,
        task: data?.task?.metadata,
        payload: {
          child_pool_name: payload.destPool,
          child_image_name: payload.destImage,
          child_namespace: payload.destNamespace,
          configuration: payload.configuration,
        },
      };

      logger.error('cloneFromCatalogSnapshot: ceph clone failed (compact)', compact);
      logger.debug('cloneFromCatalogSnapshot: ceph clone failed (raw)', {
        srcPool: payload.srcPool,
        srcImage: payload.srcImage,
        snapshot: payload.snapshot,
        destPool: payload.destPool,
        destNamespace: payload.destNamespace,
        destImage: payload.destImage,
        status,
        data,
        err,
      });
      const message = cephDetail ? `Ceph clone failed: ${cephDetail}` : err?.message || 'unknown clone error';
      const wrapped = new Error(message);
      (wrapped as any).cause = err;
      throw wrapped;
    }
  }

  /**
   * Ensure a namespace exists on a pool. Creates it when missing.
   */
  async ensureNamespace(pool: string, namespace: string): Promise<void> {
    if (!namespace) return;
    try {
      const res = await this.api.listNamespaces(pool);
      const namespaces = this.extractNamespaceNames(res);
      if (namespaces.includes(namespace)) return;
    } catch (err) {
      logger.warn('ensureNamespace: failed to list namespaces, will attempt create', { pool, namespace, err });
    }

    try {
      await this.api.createNamespace(pool, namespace);
      logger.info('ensureNamespace: created namespace', { pool, namespace });
    } catch (err: any) {
      const msg = err?.response?.data?.detail ?? err?.message ?? String(err);
      logger.error('ensureNamespace: failed to create namespace', {
        pool,
        namespace,
        status: err?.response?.status ?? err?.status,
        detail: err?.response?.data ?? err,
      });
      throw new Error(`Failed to ensure namespace '${namespace}' on pool '${pool}': ${msg}`);
    }
  }

  /**
   * Pick a catalog snapshot by name if it exists; returns null otherwise.
   */
  async resolveCatalogSnapshotName(image: string, preferred: string): Promise<string | null> {
    const entry = await this.findCatalogImage(image);
    if (!entry) {
      logger.warn('catalog image not found', { pool: 'catalog', image });
      return null;
    }

    const snapshots = this.extractSnapshots(entry);
    if (snapshots.length === 0) {
      logger.warn('no snapshots on catalog image', { pool: 'catalog', image });
      return null;
    }

    const snapNames = snapshots.map((s) => s.name);
    logger.info('catalog snapshots detected', { pool: 'catalog', image, snapshots: snapNames });

    const match = snapshots.find((s) => s.name === preferred);
    if (!match) {
      logger.warn('preferred snapshot not found', { pool: 'catalog', image, preferred, snapshots: snapNames });
      return null;
    }
    return match.name;
  }

  /**
   * Normalize snapshots coming from different Ceph dashboard representations.
   */
  private extractSnapshots(entry: any): RbdSnapshot[] {
    const snaps =
      (entry as any)?.snapshots ||
      (entry as any)?.snapshot_list ||
      (Array.isArray((entry as any)?.value) ? (entry as any).value : []);
    if (!Array.isArray(snaps)) return [];

    return snaps
      .map((s: any): RbdSnapshot | null => {
        const name = typeof s === 'string' ? s : s?.name ?? s?.id;
        if (!name) return null;
        return {
          id: s?.id ?? s?.snap_id ?? undefined,
          name: String(name),
          sizeBytes: this.toNumber(s?.size ?? s?.size_bytes ?? s?.disk_usage ?? s?.used_bytes),
          namespace: s?.namespace ?? s?.pool_namespace ?? undefined,
          timestamp: s?.timestamp ?? s?.created_at ?? undefined,
          isProtected: s?.is_protected ?? s?.protected ?? undefined,
          usedBytes: s?.used_bytes ?? undefined,
          diskUsage: this.toNumber(s?.disk_usage),
        };
      })
      .filter(Boolean) as RbdSnapshot[];
  }

  /**
   * Wait until an image appears in Ceph or time out.
   */
  async waitForImageReady(
    pool: string,
    image: string,
    timeoutMs = 60_000,
    pollMs = 3_000,
    namespace?: string
  ): Promise<void> {
    const start = Date.now();
    let attempt = 0;
    let lastEntriesNames: string[] = [];
    while (Date.now() - start < timeoutMs) {
      attempt += 1;
      const entries = await this.listRbdEntries(pool, 200, namespace);
      const found = entries.find(
        (i: any) => (i?.name === image || i?.image === image || i?.id === image) &&
          (namespace === undefined || i?.namespace === namespace)
      );
      lastEntriesNames = entries
        .map((i: any) => i?.name ?? i?.image ?? i?.id)
        .filter(Boolean)
        .slice(0, 10);
      if (attempt === 1 || attempt % 5 === 0) {
        const sample = entries[0];
        const sampleKeys = sample ? Object.keys(sample).slice(0, 10) : [];
        logger.info('waitForImageReady: poll', {
          pool,
          image,
          attempt,
          found: Boolean(found),
          entries: lastEntriesNames,
          sampleKeys,
          sample,
        });
      }
      if (found) return;
      await this.sleep(pollMs);
    }
    logger.error('waitForImageReady: timeout', { pool, image, timeoutMs, lastEntries: lastEntriesNames });
    throw new Error(`Image ${pool}/${image} not ready after ${timeoutMs}ms`);
  }

  /**
   * Wait until an iSCSI target is available on the gateway.
   */
  async waitForIscsiTarget(iqn: string, timeoutMs = 60_000, pollMs = 3_000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const targetsRes = await this.api.getTargets();
      const targets = Array.isArray(targetsRes) ? targetsRes : [];
      const found = targets.find((t: any) => t?.target_iqn === iqn || t?.iqn === iqn);
      if (found) return;
      await this.sleep(pollMs);
    }
    throw new Error(`Target ${iqn} not ready after ${timeoutMs}ms`);
  }

  private async findCatalogImage(image: string): Promise<any | null> {
    try {
      const detail = await this.api.getImage('catalog', image);
      const entries = this.extractRbdEntries(detail, 'catalog');
      const found = entries.find((i: any) => i?.name === image || i?.id === image || i?.image === image);
      if (found) return found;
      logger.warn('findCatalogImage: detail returned no match', {
        image,
        receivedEntries: entries.map((e: any) => e?.name ?? e?.id).filter(Boolean),
      });
    } catch (err) {
      logger.warn('findCatalogImage: detail lookup failed, falling back to listing', { image, err });
    }

    const entries = await this.listRbdEntries('catalog');
    return entries.find((i: any) => i?.name === image || i?.id === image) ?? null;
  }

  /**
   * Create or update an iSCSI target exposing a single RBD image.
   */
  async exposeIscsiTarget(params: {
    iqn: string;
    pool: string;
    image: string;
    portals: TargetPortal[];
    lun?: number;
    backstore?: string;
    namespace?: string;
  }): Promise<unknown> {
    const buildDisk = (image: string) => ({
      pool: params.pool,
      image,
      backstore: params.backstore ?? 'user:rbd',
      controls: {},
      lun: params.lun ?? 0,
    });

    const auth = config.ceph.iscsi.auth;
    const basePayload = {
      iqn: params.iqn,
      portals: params.portals,
      targetControls: { immediate_data: true },
      aclEnabled: false,
      ...(auth ? { auth } : {}),
    } as const;

    try {
      return await this.api.setTarget({
        ...basePayload,
        disks: [buildDisk(params.image)],
      });
    } catch (err) {
      // Fallback: try namespaced image string if the API complains the image is missing
      if (params.namespace && this.isImageMissingError(err)) {
        logger.warn('exposeIscsiTarget: retry with namespaced image string', {
          iqn: params.iqn,
          pool: params.pool,
          image: params.image,
          namespace: params.namespace,
        });
        return this.api.setTarget({
          ...basePayload,
          disks: [buildDisk(`${params.namespace}/${params.image}`)],
        });
      }
      throw err;
    }
  }

  async deleteIscsiTarget(iqn: string): Promise<unknown> {
    return this.api.deleteTarget(iqn);
  }

  /**
   * Find an iSCSI target by IQN and return its disks.
   */
  async findIscsiTarget(
    iqn: string
  ): Promise<{ iqn: string; disks: IscsiDiskRef[] } | null> {
    const targetsRes = await this.api.getTargets();
    const targets = Array.isArray(targetsRes) ? targetsRes : [];
    const match = targets.find((t: any) => t?.target_iqn === iqn || t?.iqn === iqn);
    if (!match) return null;

    const parsedDisks = this.parseIscsiDisks(match?.disks);

    return {
      iqn: match.target_iqn ?? match.iqn ?? iqn,
      disks: parsedDisks,
    };
  }

  /**
   * Locate all iSCSI targets serving a specific image (optionally limited to a pool).
   */
  async findIscsiTargetsByDisk(
    image: string,
    pool?: string
  ): Promise<Array<{ iqn: string; disks: IscsiDiskRef[] }>> {
    const targetsRes = await this.api.getTargets();
    const targets = Array.isArray(targetsRes) ? targetsRes : [];

    return targets
      .map((t: any) => {
        const parsedDisks = this.parseIscsiDisks(t?.disks);

        const matches = parsedDisks.filter(
          (d: IscsiDiskRef) => d.image === image && (!pool || d.pool === pool)
        );
        if (matches.length === 0) return null;

        return {
          iqn: t?.target_iqn ?? t?.iqn,
          disks: parsedDisks,
        };
      })
      .filter(Boolean) as Array<{ iqn: string; disks: IscsiDiskRef[] }>;
  }

  /**
   * Return Ceph health summary as provided by the API.
   */
  async status(): Promise<unknown> {
    return this.api.getHealthSummary();
  }

  /**
   * Build a StorageInventoryV1 object from Ceph API calls.
   * Uses best-effort parsing with safe defaults to avoid crashing on partial responses.
   */
  async collectInventory(storageId: string): Promise<StorageInventoryV1> {
    const [healthRes, targetsRes, catalogRes] = await Promise.all([
      this.api.getHealthSummary(),
      this.api.getTargets(),
      this.api.getRBDs('catalog').catch(() => []), // catalog pool is optional
    ]);

    const targets = Array.isArray(targetsRes) ? targetsRes : [];
    const rbdMetaByPool = await this.loadRbdMetadataForTargets(targets);

    const cluster = this.extractCluster(healthRes);
    const volumes = this.extractIscsiVolumes(targets, rbdMetaByPool);

    const catalog = this.extractRbdEntries(catalogRes, 'catalog').map((item) => ({
      refId: item.name,
      pool: item.pool,
      name: item.name,
      sizeBytes: item.sizeBytes,
    }));

    const capacity = this.extractCapacity(healthRes, volumes);

    return {
      schemaVersion: 'storage.inventory.v1',
      storageId,
      collectedAt: new Date().toISOString(),
      cluster,
      capacity,
      volumes,
      catalog,
    };
  }

  private extractCluster(raw: any): StorageInventoryV1['cluster'] {
    const fsid = (raw as any)?.fsid ?? 'unknown';
    const health =
      (raw as any)?.health?.status ??
      (raw as any)?.health?.overall_status ??
      (raw as any)?.overall_status ??
      'UNKNOWN';
    return { fsid, health };
  }

  private extractCapacity(raw: any, volumes: StorageInventoryV1['volumes']): StorageInventoryV1['capacity'] {
    const pgmap = (raw as any)?.pgmap ?? {};
    const totalBytes = this.toNumber(pgmap.bytes_total) || this.sum(volumes.map((v) => v.sizeBytes));
    const usedBytes =
      this.toNumber(pgmap.bytes_used) || this.sum(volumes.map((v) => v.usedBytes ?? 0));
    const availBytes =
      this.toNumber(pgmap.bytes_avail) || Math.max(totalBytes - usedBytes, 0);

    return {
      totalBytes,
      usedBytes,
      availBytes,
    };
  }

  private extractRbdEntries(
    raw: any,
    pool: string
  ): RbdEntry[] {
    const normalized = Array.isArray(raw) ? raw : raw ? [raw] : [];
    const entries = normalized.flatMap((item: any) => (Array.isArray(item?.value) ? item.value : [item]));

    return entries
      .map((item: any) => ({
        name: item?.name ?? item?.image ?? item?.id,
        sizeBytes: this.toNumber(item?.size ?? item?.size_bytes),
        usedBytes: item?.used_size ?? item?.used_bytes,
        pool: item?.pool_name ?? pool,
        namespace: item?.namespace ?? item?.pool_namespace ?? '',
        snapshots: this.extractSnapshots(item),
      }))
      .filter((i) => Boolean(i.name));
  }

  private async loadRbdMetadataForTargets(
    targets: any[]
  ): Promise<
    Record<string, Record<string, { sizeBytes: number; usedBytes?: number; namespace?: string; snapshots?: RbdSnapshot[] }>>
  > {
    const pools = new Set<string>();
    targets.forEach((t: any) => {
      const disks = Array.isArray(t?.disks) ? t.disks : [];
      disks.forEach((d: any) => {
        const pool = d?.pool ?? this.defaultPool;
        if (pool) pools.add(pool);
      });
    });

    const metaByPool: Record<
      string,
      Record<string, { sizeBytes: number; usedBytes?: number; namespace?: string }>
    > = {};
    await Promise.all(
      Array.from(pools).map(async (pool) => {
        try {
          const entries = await this.listRbdEntries(pool);
          metaByPool[pool] = entries.reduce<
            Record<string, { sizeBytes: number; usedBytes?: number; namespace?: string; snapshots?: RbdSnapshot[] }>
          >((acc, entry) => {
            const key = `${entry.namespace ?? ''}::${entry.name}`;
            acc[key] = {
              sizeBytes: entry.sizeBytes,
              usedBytes: entry.usedBytes,
              namespace: entry.namespace,
              snapshots: entry.snapshots,
            };
            return acc;
          }, {});
        } catch {
          // best effort; leave pool empty on failure
        }
      })
    );

    return metaByPool;
  }

  private extractIscsiVolumes(
    targets: any[],
    metaByPool: Record<string, Record<string, { sizeBytes: number; usedBytes?: number; namespace?: string }>>
  ): StorageInventoryV1['volumes'] {
    return targets.flatMap((t: any) => {
      const iqn = t?.target_iqn ?? t?.iqn;
      if (!iqn) return [];
      const sessionCount = this.toNumber(t?.info?.num_sessions ?? t?.num_sessions ?? t?.sessions);
      const isAttached = sessionCount > 0;
      const portals = Array.isArray(t?.portals) ? t.portals : [];
      const rawPortal = portals[0];
      const portal = rawPortal
        ? {
            host: rawPortal?.host ?? rawPortal?.gateway_name ?? rawPortal?.gateway ?? rawPortal?.name,
            ip:
              rawPortal?.ip ??
              rawPortal?.ip_address ??
              rawPortal?.ipAddress ??
              rawPortal?.address,
          }
        : undefined;
      const portalWithIp = portal?.ip ? portal : undefined;

      const disks = Array.isArray(t?.disks) ? t.disks : [];
      return disks
        .map((d: any) => {
          const pool = d?.pool ?? this.defaultPool;
          const image = d?.image ?? d?.name;
          const namespace = d?.pool_namespace ?? d?.namespace ?? '';
          if (!pool || !image) return null;
          const metaKey = `${namespace ?? ''}::${image}`;
          const meta = metaByPool[pool]?.[metaKey];
          return {
            refId: image,
            iqn,
            portal: portalWithIp,
            pool,
            sizeBytes: meta?.sizeBytes ?? this.toNumber(d?.size),
            usedBytes: meta?.usedBytes,
            isAttached,
          };
        })
        .filter(Boolean) as StorageInventoryV1['volumes'];
    });
  }

  private toNumber(value: unknown): number {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  }

  private sum(values: Array<number | undefined>): number {
    return values.reduce((acc: number, val) => acc + (val ?? 0), 0);
  }

  private resolvePool(pool?: string): string {
    return pool ?? this.defaultPool;
  }

  private extractNamespaceNames(raw: any): string[] {
    if (!Array.isArray(raw)) return [];
    return raw
      .map((item: any) => {
        if (typeof item === 'string') return item;
        if (item?.namespace) return item.namespace;
        if (item?.name) return item.name;
        return null;
      })
      .filter(Boolean) as string[];
  }

  private parseIscsiDisks(rawDisks: any): IscsiDiskRef[] {
    const disks = Array.isArray(rawDisks) ? rawDisks : [];
    return disks
      .map((d: any) => ({
        pool: d?.pool ?? this.defaultPool,
        image: d?.image ?? d?.name,
        namespace: d?.pool_namespace ?? d?.namespace,
      }))
      .filter((d: IscsiDiskRef) => Boolean(d.pool) && Boolean(d.image));
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async listRbdEntries(
    pool: string,
    pageSize = 200,
    namespace?: string
  ): Promise<RbdEntry[]> {
    // Paginate defensively; filter namespaces client-side because the API breaks when sent as a query param.
    const all: RbdEntry[] = [];
    let offset = 0;
    let lastPageKey = '';

    while (true) {
      try {
        const pageRaw = await this.api.getRBDs(pool, { offset, limit: pageSize });
        let entries = this.extractRbdEntries(pageRaw, pool);
        if (namespace !== undefined) {
          entries = entries.filter((e) => e.namespace === namespace);
        }
        if (entries.length === 0) break;
        const pageKey = entries.map((e) => `${e.namespace ?? ''}/${e.name}`).join('|');
        if (offset > 0 && pageKey === lastPageKey) break;
        all.push(...entries);
        if (entries.length < pageSize) break;
        lastPageKey = pageKey;
        offset += pageSize;
      } catch (err) {
        if (offset === 0) throw err;
        break;
      }
    }

    if (all.length === 0) {
      try {
        let entries = this.extractRbdEntries(await this.api.getRBDs(pool), pool);
        if (namespace !== undefined) {
          entries = entries.filter((e) => e.namespace === namespace);
        }
        return entries;
      } catch {
        return [];
      }
    }

    return all;
  }
}
