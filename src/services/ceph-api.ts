import axios, { AxiosInstance, AxiosResponse, Method } from 'axios';
import https from 'https';
import { SetTargetParams } from '../types/iscsi/target';

const CEPH_ACCEPT_V1 = 'application/vnd.ceph.api.v1.0+json';
const CEPH_ACCEPT_V2 = 'application/vnd.ceph.api.v2.0+json';

export type CreateImageParams = {
  pool: string;
  name: string;
  sizeBytes: number;
  namespace?: string;
};

export type CloneImageParams = {
  srcPool: string;
  srcImage: string;
  snapshot: string;
  destPool: string;
  destImage: string;
  destNamespace?: string;
  objSize?: number | null;
  features?: number | null;
  stripeUnit?: number | null;
  stripeCount?: number | null;
  dataPool?: string | null;
  configuration?: string | Record<string, unknown> | Array<Record<string, unknown>> | null;
  metadata?: Record<string, unknown> | null;
};

export type CephApiOptions = {
  baseUrl: string;
  username: string;
  password: string;
  allowInsecureTls?: boolean;
  acceptHeader?: string;
};

/**
 * Thin, opinionated wrapper around the Ceph dashboard REST API.
 * Adds a single request helper and namespaces for future expansion.
 */
export class CephApi {
  private readonly client: AxiosInstance;
  private readonly username: string;
  private readonly password: string;
  private readonly defaultAccept: string;
  private readonly acceptCache = new Map<string, string>();
  private loginPromise?: Promise<void>;

  constructor(options: CephApiOptions) {
    this.username = options.username;
    this.password = options.password;
    this.defaultAccept = options.acceptHeader ?? CEPH_ACCEPT_V1;

    const httpsAgent = options.allowInsecureTls
      ? new https.Agent({ rejectUnauthorized: false })
      : undefined;

    this.client = axios.create({
      baseURL: options.baseUrl.replace(/\/$/, ''),
      auth: {
        username: options.username,
        password: options.password,
      },
      timeout: 15_000,
      httpsAgent,
      headers: {
        Accept: this.defaultAccept,
      },
    });
  }

  private normalizePath(url: string): string {
    return url.split('?')[0];
  }

  private parseExpectedAcceptFrom415(data: any): string | null {
    const detail = typeof data?.detail === 'string' ? data.detail : '';
    if (!detail) return null;

    if (detail.includes("endpoint is '1.0'")) return CEPH_ACCEPT_V1;
    if (detail.includes("endpoint is '2.0'")) return CEPH_ACCEPT_V2;
    return null;
  }

  private buildAcceptAttempts(url: string): string[] {
    const cacheKey = this.normalizePath(url);
    const cached = this.acceptCache.get(cacheKey);
    const fallback = this.defaultAccept === CEPH_ACCEPT_V1 ? CEPH_ACCEPT_V2 : CEPH_ACCEPT_V1;
    const attempts = [cached, this.defaultAccept, fallback].filter(Boolean) as string[];
    return [...new Set(attempts)];
  }

  private async login(): Promise<void> {
    const res = await this.client.post(
      '/api/auth',
      {
        username: this.username,
        password: this.password,
      },
      {
        headers: {
          Accept: CEPH_ACCEPT_V1,
        },
      }
    );

    const token = (res.data && (res.data as any).token) as string | undefined;
    if (token) {
      this.client.defaults.headers.common.Authorization = `Bearer ${token}`;
    }

    const setCookie = res.headers['set-cookie'];
    if (setCookie && setCookie.length > 0) {
      this.client.defaults.headers.common.Cookie = setCookie.join('; ');
    }

    this.client.defaults.headers.common.Accept = this.defaultAccept;
  }

  private async ensureLogin(): Promise<void> {
    if (!this.loginPromise) {
      this.loginPromise = this.login();
    }
    return this.loginPromise;
  }

  async rawRequest<T>(
    method: Method,
    url: string,
    data?: unknown,
    config?: Partial<import('axios').AxiosRequestConfig>
  ): Promise<AxiosResponse<T>> {
    await this.ensureLogin();

    const cacheKey = this.normalizePath(url);
    const attempts = this.buildAcceptAttempts(url);
    let lastErr: any;

    for (const accept of attempts) {
      try {
        const res = await this.client.request<T>({
          method,
          url,
          data,
          headers: {
            Accept: accept,
          },
          ...config,
        });

        this.acceptCache.set(cacheKey, accept);
        return res;
      } catch (err: any) {
        const resp = err?.response;
        lastErr = err;

        if (!resp || resp.status !== 415) {
          throw err;
        }

        const expected = this.parseExpectedAcceptFrom415(resp.data);
        if (expected && expected !== accept) {
          try {
            const res2 = await this.client.request<T>({
              method,
              url,
              data,
              headers: {
                Accept: expected,
              },
              ...config,
            });

            this.acceptCache.set(cacheKey, expected);
            return res2;
          } catch (err2: any) {
            lastErr = err2;
          }
        }
      }
    }

    throw lastErr;
  }

  private async request<T>(
    method: Method,
    url: string,
    data?: unknown
  ): Promise<T> {
    const response = await this.rawRequest<T>(method, url, data);
    return response.data;
  }

  // ---- Health ----

  async getHealthSummary(): Promise<unknown> {
    return this.request('GET', '/api/health/minimal');
  }


// ---- iSCSI ----
  async setTarget(params: SetTargetParams): Promise<unknown> {
    const payload = {
      target_iqn: params.iqn,
      target_controls: params.targetControls ?? { immediate_data: true },
      acl_enabled: params.aclEnabled ?? false,
      portals: params.portals,
      disks: params.disks.map((disk, idx) => ({
        pool: disk.pool,
        image: disk.image,
        backstore: disk.backstore ?? 'user:rbd',
        controls: disk.controls ?? {},
        lun: disk.lun ?? idx,
        wwn: disk.wwn ?? null,
      })),
      clients: params.clients ?? [],
      groups: params.groups ?? [],
      auth: {
        user: params.auth?.user ?? '',
        password: params.auth?.password ?? '',
        mutual_user: params.auth?.mutualUser ?? '',
        mutual_password: params.auth?.mutualPassword ?? '',
      },
    };

    return this.request('POST', '/api/iscsi/target', payload);
  }

  async getTargets(): Promise<unknown> {
    return this.request('GET', '/api/iscsi/target');
  }

  async deleteTarget(iqn: string): Promise<unknown> {
    const path = `/api/iscsi/target/${encodeURIComponent(iqn)}`;
    return this.request('DELETE', path);
  }

  // ---- RBD / Block ----

  async getRBDs(pool: string, params?: { offset?: number; limit?: number; namespace?: string }): Promise<unknown> {
    const search = new URLSearchParams();
    search.set('pool_name', pool);
    if (typeof params?.offset === 'number') search.set('offset', String(params.offset));
    if (typeof params?.limit === 'number') search.set('limit', String(params.limit));
    return this.request('GET', `/api/block/image?${search.toString()}`);
  }

  async getImage(pool: string, name: string, params?: { namespace?: string; omitUsage?: boolean }): Promise<unknown> {
    const namespace = params?.namespace;
    const omitUsage = params?.omitUsage ?? false;
    const parts = [pool];
    if (namespace) parts.push(namespace);
    parts.push(name);
    const imageSpec = encodeURIComponent(parts.join('/'));
    const search = new URLSearchParams();
    if (omitUsage) search.set('omit_usage', 'true');
    const query = search.toString();
    return this.request('GET', `/api/block/image/${imageSpec}${query ? `?${query}` : ''}`);
  }

  private buildImageSpec(pool: string, name: string, namespace?: string): string {
    const parts = [pool];
    if (namespace) parts.push(namespace);
    parts.push(name);
    return encodeURIComponent(parts.join('/'));
  }

  async createBlockImage(params: CreateImageParams): Promise<unknown> {
    const payload = {
      pool_name: params.pool,
      image_name: params.name,
      size: params.sizeBytes,
      namespace: params.namespace ?? '',
    };
    return this.request('POST', '/api/block/image', payload);
  }

  async cloneBlockImageFromSnapshot(params: CloneImageParams): Promise<unknown> {
    const imageSpec = encodeURIComponent(`${params.srcPool}/${params.srcImage}`);
    const path = `/api/block/image/${imageSpec}/snap/${encodeURIComponent(params.snapshot)}/clone`;
    const payload: Record<string, unknown> = {
      child_pool_name: params.destPool,
      child_image_name: params.destImage,
      child_namespace: params.destNamespace
      
    };

    // Only send optional fields when provided to avoid Ceph API complaining about nulls.
    if (params.destNamespace !== undefined) payload.child_namespace = params.destNamespace;
    if (params.objSize != null) payload.obj_size = params.objSize;
    if (params.features != null) payload.features = params.features;
    if (params.stripeUnit != null) payload.stripe_unit = params.stripeUnit;
    if (params.stripeCount != null) payload.stripe_count = params.stripeCount;
    if (params.dataPool != null) payload.data_pool = params.dataPool;
    if (params.configuration != null) payload.configuration = params.configuration;
    if (params.metadata != null) payload.metadata = params.metadata;

    return this.rawRequest('POST', path, payload).then((res) => res.data);
  }

  async protectSnapshot(pool: string, image: string, snapshot: string, opts?: { namespace?: string }): Promise<unknown> {
    const namespace = opts?.namespace;
    const parts = [pool];
    if (namespace) parts.push(namespace);
    parts.push(image);
    const imageSpec = encodeURIComponent(parts.join('/'));
    const path = `/api/block/image/${imageSpec}/snap/${encodeURIComponent(snapshot)}`;
    const payload = { is_protected: true };
    return this.request('PUT', path, payload);
  }

  async deleteBlockImage(pool: string, name: string, namespace?: string): Promise<unknown> {
    const imageSpec = this.buildImageSpec(pool, name, namespace);
    const path = `/api/block/image/${imageSpec}`;
    return this.request('DELETE', path);
  }

  async deleteBlockImageFromTrash(pool: string, name: string, namespace?: string): Promise<unknown> {
    const imageSpec = this.buildImageSpec(pool, name, namespace);
    const path = `/api/block/image/trash/${imageSpec}`;
    return this.request('DELETE', path);
  }

  async moveBlockImageToTrash(pool: string, name: string, delay = 0, namespace?: string): Promise<unknown> {
    const imageSpec = this.buildImageSpec(pool, name, namespace);
    const path = `/api/block/image/${imageSpec}/move_trash`;
    return this.request('POST', path, { delay });
  }

  async resizeBlockImage(pool: string, name: string, sizeBytes: number, namespace?: string): Promise<unknown> {
    const imageSpec = this.buildImageSpec(pool, name, namespace);
    const path = `/api/block/image/${imageSpec}`;
    const payload = { size: sizeBytes };
    return this.request('PUT', path, payload);
  }

  // ---- Namespaces ----

  async listNamespaces(pool: string): Promise<unknown> {
    const path = `/api/block/pool/${encodeURIComponent(pool)}/namespace`;
    return this.request('GET', path);
  }

  async createNamespace(pool: string, namespace: string): Promise<unknown> {
    const path = `/api/block/pool/${encodeURIComponent(pool)}/namespace`;
    const payload = { namespace };
    return this.request('POST', path, payload);
  }
}
