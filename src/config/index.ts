import dotenv from 'dotenv';

dotenv.config();

export type Config = {
  broker: {
    url: string;
    task: {
      queue: string;
      exchange: string;
    };
    telemetry: {
      exchange: string;
      routingKey: string;
    };
    results: {
      exchange: string;
    };
    service: {
      name: string;
      agentId: string;
    };
    prefetchCount: number;
  };
  ceph: {
    url: string;
    user: string;
    password: string;
    defaultPool: string;
    insecureTls: boolean;
    apiAccept: string;
    iscsi: {
      gw: {
        host: string;
        ip: string;
      };
      auth?: {
        user: string;
        password: string;
      };
    };
  };
};

const getEnv = (key: string, fallback?: string): string => {
  const value = process.env[key] ?? fallback;
  if (!value) {
    throw new Error(`Missing required env var ${key}`);
  }
  return value;
};

const getEnvOptional = (key: string): string | undefined => {
  const value = process.env[key];
  if (!value) return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
};

const toNumber = (value: string, key: string): number => {
  const parsed = Number(value);
  if (Number.isNaN(parsed)) {
    throw new Error(`Env var ${key} must be a number`);
  }
  return parsed;
};

const toBoolean = (value: string, key: string): boolean => {
  const normalized = value.trim().toLowerCase();
  if (['true', '1', 'yes', 'y'].includes(normalized)) return true;
  if (['false', '0', 'no', 'n'].includes(normalized)) return false;
  throw new Error(`Env var ${key} must be boolean-like (true/false)`);
};

export const config: Config = {
  broker: {
    url: getEnv('BROKER_URL', 'amqp://guest:guest@localhost:5672/'),
    task: {
      queue: getEnv('TASK_QUEUE', 'openhvx.storage.tasks'),
      exchange: getEnv('JOBS_EXCHANGE', 'jobs'),
    },
    telemetry: {
      exchange: getEnv('TELEMETRY_EXCHANGE', 'openhvx.telemetry'),
      routingKey: getEnv('TELEMETRY_ROUTING_KEY', 'storage'),
    },
    results: {
      exchange: getEnv('RESULTS_EXCHANGE', 'results'),
    },
    service: {
      name: getEnv('SERVICE_NAME', 'storage-controller'),
      agentId: getEnv('STORAGE_CONTROLLER_AGENT_ID', 'storage-controller-1'),
    },
    prefetchCount: toNumber(getEnv('PREFETCH_COUNT', '10'), 'PREFETCH_COUNT'),
  },
  ceph: {
    url: getEnv('CEPH_API_URL', 'http://localhost:8003'),
    user: getEnv('CEPH_API_USER', 'admin'),
    password: getEnv('CEPH_API_PASSWORD', 'password'),
    defaultPool: getEnv('CEPH_DEFAULT_POOL', 'rbd'),
    insecureTls: toBoolean(getEnv('CEPH_INSECURE_TLS', 'false'), 'CEPH_INSECURE_TLS'),
    apiAccept: getEnv('CEPH_API_ACCEPT', 'application/vnd.ceph.api.v1.0+json'),
    iscsi: {
      gw: {
        host: getEnv('CEPH_GW_ISCSI_HOST', 'ceph-gw-01'),
        ip: getEnv('CEPH_GW_ISCSI_IP', '127.0.0.1'),
      },
      auth: (() => {
        const user = getEnvOptional('CEPH_TARGET_USER');
        const password = getEnvOptional('CEPH_TARGET_PASSWORD');
        if ((user && !password) || (!user && password)) {
          throw new Error('CEPH_TARGET_USER and CEPH_TARGET_PASSWORD must be set together');
        }
        return user && password ? { user, password } : undefined;
      })(),
    },
  },
};
