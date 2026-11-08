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
      agentId: string;
    };
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

export const positiveInteger = (key: string, fallback: number): number => {
  const value = Number(process.env[key] ?? fallback);
  if (!Number.isSafeInteger(value) || value <= 0 || value > 2147483647) throw new Error(`${key} must be a positive integer within the timer range`);
  return value;
};

if (process.env.RMQ_URL && process.env.BROKER_URL && process.env.RMQ_URL !== process.env.BROKER_URL) {
  throw new Error('RMQ_URL and legacy BROKER_URL disagree');
}

const toBoolean = (value: string, key: string): boolean => {
  const normalized = value.trim().toLowerCase();
  if (['true', '1', 'yes', 'y'].includes(normalized)) return true;
  if (['false', '0', 'no', 'n'].includes(normalized)) return false;
  throw new Error(`Env var ${key} must be boolean-like (true/false)`);
};

export const config: Config = {
  broker: {
    url: getEnv('RMQ_URL', process.env.BROKER_URL),
    task: {
      queue: getEnv('TASK_QUEUE', `agent.${getEnv('STORAGE_CONTROLLER_AGENT_ID')}.tasks`),
      exchange: getEnv('JOBS_EXCHANGE', 'jobs'),
    },
    telemetry: {
      exchange: getEnv('TELEMETRY_EXCHANGE', 'agent.telemetry'),
      routingKey: getEnv('TELEMETRY_ROUTING_KEY', 'inventory.storage.full'),
    },
    results: {
      exchange: getEnv('RESULTS_EXCHANGE', 'results'),
    },
    service: {
      agentId: getEnv('STORAGE_CONTROLLER_AGENT_ID'),
    },
  },
  ceph: {
    url: getEnv('CEPH_API_URL'),
    user: getEnv('CEPH_API_USER'),
    password: getEnv('CEPH_API_PASSWORD'),
    defaultPool: getEnv('CEPH_DEFAULT_POOL', 'rbd'),
    insecureTls: toBoolean(getEnv('CEPH_INSECURE_TLS', 'false'), 'CEPH_INSECURE_TLS'),
    apiAccept: getEnv('CEPH_API_ACCEPT', 'application/vnd.ceph.api.v1.0+json'),
    iscsi: {
      gw: {
        host: getEnv('CEPH_GW_ISCSI_HOST'),
        ip: getEnv('CEPH_GW_ISCSI_IP'),
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
