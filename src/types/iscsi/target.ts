export type TargetPortal = {
  host: string;
  ip: string;
};

export type TargetDiskParams = {
  pool: string;
  image: string;
  namespace?: string;
  poolNamespace?: string;
  lun?: number;
  backstore?: string;
  controls?: Record<string, unknown>;
  wwn?: string | null;
};

export type TargetAuthParams = {
  user?: string;
  password?: string;
  mutualUser?: string;
  mutualPassword?: string;
};

export type SetTargetParams = {
  iqn: string;
  portals: TargetPortal[];
  disks: TargetDiskParams[];
  targetControls?: Record<string, unknown>;
  aclEnabled?: boolean;
  clients?: unknown[];
  groups?: unknown[];
  auth?: TargetAuthParams;
};
