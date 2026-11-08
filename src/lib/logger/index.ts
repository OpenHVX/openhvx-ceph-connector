/**
 * Tiny structured logger with namespaces.
 *
 * Env:
 *  - LOG_ENABLED=0            -> disable logs (default: enabled)
 *  - LOG_LEVEL=debug|info|... -> min level (default: info)
 *  - LOG_JSON=1               -> JSON lines (default: pretty text)
 *  - LOG_SERVICE_NAME=ohvx    -> service tag (optional)
 */

type LevelMap = Record<LevelName, number>;

type LevelName = "trace" | "debug" | "info" | "warn" | "error";

export interface LogMeta {
    [key: string]: unknown;
    error?: unknown;
    err?: unknown;
}

export interface Logger {
    trace(message: unknown, meta?: LogMeta): void;
    debug(message: unknown, meta?: LogMeta): void;
    info(message: unknown, meta?: LogMeta): void;
    warn(message: unknown, meta?: LogMeta): void;
    error(message: unknown, meta?: LogMeta): void;
    child(namespace: string | string[]): Logger;
}

const LEVELS: LevelMap = {
    trace: 10,
    debug: 20,
    info: 30,
    warn: 40,
    error: 50,
};

const ENABLED = process.env.LOG_ENABLED !== "0";
const MIN_LEVEL =
    LEVELS[(process.env.LOG_LEVEL || "info").toLowerCase() as LevelName] ?? LEVELS.info;
const AS_JSON = process.env.LOG_JSON === "1";
const SERVICE = process.env.LOG_SERVICE_NAME || "";

function levelName(value: number): LevelName {
    const entry = (Object.entries(LEVELS) as Array<[LevelName, number]>).find(
        ([, v]) => v === value
    );
    return entry ? entry[0] : "info";
}

function redact(value: unknown, key = '', depth = 0): unknown {
    if (/password|secret|token|authorization|cookie|commandBody|config|headers/i.test(key)) return '[redacted]';
    if (depth > 8) return '[truncated]';
    if (value instanceof Error) return { name: value.name, message: redact(value.message, '', depth + 1) };
    if (typeof value === 'string') return value.replace(/(amqps?|https?):\/\/[^\s/@]+:[^\s/@]+@/g, '$1://[redacted]@');
    if (Array.isArray(value)) return value.map(item => redact(item, '', depth + 1));
    if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, redact(v, k, depth + 1)]));
    return value;
}

function safeStringify(obj: unknown): string {
    try {
        return JSON.stringify(obj);
    } catch {
        return '{"_":"[unserializable]"}';
    }
}

function joinNamespace(ns?: string | string[]): string {
    if (!ns) return "";
    if (Array.isArray(ns)) return ns.join(":");
    return String(ns);
}

function baseLog({ ns }: { ns?: string | string[] }): Logger {
    const namespace = joinNamespace(ns);

    const write = (levelValue: number, msg: unknown, meta?: LogMeta) => {
        if (!ENABLED || levelValue < MIN_LEVEL) return;

        const now = new Date();
        const lvl = levelName(levelValue);
        const payload = {
            ts: now.toISOString(),
            level: lvl,
            ns: namespace || undefined,
            service: SERVICE || undefined,
            pid: process.pid,
            msg: String(msg ?? ""),
            ...(meta ? { meta: redact(meta) as LogMeta } : {}),
        };

        if (AS_JSON) {
            const line = safeStringify(payload);
            if (levelValue >= LEVELS.error) {
                console.error(line);
            } else if (levelValue >= LEVELS.warn) {
                console.warn(line);
            } else {
                console.log(line);
            }
            return;
        }

        const tags = [
            `[${payload.ts}]`,
            SERVICE && `[${SERVICE}]`,
            `[${lvl.toUpperCase()}]`,
            namespace && `[${namespace}]`,
        ]
            .filter(Boolean)
            .join(" ");

        const tail = payload.meta ? ` ${safeStringify(payload.meta)}` : "";
        const line = `${tags} ${payload.msg}${tail}`;

        if (levelValue >= LEVELS.error) {
            console.error(line);
        } else if (levelValue >= LEVELS.warn) {
            console.warn(line);
        } else {
            console.log(line);
        }
    };

    const child = (subNs: string | string[]): Logger => {
        const next = Array.isArray(subNs) ? subNs : [String(subNs)];
        const merged = namespace ? [namespace, ...next] : next;
        return baseLog({ ns: merged });
    };

    return {
        trace: (m, meta) => write(LEVELS.trace, m, meta),
        debug: (m, meta) => write(LEVELS.debug, m, meta),
        info: (m, meta) => write(LEVELS.info, m, meta),
        warn: (m, meta) => write(LEVELS.warn, m, meta),
        error: (m, meta) => write(LEVELS.error, m, meta),
        child,
    };
}

const logger = baseLog({ ns: "" });

export default logger;
