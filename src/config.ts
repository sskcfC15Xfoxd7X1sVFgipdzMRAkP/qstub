export type LogLevel = "debug" | "info" | "warn" | "error";

export interface Config {
  port: number;
  dbPath: string;
  tickMs: number;
  currentSigningKey: string;
  nextSigningKey: string;
  logLevel: LogLevel;
  redisToken: string;
}

const DEFAULT_CURRENT_KEY = "sig_downstash_current_dev_key_do_not_use_in_prod";
const DEFAULT_NEXT_KEY = "sig_downstash_next_dev_key_do_not_use_in_prod";

export interface ConfigOverrides {
  port?: number;
  dbPath?: string;
  tickMs?: number;
  currentSigningKey?: string;
  nextSigningKey?: string;
  logLevel?: LogLevel;
  redisToken?: string;
}

export function resolveConfig(
  overrides: ConfigOverrides = {},
  env: Record<string, string | undefined> = process.env,
): Config {
  return {
    port: overrides.port ?? parseIntOr(env.DOWNSTASH_PORT, 8080),
    dbPath: overrides.dbPath ?? env.DOWNSTASH_DB ?? ".downstash/db.sqlite",
    tickMs: overrides.tickMs ?? parseIntOr(env.DOWNSTASH_TICK_MS, 250),
    currentSigningKey:
      overrides.currentSigningKey ?? env.DOWNSTASH_CURRENT_SIGNING_KEY ?? DEFAULT_CURRENT_KEY,
    nextSigningKey: overrides.nextSigningKey ?? env.DOWNSTASH_NEXT_SIGNING_KEY ?? DEFAULT_NEXT_KEY,
    logLevel: overrides.logLevel ?? (env.DOWNSTASH_LOG_LEVEL as LogLevel | undefined) ?? "info",
    redisToken: overrides.redisToken ?? env.DOWNSTASH_REDIS_TOKEN ?? "dev",
  };
}

function parseIntOr(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export const DEFAULT_KEYS = {
  current: DEFAULT_CURRENT_KEY,
  next: DEFAULT_NEXT_KEY,
} as const;
