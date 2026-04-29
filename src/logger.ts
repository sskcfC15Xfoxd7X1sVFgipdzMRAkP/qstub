import type { LogLevel } from "./config.ts";

const LEVELS: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

export interface Logger {
  debug: (msg: string, fields?: Record<string, unknown>) => void;
  info: (msg: string, fields?: Record<string, unknown>) => void;
  warn: (msg: string, fields?: Record<string, unknown>) => void;
  error: (msg: string, fields?: Record<string, unknown>) => void;
}

export function createLogger(level: LogLevel = "info"): Logger {
  const threshold = LEVELS[level];
  const emit = (atLevel: LogLevel, msg: string, fields?: Record<string, unknown>) => {
    if (LEVELS[atLevel] < threshold) return;
    const ts = new Date().toISOString();
    const tag = atLevel.toUpperCase().padEnd(5);
    const tail = fields && Object.keys(fields).length > 0 ? ` ${formatFields(fields)}` : "";
    const line = `${ts} ${tag} ${msg}${tail}`;
    if (atLevel === "error") {
      console.error(line);
    } else if (atLevel === "warn") {
      console.warn(line);
    } else {
      console.log(line);
    }
  };
  return {
    debug: (m, f) => emit("debug", m, f),
    info: (m, f) => emit("info", m, f),
    warn: (m, f) => emit("warn", m, f),
    error: (m, f) => emit("error", m, f),
  };
}

function formatFields(fields: Record<string, unknown>): string {
  return Object.entries(fields)
    .map(([k, v]) => `${k}=${formatValue(v)}`)
    .join(" ");
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return String(value);
  if (typeof value === "string") {
    return /[\s"=]/.test(value) ? JSON.stringify(value) : value;
  }
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
