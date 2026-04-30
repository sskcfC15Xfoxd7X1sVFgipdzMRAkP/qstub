import { globToRegex } from "./glob.ts";

export type RedisType = "string" | "list" | "set" | "zset" | "hash";

interface RedisEntry {
  type: RedisType;
  value: string | string[] | Set<string> | Map<string, string> | Map<string, number>;
  expiresAt: number | null;
}

export interface SetOptions {
  ex?: number;
  px?: number;
  nx?: boolean;
  xx?: boolean;
  get?: boolean;
  exat?: number;
  pxat?: number;
  keepttl?: boolean;
}

export interface ScanOptions {
  match?: string;
  count?: number;
}

export interface ZAddOptions {
  nx?: boolean;
  xx?: boolean;
  gt?: boolean;
  lt?: boolean;
  ch?: boolean;
}

export interface ZRangeByScoreOptions {
  withScores?: boolean;
  limit?: { offset: number; count: number };
}

export interface RedisStore {
  set(key: string, value: string, opts?: SetOptions): string | null;
  get(key: string): string | null;
  mset(pairs: [string, string][]): void;
  mget(keys: string[]): (string | null)[];
  incr(key: string): number;
  incrby(key: string, increment: number): number;
  incrbyfloat(key: string, increment: number): string;
  decr(key: string): number;
  decrby(key: string, decrement: number): number;
  append(key: string, value: string): number;
  strlen(key: string): number;
  getrange(key: string, start: number, end: number): string;
  setrange(key: string, offset: number, value: string): number;
  getdel(key: string): string | null;
  getex(key: string, opts?: SetOptions): string | null;
  setnx(key: string, value: string): boolean;
  setex(key: string, seconds: number, value: string): void;
  psetex(key: string, ms: number, value: string): void;

  del(keys: string[]): number;
  exists(keys: string[]): number;
  expire(key: string, seconds: number): boolean;
  expireat(key: string, timestamp: number): boolean;
  pexpire(key: string, ms: number): boolean;
  pexpireat(key: string, ms: number): boolean;
  ttl(key: string): number;
  pttl(key: string): number;
  persist(key: string): boolean;
  rename(from: string, to: string): void;
  type(key: string): string;
  keys(pattern: string): string[];
  scan(cursor: number, opts?: ScanOptions): [number, string[]];
  unlink(keys: string[]): number;
  dbsize(): number;
  flushdb(): void;
  flushall(): void;
  randomkey(): string | null;
  copy(source: string, dest: string, replace?: boolean): boolean;

  hset(key: string, fields: [string, string][]): number;
  hget(key: string, field: string): string | null;
  hmset(key: string, fields: [string, string][]): void;
  hmget(key: string, fields: string[]): (string | null)[];
  hgetall(key: string): string[];
  hdel(key: string, fields: string[]): number;
  hexists(key: string, field: string): boolean;
  hlen(key: string): number;
  hkeys(key: string): string[];
  hvals(key: string): string[];
  hincrby(key: string, field: string, increment: number): number;
  hincrbyfloat(key: string, field: string, increment: number): string;
  hsetnx(key: string, field: string, value: string): boolean;
  hscan(key: string, cursor: number, opts?: ScanOptions): [number, string[]];

  lpush(key: string, values: string[]): number;
  rpush(key: string, values: string[]): number;
  lpop(key: string, count?: number): string | string[] | null;
  rpop(key: string, count?: number): string | string[] | null;
  lrange(key: string, start: number, stop: number): string[];
  llen(key: string): number;
  lindex(key: string, index: number): string | null;
  lset(key: string, index: number, value: string): void;
  linsert(key: string, before: boolean, pivot: string, value: string): number;
  lrem(key: string, count: number, value: string): number;
  ltrim(key: string, start: number, stop: number): void;

  sadd(key: string, members: string[]): number;
  srem(key: string, members: string[]): number;
  smembers(key: string): string[];
  sismember(key: string, member: string): boolean;
  scard(key: string): number;
  spop(key: string, count?: number): string | string[] | null;
  srandmember(key: string, count?: number): string | string[] | null;
  sunion(keys: string[]): string[];
  sinter(keys: string[]): string[];
  sdiff(keys: string[]): string[];
  sunionstore(dest: string, keys: string[]): number;
  sinterstore(dest: string, keys: string[]): number;
  sdiffstore(dest: string, keys: string[]): number;
  sscan(key: string, cursor: number, opts?: ScanOptions): [number, string[]];

  zadd(key: string, entries: [number, string][], opts?: ZAddOptions): number | string;
  zrem(key: string, members: string[]): number;
  zscore(key: string, member: string): number | null;
  zrank(key: string, member: string): number | null;
  zrevrank(key: string, member: string): number | null;
  zrange(key: string, start: number, stop: number, withScores?: boolean): string[];
  zrangebyscore(key: string, min: string, max: string, opts?: ZRangeByScoreOptions): string[];
  zrevrange(key: string, start: number, stop: number, withScores?: boolean): string[];
  zrevrangebyscore(key: string, max: string, min: string, opts?: ZRangeByScoreOptions): string[];
  zcard(key: string): number;
  zcount(key: string, min: string, max: string): number;
  zincrby(key: string, increment: number, member: string): string;
  zpopmin(key: string, count?: number): string[];
  zpopmax(key: string, count?: number): string[];
  zunionstore(dest: string, keys: string[], weights?: number[]): number;
  zinterstore(dest: string, keys: string[], weights?: number[]): number;
  zscan(key: string, cursor: number, opts?: ScanOptions): [number, string[]];

  ping(message?: string): string;
  echo(message: string): string;
  time(): [string, string];
}

const WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";

export function createRedisStore(): RedisStore {
  const data = new Map<string, RedisEntry>();

  function now(): number {
    return Date.now();
  }

  function isExpired(entry: RedisEntry): boolean {
    return entry.expiresAt !== null && entry.expiresAt <= now();
  }

  function getEntry(key: string): RedisEntry | null {
    const entry = data.get(key);
    if (!entry) return null;
    if (isExpired(entry)) {
      data.delete(key);
      return null;
    }
    return entry;
  }

  function assertType(key: string, expected: RedisType): RedisEntry | null {
    const entry = getEntry(key);
    if (!entry) return null;
    if (entry.type !== expected) throw new Error(WRONGTYPE);
    return entry;
  }

  function getStringVal(key: string): string | null {
    const entry = assertType(key, "string");
    return entry ? (entry.value as string) : null;
  }

  function getOrCreateList(key: string): string[] {
    const entry = getEntry(key);
    if (!entry) {
      const list: string[] = [];
      data.set(key, { type: "list", value: list, expiresAt: null });
      return list;
    }
    if (entry.type !== "list") throw new Error(WRONGTYPE);
    return entry.value as string[];
  }

  function getOrCreateSet(key: string): Set<string> {
    const entry = getEntry(key);
    if (!entry) {
      const s = new Set<string>();
      data.set(key, { type: "set", value: s, expiresAt: null });
      return s;
    }
    if (entry.type !== "set") throw new Error(WRONGTYPE);
    return entry.value as Set<string>;
  }

  function getOrCreateHash(key: string): Map<string, string> {
    const entry = getEntry(key);
    if (!entry) {
      const m = new Map<string, string>();
      data.set(key, { type: "hash", value: m, expiresAt: null });
      return m;
    }
    if (entry.type !== "hash") throw new Error(WRONGTYPE);
    return entry.value as Map<string, string>;
  }

  function getOrCreateZset(key: string): Map<string, number> {
    const entry = getEntry(key);
    if (!entry) {
      const m = new Map<string, number>();
      data.set(key, { type: "zset", value: m, expiresAt: null });
      return m;
    }
    if (entry.type !== "zset") throw new Error(WRONGTYPE);
    return entry.value as Map<string, number>;
  }

  function sortedMembers(zset: Map<string, number>): [string, number][] {
    return Array.from(zset.entries()).sort((a, b) => {
      if (a[1] !== b[1]) return a[1] - b[1];
      return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
    });
  }

  function parseScoreBound(s: string, isMin: boolean): number {
    if (s === "-inf") return Number.NEGATIVE_INFINITY;
    if (s === "+inf" || s === "inf") return Number.POSITIVE_INFINITY;
    if (s.startsWith("(")) {
      const val = Number.parseFloat(s.slice(1));
      return isMin ? val + Number.EPSILON : val - Number.EPSILON;
    }
    return Number.parseFloat(s);
  }

  function resolveIndex(index: number, len: number): number {
    return index < 0 ? Math.max(0, len + index) : index;
  }

  function scanIterable(items: string[], cursor: number, opts?: ScanOptions): [number, string[]] {
    const count = opts?.count ?? 10;
    const pattern = opts?.match ? globToRegex(opts.match) : null;
    const results: string[] = [];
    let i = cursor;
    let scanned = 0;
    while (i < items.length && scanned < count) {
      const item = items[i]!;
      if (!pattern || pattern.test(item)) {
        results.push(item);
      }
      i++;
      scanned++;
    }
    const nextCursor = i >= items.length ? 0 : i;
    return [nextCursor, results];
  }

  function cleanupEmpty(key: string, entry: RedisEntry | null): void {
    if (!entry) return;
    if (entry.type === "list" && (entry.value as string[]).length === 0) {
      data.delete(key);
    } else if (entry.type === "set" && (entry.value as Set<string>).size === 0) {
      data.delete(key);
    } else if (entry.type === "zset" && (entry.value as Map<string, number>).size === 0) {
      data.delete(key);
    } else if (entry.type === "hash" && (entry.value as Map<string, string>).size === 0) {
      data.delete(key);
    }
  }

  function applyExpiry(key: string, opts?: SetOptions): void {
    const entry = data.get(key);
    if (!entry) return;
    if (opts?.keepttl) return;
    if (opts?.ex !== undefined) {
      entry.expiresAt = now() + opts.ex * 1000;
    } else if (opts?.px !== undefined) {
      entry.expiresAt = now() + opts.px;
    } else if (opts?.exat !== undefined) {
      entry.expiresAt = opts.exat * 1000;
    } else if (opts?.pxat !== undefined) {
      entry.expiresAt = opts.pxat;
    } else if (!opts?.keepttl) {
      entry.expiresAt = null;
    }
  }

  const store: RedisStore = {
    set(key, value, opts) {
      const existing = getEntry(key);
      if (opts?.nx && existing) return null;
      if (opts?.xx && !existing) return null;

      let prev: string | null = null;
      if (opts?.get) {
        if (existing && existing.type !== "string") throw new Error(WRONGTYPE);
        prev = existing ? (existing.value as string) : null;
      }

      data.set(key, { type: "string", value, expiresAt: null });
      applyExpiry(key, opts);

      return opts?.get ? prev : "OK";
    },

    get(key) {
      return getStringVal(key);
    },

    mset(pairs) {
      for (const [k, v] of pairs) {
        data.set(k, { type: "string", value: v, expiresAt: null });
      }
    },

    mget(keys) {
      return keys.map((k) => getStringVal(k));
    },

    incr(key) {
      return store.incrby(key, 1);
    },

    incrby(key, increment) {
      const current = getStringVal(key);
      const val = current === null ? 0 : Number.parseInt(current, 10);
      if (!Number.isFinite(val)) throw new Error("ERR value is not an integer or out of range");
      const result = val + increment;
      if (!Number.isSafeInteger(result))
        throw new Error("ERR value is not an integer or out of range");
      const entry = getEntry(key);
      const expiresAt = entry?.expiresAt ?? null;
      data.set(key, { type: "string", value: String(result), expiresAt });
      return result;
    },

    incrbyfloat(key, increment) {
      const current = getStringVal(key);
      const val = current === null ? 0 : Number.parseFloat(current);
      if (!Number.isFinite(val)) throw new Error("ERR value is not a valid float");
      const result = val + increment;
      const entry = getEntry(key);
      const expiresAt = entry?.expiresAt ?? null;
      const str = Number.isInteger(result) ? `${result}` : `${result}`;
      data.set(key, { type: "string", value: str, expiresAt });
      return str;
    },

    decr(key) {
      return store.incrby(key, -1);
    },

    decrby(key, decrement) {
      return store.incrby(key, -decrement);
    },

    append(key, value) {
      const current = getStringVal(key) ?? "";
      const result = current + value;
      const entry = getEntry(key);
      const expiresAt = entry?.expiresAt ?? null;
      data.set(key, { type: "string", value: result, expiresAt });
      return result.length;
    },

    strlen(key) {
      const val = getStringVal(key);
      return val === null ? 0 : val.length;
    },

    getrange(key, start, end) {
      const val = getStringVal(key) ?? "";
      const s = resolveIndex(start, val.length);
      let e = end < 0 ? val.length + end : end;
      e = Math.min(e, val.length - 1);
      if (s > e) return "";
      return val.slice(s, e + 1);
    },

    setrange(key, offset, value) {
      let current = getStringVal(key) ?? "";
      if (current.length < offset) {
        current = current.padEnd(offset, "\0");
      }
      const result = current.slice(0, offset) + value + current.slice(offset + value.length);
      const entry = getEntry(key);
      const expiresAt = entry?.expiresAt ?? null;
      data.set(key, { type: "string", value: result, expiresAt });
      return result.length;
    },

    getdel(key) {
      const val = getStringVal(key);
      if (val !== null) data.delete(key);
      return val;
    },

    getex(key, opts) {
      const val = getStringVal(key);
      if (val !== null && opts) {
        applyExpiry(key, opts);
      }
      return val;
    },

    setnx(key, value) {
      if (getEntry(key)) return false;
      data.set(key, { type: "string", value, expiresAt: null });
      return true;
    },

    setex(key, seconds, value) {
      data.set(key, { type: "string", value, expiresAt: now() + seconds * 1000 });
    },

    psetex(key, ms, value) {
      data.set(key, { type: "string", value, expiresAt: now() + ms });
    },

    del(keys) {
      let count = 0;
      for (const k of keys) {
        if (data.delete(k)) count++;
      }
      return count;
    },

    exists(keys) {
      let count = 0;
      for (const k of keys) {
        if (getEntry(k)) count++;
      }
      return count;
    },

    expire(key, seconds) {
      const entry = getEntry(key);
      if (!entry) return false;
      entry.expiresAt = now() + seconds * 1000;
      return true;
    },

    expireat(key, timestamp) {
      const entry = getEntry(key);
      if (!entry) return false;
      entry.expiresAt = timestamp * 1000;
      return true;
    },

    pexpire(key, ms) {
      const entry = getEntry(key);
      if (!entry) return false;
      entry.expiresAt = now() + ms;
      return true;
    },

    pexpireat(key, ms) {
      const entry = getEntry(key);
      if (!entry) return false;
      entry.expiresAt = ms;
      return true;
    },

    ttl(key) {
      const entry = getEntry(key);
      if (!entry) return -2;
      if (entry.expiresAt === null) return -1;
      return Math.max(0, Math.ceil((entry.expiresAt - now()) / 1000));
    },

    pttl(key) {
      const entry = getEntry(key);
      if (!entry) return -2;
      if (entry.expiresAt === null) return -1;
      return Math.max(0, entry.expiresAt - now());
    },

    persist(key) {
      const entry = getEntry(key);
      if (!entry || entry.expiresAt === null) return false;
      entry.expiresAt = null;
      return true;
    },

    rename(from, to) {
      const entry = data.get(from);
      if (!entry) throw new Error("ERR no such key");
      if (isExpired(entry)) {
        data.delete(from);
        throw new Error("ERR no such key");
      }
      data.delete(from);
      data.set(to, entry);
    },

    type(key) {
      const entry = getEntry(key);
      return entry ? entry.type : "none";
    },

    keys(pattern) {
      const regex = globToRegex(pattern);
      const result: string[] = [];
      for (const key of data.keys()) {
        if (getEntry(key) && regex.test(key)) {
          result.push(key);
        }
      }
      return result;
    },

    scan(cursor, opts) {
      const allKeys: string[] = [];
      for (const key of data.keys()) {
        if (getEntry(key)) allKeys.push(key);
      }
      return scanIterable(allKeys, cursor, opts);
    },

    unlink(keys) {
      return store.del(keys);
    },

    dbsize() {
      let count = 0;
      for (const key of data.keys()) {
        if (getEntry(key)) count++;
      }
      return count;
    },

    flushdb() {
      data.clear();
    },

    flushall() {
      data.clear();
    },

    randomkey() {
      const keys: string[] = [];
      for (const key of data.keys()) {
        if (getEntry(key)) keys.push(key);
      }
      if (keys.length === 0) return null;
      return keys[Math.floor(Math.random() * keys.length)]!;
    },

    copy(source, dest, replace) {
      const entry = getEntry(source);
      if (!entry) return false;
      if (getEntry(dest) && !replace) return false;
      const cloned = structuredClone(entry);
      cloned.expiresAt = null;
      data.set(dest, cloned);
      return true;
    },

    // Hash operations
    hset(key, fields) {
      const hash = getOrCreateHash(key);
      let added = 0;
      for (const [f, v] of fields) {
        if (!hash.has(f)) added++;
        hash.set(f, v);
      }
      return added;
    },

    hget(key, field) {
      const entry = assertType(key, "hash");
      if (!entry) return null;
      return (entry.value as Map<string, string>).get(field) ?? null;
    },

    hmset(key, fields) {
      const hash = getOrCreateHash(key);
      for (const [f, v] of fields) {
        hash.set(f, v);
      }
    },

    hmget(key, fields) {
      const entry = assertType(key, "hash");
      if (!entry) return fields.map(() => null);
      const hash = entry.value as Map<string, string>;
      return fields.map((f) => hash.get(f) ?? null);
    },

    hgetall(key) {
      const entry = assertType(key, "hash");
      if (!entry) return [];
      const result: string[] = [];
      for (const [f, v] of entry.value as Map<string, string>) {
        result.push(f, v);
      }
      return result;
    },

    hdel(key, fields) {
      const entry = assertType(key, "hash");
      if (!entry) return 0;
      const hash = entry.value as Map<string, string>;
      let count = 0;
      for (const f of fields) {
        if (hash.delete(f)) count++;
      }
      cleanupEmpty(key, entry);
      return count;
    },

    hexists(key, field) {
      const entry = assertType(key, "hash");
      if (!entry) return false;
      return (entry.value as Map<string, string>).has(field);
    },

    hlen(key) {
      const entry = assertType(key, "hash");
      if (!entry) return 0;
      return (entry.value as Map<string, string>).size;
    },

    hkeys(key) {
      const entry = assertType(key, "hash");
      if (!entry) return [];
      return Array.from((entry.value as Map<string, string>).keys());
    },

    hvals(key) {
      const entry = assertType(key, "hash");
      if (!entry) return [];
      return Array.from((entry.value as Map<string, string>).values());
    },

    hincrby(key, field, increment) {
      const hash = getOrCreateHash(key);
      const current = hash.get(field);
      const val = current === undefined ? 0 : Number.parseInt(current, 10);
      if (!Number.isFinite(val)) throw new Error("ERR hash value is not an integer");
      const result = val + increment;
      hash.set(field, String(result));
      return result;
    },

    hincrbyfloat(key, field, increment) {
      const hash = getOrCreateHash(key);
      const current = hash.get(field);
      const val = current === undefined ? 0 : Number.parseFloat(current);
      if (!Number.isFinite(val)) throw new Error("ERR hash value is not a valid float");
      const result = val + increment;
      const str = String(result);
      hash.set(field, str);
      return str;
    },

    hsetnx(key, field, value) {
      const hash = getOrCreateHash(key);
      if (hash.has(field)) return false;
      hash.set(field, value);
      return true;
    },

    hscan(key, cursor, opts) {
      const entry = assertType(key, "hash");
      if (!entry) return [0, []];
      const hash = entry.value as Map<string, string>;
      const pairs: string[] = [];
      for (const [f, v] of hash) {
        pairs.push(f, v);
      }
      const allFields = Array.from(hash.keys());
      const [nextCursor, matchedFields] = scanIterable(allFields, cursor, opts);
      const result: string[] = [];
      for (const f of matchedFields) {
        result.push(f, hash.get(f)!);
      }
      return [nextCursor, result];
    },

    // List operations
    lpush(key, values) {
      const list = getOrCreateList(key);
      for (const v of values) {
        list.unshift(v);
      }
      return list.length;
    },

    rpush(key, values) {
      const list = getOrCreateList(key);
      list.push(...values);
      return list.length;
    },

    lpop(key, count) {
      const entry = assertType(key, "list");
      if (!entry) return count !== undefined ? null : null;
      const list = entry.value as string[];
      if (list.length === 0) return null;
      if (count !== undefined) {
        const result = list.splice(0, count);
        cleanupEmpty(key, entry);
        return result;
      }
      const val = list.shift()!;
      cleanupEmpty(key, entry);
      return val;
    },

    rpop(key, count) {
      const entry = assertType(key, "list");
      if (!entry) return null;
      const list = entry.value as string[];
      if (list.length === 0) return null;
      if (count !== undefined) {
        const result = list.splice(-count, count).reverse();
        cleanupEmpty(key, entry);
        return result;
      }
      const val = list.pop()!;
      cleanupEmpty(key, entry);
      return val;
    },

    lrange(key, start, stop) {
      const entry = assertType(key, "list");
      if (!entry) return [];
      const list = entry.value as string[];
      const s = resolveIndex(start, list.length);
      let e = stop < 0 ? list.length + stop : stop;
      e = Math.min(e, list.length - 1);
      if (s > e) return [];
      return list.slice(s, e + 1);
    },

    llen(key) {
      const entry = assertType(key, "list");
      if (!entry) return 0;
      return (entry.value as string[]).length;
    },

    lindex(key, index) {
      const entry = assertType(key, "list");
      if (!entry) return null;
      const list = entry.value as string[];
      const i = index < 0 ? list.length + index : index;
      return i >= 0 && i < list.length ? list[i]! : null;
    },

    lset(key, index, value) {
      const entry = assertType(key, "list");
      if (!entry) throw new Error("ERR no such key");
      const list = entry.value as string[];
      const i = index < 0 ? list.length + index : index;
      if (i < 0 || i >= list.length) throw new Error("ERR index out of range");
      list[i] = value;
    },

    linsert(key, before, pivot, value) {
      const entry = assertType(key, "list");
      if (!entry) return 0;
      const list = entry.value as string[];
      const idx = list.indexOf(pivot);
      if (idx === -1) return -1;
      list.splice(before ? idx : idx + 1, 0, value);
      return list.length;
    },

    lrem(key, count, value) {
      const entry = assertType(key, "list");
      if (!entry) return 0;
      const list = entry.value as string[];
      let removed = 0;
      if (count > 0) {
        for (let i = 0; i < list.length && removed < count; ) {
          if (list[i] === value) {
            list.splice(i, 1);
            removed++;
          } else {
            i++;
          }
        }
      } else if (count < 0) {
        const absCount = Math.abs(count);
        for (let i = list.length - 1; i >= 0 && removed < absCount; i--) {
          if (list[i] === value) {
            list.splice(i, 1);
            removed++;
          }
        }
      } else {
        for (let i = list.length - 1; i >= 0; i--) {
          if (list[i] === value) {
            list.splice(i, 1);
            removed++;
          }
        }
      }
      cleanupEmpty(key, entry);
      return removed;
    },

    ltrim(key, start, stop) {
      const entry = assertType(key, "list");
      if (!entry) return;
      const list = entry.value as string[];
      const s = resolveIndex(start, list.length);
      let e = stop < 0 ? list.length + stop : stop;
      e = Math.min(e, list.length - 1);
      if (s > e) {
        list.length = 0;
      } else {
        const trimmed = list.slice(s, e + 1);
        list.length = 0;
        list.push(...trimmed);
      }
      cleanupEmpty(key, entry);
    },

    // Set operations
    sadd(key, members) {
      const s = getOrCreateSet(key);
      let added = 0;
      for (const m of members) {
        if (!s.has(m)) {
          s.add(m);
          added++;
        }
      }
      return added;
    },

    srem(key, members) {
      const entry = assertType(key, "set");
      if (!entry) return 0;
      const s = entry.value as Set<string>;
      let count = 0;
      for (const m of members) {
        if (s.delete(m)) count++;
      }
      cleanupEmpty(key, entry);
      return count;
    },

    smembers(key) {
      const entry = assertType(key, "set");
      if (!entry) return [];
      return Array.from(entry.value as Set<string>);
    },

    sismember(key, member) {
      const entry = assertType(key, "set");
      if (!entry) return false;
      return (entry.value as Set<string>).has(member);
    },

    scard(key) {
      const entry = assertType(key, "set");
      if (!entry) return 0;
      return (entry.value as Set<string>).size;
    },

    spop(key, count) {
      const entry = assertType(key, "set");
      if (!entry) return null;
      const s = entry.value as Set<string>;
      if (s.size === 0) return null;
      const arr = Array.from(s);
      if (count !== undefined) {
        const result: string[] = [];
        for (let i = 0; i < count && arr.length > 0; i++) {
          const idx = Math.floor(Math.random() * arr.length);
          result.push(arr[idx]!);
          s.delete(arr[idx]!);
          arr.splice(idx, 1);
        }
        cleanupEmpty(key, entry);
        return result;
      }
      const idx = Math.floor(Math.random() * arr.length);
      const val = arr[idx]!;
      s.delete(val);
      cleanupEmpty(key, entry);
      return val;
    },

    srandmember(key, count) {
      const entry = assertType(key, "set");
      if (!entry) return null;
      const s = entry.value as Set<string>;
      if (s.size === 0) return null;
      const arr = Array.from(s);
      if (count !== undefined) {
        if (count < 0) {
          const result: string[] = [];
          const absCount = Math.abs(count);
          for (let i = 0; i < absCount; i++) {
            result.push(arr[Math.floor(Math.random() * arr.length)]!);
          }
          return result;
        }
        const shuffled = [...arr].sort(() => Math.random() - 0.5);
        return shuffled.slice(0, count);
      }
      return arr[Math.floor(Math.random() * arr.length)]!;
    },

    sunion(keys) {
      const result = new Set<string>();
      for (const k of keys) {
        const entry = assertType(k, "set");
        if (entry) {
          for (const m of entry.value as Set<string>) {
            result.add(m);
          }
        }
      }
      return Array.from(result);
    },

    sinter(keys) {
      if (keys.length === 0) return [];
      const sets = keys.map((k) => {
        const entry = assertType(k, "set");
        return entry ? (entry.value as Set<string>) : new Set<string>();
      });
      const smallest = sets.reduce((a, b) => (a.size <= b.size ? a : b));
      const result: string[] = [];
      for (const m of smallest) {
        if (sets.every((s) => s.has(m))) {
          result.push(m);
        }
      }
      return result;
    },

    sdiff(keys) {
      if (keys.length === 0) return [];
      const first = assertType(keys[0]!, "set");
      if (!first) return [];
      const result = new Set(first.value as Set<string>);
      for (let i = 1; i < keys.length; i++) {
        const entry = assertType(keys[i]!, "set");
        if (entry) {
          for (const m of entry.value as Set<string>) {
            result.delete(m);
          }
        }
      }
      return Array.from(result);
    },

    sunionstore(dest, keys) {
      const members = store.sunion(keys);
      data.delete(dest);
      if (members.length > 0) {
        data.set(dest, { type: "set", value: new Set(members), expiresAt: null });
      }
      return members.length;
    },

    sinterstore(dest, keys) {
      const members = store.sinter(keys);
      data.delete(dest);
      if (members.length > 0) {
        data.set(dest, { type: "set", value: new Set(members), expiresAt: null });
      }
      return members.length;
    },

    sdiffstore(dest, keys) {
      const members = store.sdiff(keys);
      data.delete(dest);
      if (members.length > 0) {
        data.set(dest, { type: "set", value: new Set(members), expiresAt: null });
      }
      return members.length;
    },

    sscan(key, cursor, opts) {
      const entry = assertType(key, "set");
      if (!entry) return [0, []];
      const arr = Array.from(entry.value as Set<string>);
      return scanIterable(arr, cursor, opts);
    },

    // Sorted set operations
    zadd(key, entries, opts) {
      const zset = getOrCreateZset(key);
      let changed = 0;
      for (const [score, member] of entries) {
        const existing = zset.get(member);
        if (existing !== undefined) {
          if (opts?.nx) continue;
          const newScore = score;
          if (opts?.gt && score <= existing) continue;
          if (opts?.lt && score >= existing) continue;
          if (newScore !== existing) {
            zset.set(member, newScore);
            if (opts?.ch) changed++;
          }
        } else {
          if (opts?.xx) continue;
          zset.set(member, score);
          changed++;
        }
      }
      if (zset.size === 0) data.delete(key);
      return changed;
    },

    zrem(key, members) {
      const entry = assertType(key, "zset");
      if (!entry) return 0;
      const zset = entry.value as Map<string, number>;
      let count = 0;
      for (const m of members) {
        if (zset.delete(m)) count++;
      }
      cleanupEmpty(key, entry);
      return count;
    },

    zscore(key, member) {
      const entry = assertType(key, "zset");
      if (!entry) return null;
      return (entry.value as Map<string, number>).get(member) ?? null;
    },

    zrank(key, member) {
      const entry = assertType(key, "zset");
      if (!entry) return null;
      const zset = entry.value as Map<string, number>;
      if (!zset.has(member)) return null;
      const sorted = sortedMembers(zset);
      return sorted.findIndex(([m]) => m === member);
    },

    zrevrank(key, member) {
      const entry = assertType(key, "zset");
      if (!entry) return null;
      const zset = entry.value as Map<string, number>;
      if (!zset.has(member)) return null;
      const sorted = sortedMembers(zset);
      return sorted.length - 1 - sorted.findIndex(([m]) => m === member);
    },

    zrange(key, start, stop, withScores) {
      const entry = assertType(key, "zset");
      if (!entry) return [];
      const sorted = sortedMembers(entry.value as Map<string, number>);
      const s = resolveIndex(start, sorted.length);
      let e = stop < 0 ? sorted.length + stop : stop;
      e = Math.min(e, sorted.length - 1);
      if (s > e) return [];
      const slice = sorted.slice(s, e + 1);
      if (withScores) {
        const result: string[] = [];
        for (const [m, sc] of slice) {
          result.push(m, String(sc));
        }
        return result;
      }
      return slice.map(([m]) => m);
    },

    zrangebyscore(key, min, max, opts) {
      const entry = assertType(key, "zset");
      if (!entry) return [];
      const minVal = parseScoreBound(min, true);
      const maxVal = parseScoreBound(max, false);
      const sorted = sortedMembers(entry.value as Map<string, number>);
      let filtered = sorted.filter(([, s]) => s >= minVal && s <= maxVal);
      if (opts?.limit) {
        filtered = filtered.slice(opts.limit.offset, opts.limit.offset + opts.limit.count);
      }
      if (opts?.withScores) {
        const result: string[] = [];
        for (const [m, s] of filtered) {
          result.push(m, String(s));
        }
        return result;
      }
      return filtered.map(([m]) => m);
    },

    zrevrange(key, start, stop, withScores) {
      const entry = assertType(key, "zset");
      if (!entry) return [];
      const sorted = sortedMembers(entry.value as Map<string, number>).reverse();
      const s = resolveIndex(start, sorted.length);
      let e = stop < 0 ? sorted.length + stop : stop;
      e = Math.min(e, sorted.length - 1);
      if (s > e) return [];
      const slice = sorted.slice(s, e + 1);
      if (withScores) {
        const result: string[] = [];
        for (const [m, sc] of slice) {
          result.push(m, String(sc));
        }
        return result;
      }
      return slice.map(([m]) => m);
    },

    zrevrangebyscore(key, max, min, opts) {
      const entry = assertType(key, "zset");
      if (!entry) return [];
      const minVal = parseScoreBound(min, true);
      const maxVal = parseScoreBound(max, false);
      const sorted = sortedMembers(entry.value as Map<string, number>).reverse();
      let filtered = sorted.filter(([, s]) => s >= minVal && s <= maxVal);
      if (opts?.limit) {
        filtered = filtered.slice(opts.limit.offset, opts.limit.offset + opts.limit.count);
      }
      if (opts?.withScores) {
        const result: string[] = [];
        for (const [m, s] of filtered) {
          result.push(m, String(s));
        }
        return result;
      }
      return filtered.map(([m]) => m);
    },

    zcard(key) {
      const entry = assertType(key, "zset");
      if (!entry) return 0;
      return (entry.value as Map<string, number>).size;
    },

    zcount(key, min, max) {
      const entry = assertType(key, "zset");
      if (!entry) return 0;
      const minVal = parseScoreBound(min, true);
      const maxVal = parseScoreBound(max, false);
      let count = 0;
      for (const score of (entry.value as Map<string, number>).values()) {
        if (score >= minVal && score <= maxVal) count++;
      }
      return count;
    },

    zincrby(key, increment, member) {
      const zset = getOrCreateZset(key);
      const current = zset.get(member) ?? 0;
      const result = current + increment;
      zset.set(member, result);
      return String(result);
    },

    zpopmin(key, count = 1) {
      const entry = assertType(key, "zset");
      if (!entry) return [];
      const zset = entry.value as Map<string, number>;
      const sorted = sortedMembers(zset);
      const result: string[] = [];
      for (let i = 0; i < count && i < sorted.length; i++) {
        const [m, s] = sorted[i]!;
        result.push(m, String(s));
        zset.delete(m);
      }
      cleanupEmpty(key, entry);
      return result;
    },

    zpopmax(key, count = 1) {
      const entry = assertType(key, "zset");
      if (!entry) return [];
      const zset = entry.value as Map<string, number>;
      const sorted = sortedMembers(zset).reverse();
      const result: string[] = [];
      for (let i = 0; i < count && i < sorted.length; i++) {
        const [m, s] = sorted[i]!;
        result.push(m, String(s));
        zset.delete(m);
      }
      cleanupEmpty(key, entry);
      return result;
    },

    zunionstore(dest, keys, weights) {
      const result = new Map<string, number>();
      for (let i = 0; i < keys.length; i++) {
        const entry = assertType(keys[i]!, "zset");
        if (!entry) continue;
        const w = weights?.[i] ?? 1;
        for (const [m, s] of entry.value as Map<string, number>) {
          result.set(m, (result.get(m) ?? 0) + s * w);
        }
      }
      data.delete(dest);
      if (result.size > 0) {
        data.set(dest, { type: "zset", value: result, expiresAt: null });
      }
      return result.size;
    },

    zinterstore(dest, keys, weights) {
      if (keys.length === 0) {
        data.delete(dest);
        return 0;
      }
      const sets = keys.map((k, i) => {
        const entry = assertType(k, "zset");
        return {
          zset: entry ? (entry.value as Map<string, number>) : new Map<string, number>(),
          weight: weights?.[i] ?? 1,
        };
      });
      const smallest = sets.reduce((a, b) => (a.zset.size <= b.zset.size ? a : b));
      const result = new Map<string, number>();
      for (const [m] of smallest.zset) {
        let score = 0;
        let inAll = true;
        for (const { zset, weight } of sets) {
          const s = zset.get(m);
          if (s === undefined) {
            inAll = false;
            break;
          }
          score += s * weight;
        }
        if (inAll) result.set(m, score);
      }
      data.delete(dest);
      if (result.size > 0) {
        data.set(dest, { type: "zset", value: result, expiresAt: null });
      }
      return result.size;
    },

    zscan(key, cursor, opts) {
      const entry = assertType(key, "zset");
      if (!entry) return [0, []];
      const zset = entry.value as Map<string, number>;
      const allMembers = Array.from(zset.keys());
      const [nextCursor, matchedMembers] = scanIterable(allMembers, cursor, opts);
      const result: string[] = [];
      for (const m of matchedMembers) {
        result.push(m, String(zset.get(m)!));
      }
      return [nextCursor, result];
    },

    // Utility
    ping(message) {
      return message ?? "PONG";
    },

    echo(message) {
      return message;
    },

    time() {
      const ms = now();
      const seconds = Math.floor(ms / 1000);
      const micros = (ms % 1000) * 1000;
      return [String(seconds), String(micros)];
    },
  };

  return store;
}
