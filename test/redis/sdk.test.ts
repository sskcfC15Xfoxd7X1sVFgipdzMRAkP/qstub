import { describe, expect, test } from "bun:test";
import { Redis } from "@upstash/redis";
import { openDb } from "../../src/db.ts";
import { createLogger } from "../../src/logger.ts";
import { createRedisStore } from "../../src/redis/store.ts";
import { createServer } from "../../src/server.ts";

function fresh() {
  const db = openDb(":memory:");
  const logger = createLogger("error");
  const redisStore = createRedisStore();
  const app = createServer({ db, logger, redisStore, redisToken: "dev" });
  return { db, redisStore, app };
}

function makeFetch(app: ReturnType<typeof createServer>) {
  return ((req: Request | string | URL, init?: RequestInit) => {
    const request = req instanceof Request ? req : new Request(String(req), init);
    return app.fetch(request);
  }) as typeof fetch;
}

function withSdk(fn: (redis: InstanceType<typeof Redis>) => Promise<void>) {
  return async () => {
    const { app } = fresh();
    const redis = new Redis({
      url: "http://downstash",
      token: "dev",
    });

    const realFetch = global.fetch;
    global.fetch = makeFetch(app);
    try {
      await fn(redis);
    } finally {
      global.fetch = realFetch;
    }
  };
}

describe("@upstash/redis SDK integration", () => {
  test(
    "set and get round-trip",
    withSdk(async (redis) => {
      await redis.set("foo", "bar");
      const val = await redis.get("foo");
      expect(val).toBe("bar");
    }),
  );

  test(
    "set with EX and get",
    withSdk(async (redis) => {
      await redis.set("k", "v", { ex: 100 });
      const val = await redis.get("k");
      expect(val).toBe("v");
    }),
  );

  test(
    "set with NX",
    withSdk(async (redis) => {
      await redis.set("k", "v1");
      const result = await redis.set("k", "v2", { nx: true });
      expect(result).toBeNull();
      expect(await redis.get<string>("k")).toBe("v1");
    }),
  );

  test(
    "incr and decr",
    withSdk(async (redis) => {
      await redis.incr("counter");
      await redis.incr("counter");
      await redis.decr("counter");
      const val = await redis.get<number>("counter");
      expect(val).toBe(1);
    }),
  );

  test(
    "mset and mget",
    withSdk(async (redis) => {
      await redis.mset({ a: "1", b: "2" });
      const [a, b] = await redis.mget("a", "b");
      expect(a).toBe(1);
      expect(b).toBe(2);
    }),
  );

  test(
    "del",
    withSdk(async (redis) => {
      await redis.set("k", "v");
      await redis.del("k");
      expect(await redis.get("k")).toBeNull();
    }),
  );

  test(
    "exists",
    withSdk(async (redis) => {
      await redis.set("k", "v");
      expect(await redis.exists("k")).toBe(1);
      expect(await redis.exists("nope")).toBe(0);
    }),
  );

  test(
    "hset and hgetall",
    withSdk(async (redis) => {
      await redis.hset("user", { name: "Alice", age: "30" });
      const all = await redis.hgetall("user");
      expect(all).toEqual({ name: "Alice", age: 30 });
    }),
  );

  test(
    "hget",
    withSdk(async (redis) => {
      await redis.hset("h", { f1: "v1", f2: "v2" });
      expect(await redis.hget<string>("h", "f1")).toBe("v1");
      expect(await redis.hget("h", "f3")).toBeNull();
    }),
  );

  test(
    "lpush and lrange",
    withSdk(async (redis) => {
      await redis.lpush("list", "c", "b", "a");
      const items = await redis.lrange("list", 0, -1);
      expect(items).toEqual(["a", "b", "c"]);
    }),
  );

  test(
    "rpush, lpop, rpop",
    withSdk(async (redis) => {
      await redis.rpush("list", "a", "b", "c");
      expect(await redis.lpop<string>("list")).toBe("a");
      expect(await redis.rpop<string>("list")).toBe("c");
    }),
  );

  test(
    "sadd and smembers",
    withSdk(async (redis) => {
      await redis.sadd("s", "a", "b", "c");
      const members = await redis.smembers("s");
      expect(members.sort()).toEqual(["a", "b", "c"]);
    }),
  );

  test(
    "sismember",
    withSdk(async (redis) => {
      await redis.sadd("s", "a");
      expect(await redis.sismember("s", "a")).toBe(1);
      expect(await redis.sismember("s", "b")).toBe(0);
    }),
  );

  test(
    "zadd and zrange",
    withSdk(async (redis) => {
      await redis.zadd(
        "z",
        { score: 1, member: "a" },
        { score: 3, member: "c" },
        { score: 2, member: "b" },
      );
      const members = await redis.zrange("z", 0, -1);
      expect(members).toEqual(["a", "b", "c"]);
    }),
  );

  test(
    "pipeline",
    withSdk(async (redis) => {
      const pipe = redis.pipeline();
      pipe.set("p1", "v1");
      pipe.set("p2", "v2");
      pipe.mget("p1", "p2");
      const results = await pipe.exec();
      expect(results).toEqual(["OK", "OK", ["v1", "v2"]]);
    }),
  );

  test(
    "type",
    withSdk(async (redis) => {
      await redis.set("str", "v");
      expect(await redis.type("str")).toBe("string");
    }),
  );

  test(
    "dbsize",
    withSdk(async (redis) => {
      await redis.set("a", "1");
      await redis.set("b", "2");
      expect(await redis.dbsize()).toBe(2);
    }),
  );

  test(
    "ping",
    withSdk(async (redis) => {
      const result = await redis.ping();
      expect(result).toBe("PONG");
    }),
  );
});
