import { describe, expect, test } from "bun:test";
import { executeCommand } from "../../src/redis/commands.ts";
import { createRedisStore } from "../../src/redis/store.ts";

function fresh() {
  return createRedisStore();
}

function exec(store: ReturnType<typeof fresh>, ...args: string[]) {
  return executeCommand(store, args);
}

describe("command dispatcher", () => {
  test("empty command returns error", () => {
    const s = fresh();
    expect(executeCommand(s, [])).toEqual({ error: "ERR empty command" });
  });

  test("unknown command returns error", () => {
    const s = fresh();
    expect(exec(s, "NOSUCH")).toEqual({ error: "ERR unknown command 'NOSUCH'" });
  });

  test("case insensitive commands", () => {
    const s = fresh();
    expect(exec(s, "ping")).toEqual({ result: "PONG" });
    expect(exec(s, "PING")).toEqual({ result: "PONG" });
    expect(exec(s, "Ping")).toEqual({ result: "PONG" });
  });
});

describe("string commands", () => {
  test("SET and GET", () => {
    const s = fresh();
    expect(exec(s, "SET", "k", "v")).toEqual({ result: "OK" });
    expect(exec(s, "GET", "k")).toEqual({ result: "v" });
  });

  test("SET with EX", () => {
    const s = fresh();
    expect(exec(s, "SET", "k", "v", "EX", "100")).toEqual({ result: "OK" });
    const ttl = exec(s, "TTL", "k") as { result: number };
    expect(ttl.result).toBeGreaterThan(0);
  });

  test("SET with NX returns null when key exists", () => {
    const s = fresh();
    exec(s, "SET", "k", "v1");
    expect(exec(s, "SET", "k", "v2", "NX")).toEqual({ result: null });
  });

  test("MSET and MGET", () => {
    const s = fresh();
    expect(exec(s, "MSET", "a", "1", "b", "2")).toEqual({ result: "OK" });
    expect(exec(s, "MGET", "a", "b", "c")).toEqual({ result: ["1", "2", null] });
  });

  test("INCR and DECR", () => {
    const s = fresh();
    expect(exec(s, "INCR", "c")).toEqual({ result: 1 });
    expect(exec(s, "INCRBY", "c", "5")).toEqual({ result: 6 });
    expect(exec(s, "DECR", "c")).toEqual({ result: 5 });
    expect(exec(s, "DECRBY", "c", "3")).toEqual({ result: 2 });
  });

  test("APPEND and STRLEN", () => {
    const s = fresh();
    expect(exec(s, "APPEND", "k", "hello")).toEqual({ result: 5 });
    expect(exec(s, "STRLEN", "k")).toEqual({ result: 5 });
  });

  test("SETNX", () => {
    const s = fresh();
    expect(exec(s, "SETNX", "k", "v1")).toEqual({ result: 1 });
    expect(exec(s, "SETNX", "k", "v2")).toEqual({ result: 0 });
  });

  test("SETEX", () => {
    const s = fresh();
    expect(exec(s, "SETEX", "k", "100", "v")).toEqual({ result: "OK" });
    const ttl = exec(s, "TTL", "k") as { result: number };
    expect(ttl.result).toBeGreaterThan(0);
  });

  test("GETDEL", () => {
    const s = fresh();
    exec(s, "SET", "k", "v");
    expect(exec(s, "GETDEL", "k")).toEqual({ result: "v" });
    expect(exec(s, "GET", "k")).toEqual({ result: null });
  });

  test("wrong arg count", () => {
    const s = fresh();
    expect(exec(s, "GET")).toEqual({ error: "ERR wrong number of arguments for 'get' command" });
    expect(exec(s, "SET", "k")).toEqual({
      error: "ERR wrong number of arguments for 'set' command",
    });
  });
});

describe("hash commands", () => {
  test("HSET and HGET", () => {
    const s = fresh();
    expect(exec(s, "HSET", "h", "f1", "v1", "f2", "v2")).toEqual({ result: 2 });
    expect(exec(s, "HGET", "h", "f1")).toEqual({ result: "v1" });
  });

  test("HGETALL", () => {
    const s = fresh();
    exec(s, "HSET", "h", "f1", "v1");
    const result = exec(s, "HGETALL", "h") as { result: string[] };
    expect(result.result).toContain("f1");
    expect(result.result).toContain("v1");
  });

  test("HDEL and HEXISTS", () => {
    const s = fresh();
    exec(s, "HSET", "h", "f", "v");
    expect(exec(s, "HEXISTS", "h", "f")).toEqual({ result: 1 });
    expect(exec(s, "HDEL", "h", "f")).toEqual({ result: 1 });
    expect(exec(s, "HEXISTS", "h", "f")).toEqual({ result: 0 });
  });

  test("HINCRBY", () => {
    const s = fresh();
    expect(exec(s, "HINCRBY", "h", "count", "5")).toEqual({ result: 5 });
    expect(exec(s, "HINCRBY", "h", "count", "3")).toEqual({ result: 8 });
  });
});

describe("list commands", () => {
  test("LPUSH, RPUSH, LRANGE", () => {
    const s = fresh();
    expect(exec(s, "RPUSH", "l", "a", "b")).toEqual({ result: 2 });
    expect(exec(s, "LPUSH", "l", "z")).toEqual({ result: 3 });
    expect(exec(s, "LRANGE", "l", "0", "-1")).toEqual({ result: ["z", "a", "b"] });
  });

  test("LPOP and RPOP", () => {
    const s = fresh();
    exec(s, "RPUSH", "l", "a", "b", "c");
    expect(exec(s, "LPOP", "l")).toEqual({ result: "a" });
    expect(exec(s, "RPOP", "l")).toEqual({ result: "c" });
  });

  test("LLEN and LINDEX", () => {
    const s = fresh();
    exec(s, "RPUSH", "l", "a", "b");
    expect(exec(s, "LLEN", "l")).toEqual({ result: 2 });
    expect(exec(s, "LINDEX", "l", "0")).toEqual({ result: "a" });
    expect(exec(s, "LINDEX", "l", "-1")).toEqual({ result: "b" });
  });
});

describe("set commands", () => {
  test("SADD, SMEMBERS, SCARD", () => {
    const s = fresh();
    expect(exec(s, "SADD", "s", "a", "b", "a")).toEqual({ result: 2 });
    expect(exec(s, "SCARD", "s")).toEqual({ result: 2 });
    const members = exec(s, "SMEMBERS", "s") as { result: string[] };
    expect(members.result.sort()).toEqual(["a", "b"]);
  });

  test("SISMEMBER", () => {
    const s = fresh();
    exec(s, "SADD", "s", "a");
    expect(exec(s, "SISMEMBER", "s", "a")).toEqual({ result: 1 });
    expect(exec(s, "SISMEMBER", "s", "b")).toEqual({ result: 0 });
  });
});

describe("sorted set commands", () => {
  test("ZADD and ZSCORE", () => {
    const s = fresh();
    expect(exec(s, "ZADD", "z", "1", "a", "2", "b")).toEqual({ result: 2 });
    expect(exec(s, "ZSCORE", "z", "a")).toEqual({ result: "1" });
  });

  test("ZRANGE", () => {
    const s = fresh();
    exec(s, "ZADD", "z", "3", "c", "1", "a", "2", "b");
    expect(exec(s, "ZRANGE", "z", "0", "-1")).toEqual({ result: ["a", "b", "c"] });
  });

  test("ZRANGE with WITHSCORES", () => {
    const s = fresh();
    exec(s, "ZADD", "z", "1", "a", "2", "b");
    expect(exec(s, "ZRANGE", "z", "0", "-1", "WITHSCORES")).toEqual({
      result: ["a", "1", "b", "2"],
    });
  });

  test("ZRANK and ZREVRANK", () => {
    const s = fresh();
    exec(s, "ZADD", "z", "1", "a", "2", "b", "3", "c");
    expect(exec(s, "ZRANK", "z", "a")).toEqual({ result: 0 });
    expect(exec(s, "ZREVRANK", "z", "a")).toEqual({ result: 2 });
  });
});

describe("key commands", () => {
  test("DEL", () => {
    const s = fresh();
    exec(s, "SET", "a", "1");
    exec(s, "SET", "b", "2");
    expect(exec(s, "DEL", "a", "b", "c")).toEqual({ result: 2 });
  });

  test("EXISTS", () => {
    const s = fresh();
    exec(s, "SET", "a", "1");
    expect(exec(s, "EXISTS", "a", "b")).toEqual({ result: 1 });
  });

  test("TYPE", () => {
    const s = fresh();
    exec(s, "SET", "k", "v");
    expect(exec(s, "TYPE", "k")).toEqual({ result: "string" });
    expect(exec(s, "TYPE", "nope")).toEqual({ result: "none" });
  });

  test("RENAME", () => {
    const s = fresh();
    exec(s, "SET", "old", "v");
    expect(exec(s, "RENAME", "old", "new")).toEqual({ result: "OK" });
    expect(exec(s, "GET", "new")).toEqual({ result: "v" });
  });

  test("KEYS", () => {
    const s = fresh();
    exec(s, "SET", "user:1", "a");
    exec(s, "SET", "user:2", "b");
    exec(s, "SET", "post:1", "c");
    const result = exec(s, "KEYS", "user:*") as { result: string[] };
    expect(result.result.sort()).toEqual(["user:1", "user:2"]);
  });

  test("DBSIZE", () => {
    const s = fresh();
    exec(s, "SET", "a", "1");
    exec(s, "SET", "b", "2");
    expect(exec(s, "DBSIZE")).toEqual({ result: 2 });
  });

  test("FLUSHDB", () => {
    const s = fresh();
    exec(s, "SET", "a", "1");
    exec(s, "FLUSHDB");
    expect(exec(s, "DBSIZE")).toEqual({ result: 0 });
  });
});

describe("utility commands", () => {
  test("PING", () => {
    const s = fresh();
    expect(exec(s, "PING")).toEqual({ result: "PONG" });
    expect(exec(s, "PING", "hello")).toEqual({ result: "hello" });
  });

  test("ECHO", () => {
    const s = fresh();
    expect(exec(s, "ECHO", "hi")).toEqual({ result: "hi" });
  });

  test("TIME", () => {
    const s = fresh();
    const result = exec(s, "TIME") as { result: [string, string] };
    expect(Number.parseInt(result.result[0])).toBeGreaterThan(0);
  });
});

describe("base64 encoding", () => {
  test("string results are base64 encoded", () => {
    const s = fresh();
    exec(s, "SET", "k", "hello");
    const result = executeCommand(s, ["GET", "k"], "base64");
    expect(result).toEqual({ result: btoa("hello") });
  });

  test("null stays null with base64", () => {
    const s = fresh();
    const result = executeCommand(s, ["GET", "missing"], "base64");
    expect(result).toEqual({ result: null });
  });

  test("numbers stay numbers with base64", () => {
    const s = fresh();
    const result = executeCommand(s, ["INCR", "c"], "base64");
    expect(result).toEqual({ result: 1 });
  });

  test("array elements are base64 encoded", () => {
    const s = fresh();
    exec(s, "MSET", "a", "x", "b", "y");
    const result = executeCommand(s, ["MGET", "a", "b"], "base64") as { result: string[] };
    expect(result.result).toEqual([btoa("x"), btoa("y")]);
  });
});

describe("WRONGTYPE through commands", () => {
  test("GET on list returns WRONGTYPE error", () => {
    const s = fresh();
    exec(s, "LPUSH", "l", "a");
    const result = exec(s, "GET", "l");
    expect(result).toEqual({
      error: "WRONGTYPE Operation against a key holding the wrong kind of value",
    });
  });
});
