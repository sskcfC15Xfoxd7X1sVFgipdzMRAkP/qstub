import { describe, expect, test } from "bun:test";
import { createRedisStore } from "../../src/redis/store.ts";

function fresh() {
  return createRedisStore();
}

describe("string operations", () => {
  test("set and get", () => {
    const s = fresh();
    expect(s.set("k", "v")).toBe("OK");
    expect(s.get("k")).toBe("v");
  });

  test("get missing key returns null", () => {
    const s = fresh();
    expect(s.get("nope")).toBeNull();
  });

  test("set with NX skips existing", () => {
    const s = fresh();
    s.set("k", "v1");
    expect(s.set("k", "v2", { nx: true })).toBeNull();
    expect(s.get("k")).toBe("v1");
  });

  test("set with XX skips missing", () => {
    const s = fresh();
    expect(s.set("k", "v", { xx: true })).toBeNull();
    expect(s.get("k")).toBeNull();
  });

  test("set with GET returns old value", () => {
    const s = fresh();
    s.set("k", "old");
    expect(s.set("k", "new", { get: true })).toBe("old");
    expect(s.get("k")).toBe("new");
  });

  test("mset and mget", () => {
    const s = fresh();
    s.mset([
      ["a", "1"],
      ["b", "2"],
    ]);
    expect(s.mget(["a", "b", "c"])).toEqual(["1", "2", null]);
  });

  test("incr and decr", () => {
    const s = fresh();
    expect(s.incr("c")).toBe(1);
    expect(s.incr("c")).toBe(2);
    expect(s.decr("c")).toBe(1);
    expect(s.decrby("c", 5)).toBe(-4);
    expect(s.incrby("c", 10)).toBe(6);
  });

  test("incrbyfloat", () => {
    const s = fresh();
    s.set("f", "10.5");
    expect(s.incrbyfloat("f", 0.1)).toBe("10.6");
  });

  test("append and strlen", () => {
    const s = fresh();
    expect(s.append("k", "hello")).toBe(5);
    expect(s.append("k", " world")).toBe(11);
    expect(s.strlen("k")).toBe(11);
  });

  test("getrange", () => {
    const s = fresh();
    s.set("k", "hello world");
    expect(s.getrange("k", 0, 4)).toBe("hello");
    expect(s.getrange("k", -5, -1)).toBe("world");
  });

  test("setrange", () => {
    const s = fresh();
    s.set("k", "hello world");
    expect(s.setrange("k", 6, "redis")).toBe(11);
    expect(s.get("k")).toBe("hello redis");
  });

  test("getdel", () => {
    const s = fresh();
    s.set("k", "v");
    expect(s.getdel("k")).toBe("v");
    expect(s.get("k")).toBeNull();
  });

  test("setnx", () => {
    const s = fresh();
    expect(s.setnx("k", "v1")).toBe(true);
    expect(s.setnx("k", "v2")).toBe(false);
    expect(s.get("k")).toBe("v1");
  });

  test("setex sets TTL", () => {
    const s = fresh();
    s.setex("k", 100, "v");
    expect(s.get("k")).toBe("v");
    expect(s.ttl("k")).toBeGreaterThan(0);
  });
});

describe("TTL and expiry", () => {
  test("set with EX sets TTL", () => {
    const s = fresh();
    s.set("k", "v", { ex: 100 });
    const ttl = s.ttl("k");
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(100);
  });

  test("ttl returns -2 for missing key", () => {
    const s = fresh();
    expect(s.ttl("nope")).toBe(-2);
  });

  test("ttl returns -1 for key without expiry", () => {
    const s = fresh();
    s.set("k", "v");
    expect(s.ttl("k")).toBe(-1);
  });

  test("persist removes TTL", () => {
    const s = fresh();
    s.set("k", "v", { ex: 100 });
    expect(s.persist("k")).toBe(true);
    expect(s.ttl("k")).toBe(-1);
  });

  test("expire sets TTL on existing key", () => {
    const s = fresh();
    s.set("k", "v");
    expect(s.expire("k", 50)).toBe(true);
    expect(s.ttl("k")).toBeGreaterThan(0);
  });

  test("expire returns false for missing key", () => {
    const s = fresh();
    expect(s.expire("nope", 50)).toBe(false);
  });

  test("pttl returns milliseconds", () => {
    const s = fresh();
    s.set("k", "v", { px: 5000 });
    expect(s.pttl("k")).toBeGreaterThan(0);
    expect(s.pttl("k")).toBeLessThanOrEqual(5000);
  });
});

describe("key operations", () => {
  test("del removes keys", () => {
    const s = fresh();
    s.set("a", "1");
    s.set("b", "2");
    expect(s.del(["a", "b", "c"])).toBe(2);
    expect(s.get("a")).toBeNull();
  });

  test("exists counts existing keys", () => {
    const s = fresh();
    s.set("a", "1");
    expect(s.exists(["a", "b"])).toBe(1);
  });

  test("rename", () => {
    const s = fresh();
    s.set("old", "v");
    s.rename("old", "new");
    expect(s.get("old")).toBeNull();
    expect(s.get("new")).toBe("v");
  });

  test("rename throws for missing key", () => {
    const s = fresh();
    expect(() => s.rename("nope", "new")).toThrow("no such key");
  });

  test("type returns correct types", () => {
    const s = fresh();
    s.set("str", "v");
    s.lpush("lst", ["a"]);
    s.sadd("st", ["a"]);
    s.hset("hs", [["f", "v"]]);
    s.zadd("zs", [[1, "a"]], {});
    expect(s.type("str")).toBe("string");
    expect(s.type("lst")).toBe("list");
    expect(s.type("st")).toBe("set");
    expect(s.type("hs")).toBe("hash");
    expect(s.type("zs")).toBe("zset");
    expect(s.type("nope")).toBe("none");
  });

  test("keys with glob pattern", () => {
    const s = fresh();
    s.set("user:1", "a");
    s.set("user:2", "b");
    s.set("post:1", "c");
    expect(s.keys("user:*").sort()).toEqual(["user:1", "user:2"]);
    expect(s.keys("*:1").sort()).toEqual(["post:1", "user:1"]);
  });

  test("scan iterates keys", () => {
    const s = fresh();
    s.set("a", "1");
    s.set("b", "2");
    s.set("c", "3");
    const [cursor, keys] = s.scan(0, { count: 10 });
    expect(cursor).toBe(0);
    expect(keys.sort()).toEqual(["a", "b", "c"]);
  });

  test("dbsize", () => {
    const s = fresh();
    s.set("a", "1");
    s.set("b", "2");
    expect(s.dbsize()).toBe(2);
  });

  test("flushdb clears all", () => {
    const s = fresh();
    s.set("a", "1");
    s.flushdb();
    expect(s.dbsize()).toBe(0);
  });

  test("copy", () => {
    const s = fresh();
    s.set("src", "v");
    expect(s.copy("src", "dst")).toBe(true);
    expect(s.get("dst")).toBe("v");
  });

  test("copy without replace fails on existing dest", () => {
    const s = fresh();
    s.set("src", "v1");
    s.set("dst", "v2");
    expect(s.copy("src", "dst")).toBe(false);
    expect(s.get("dst")).toBe("v2");
  });
});

describe("hash operations", () => {
  test("hset and hget", () => {
    const s = fresh();
    expect(
      s.hset("h", [
        ["f1", "v1"],
        ["f2", "v2"],
      ]),
    ).toBe(2);
    expect(s.hget("h", "f1")).toBe("v1");
    expect(s.hget("h", "f3")).toBeNull();
  });

  test("hgetall returns flat array", () => {
    const s = fresh();
    s.hset("h", [
      ["f1", "v1"],
      ["f2", "v2"],
    ]);
    const all = s.hgetall("h");
    expect(all).toContain("f1");
    expect(all).toContain("v1");
    expect(all.length).toBe(4);
  });

  test("hdel and hexists", () => {
    const s = fresh();
    s.hset("h", [["f1", "v1"]]);
    expect(s.hexists("h", "f1")).toBe(true);
    expect(s.hdel("h", ["f1"])).toBe(1);
    expect(s.hexists("h", "f1")).toBe(false);
  });

  test("hincrby", () => {
    const s = fresh();
    expect(s.hincrby("h", "count", 5)).toBe(5);
    expect(s.hincrby("h", "count", 3)).toBe(8);
  });

  test("hsetnx", () => {
    const s = fresh();
    expect(s.hsetnx("h", "f", "v1")).toBe(true);
    expect(s.hsetnx("h", "f", "v2")).toBe(false);
    expect(s.hget("h", "f")).toBe("v1");
  });

  test("hlen, hkeys, hvals", () => {
    const s = fresh();
    s.hset("h", [
      ["a", "1"],
      ["b", "2"],
    ]);
    expect(s.hlen("h")).toBe(2);
    expect(s.hkeys("h").sort()).toEqual(["a", "b"]);
    expect(s.hvals("h").sort()).toEqual(["1", "2"]);
  });
});

describe("list operations", () => {
  test("lpush and rpush", () => {
    const s = fresh();
    expect(s.lpush("l", ["b", "a"])).toBe(2);
    expect(s.rpush("l", ["c"])).toBe(3);
    expect(s.lrange("l", 0, -1)).toEqual(["a", "b", "c"]);
  });

  test("lpop and rpop", () => {
    const s = fresh();
    s.rpush("l", ["a", "b", "c"]);
    expect(s.lpop("l")).toBe("a");
    expect(s.rpop("l")).toBe("c");
    expect(s.lrange("l", 0, -1)).toEqual(["b"]);
  });

  test("lpop with count", () => {
    const s = fresh();
    s.rpush("l", ["a", "b", "c"]);
    expect(s.lpop("l", 2)).toEqual(["a", "b"]);
  });

  test("llen", () => {
    const s = fresh();
    s.rpush("l", ["a", "b"]);
    expect(s.llen("l")).toBe(2);
  });

  test("lindex", () => {
    const s = fresh();
    s.rpush("l", ["a", "b", "c"]);
    expect(s.lindex("l", 0)).toBe("a");
    expect(s.lindex("l", -1)).toBe("c");
    expect(s.lindex("l", 5)).toBeNull();
  });

  test("lset", () => {
    const s = fresh();
    s.rpush("l", ["a", "b", "c"]);
    s.lset("l", 1, "x");
    expect(s.lrange("l", 0, -1)).toEqual(["a", "x", "c"]);
  });

  test("linsert", () => {
    const s = fresh();
    s.rpush("l", ["a", "c"]);
    expect(s.linsert("l", true, "c", "b")).toBe(3);
    expect(s.lrange("l", 0, -1)).toEqual(["a", "b", "c"]);
  });

  test("lrem", () => {
    const s = fresh();
    s.rpush("l", ["a", "b", "a", "c", "a"]);
    expect(s.lrem("l", 2, "a")).toBe(2);
    expect(s.lrange("l", 0, -1)).toEqual(["b", "c", "a"]);
  });

  test("ltrim", () => {
    const s = fresh();
    s.rpush("l", ["a", "b", "c", "d"]);
    s.ltrim("l", 1, 2);
    expect(s.lrange("l", 0, -1)).toEqual(["b", "c"]);
  });

  test("empty list is cleaned up", () => {
    const s = fresh();
    s.rpush("l", ["a"]);
    s.lpop("l");
    expect(s.type("l")).toBe("none");
  });
});

describe("set operations", () => {
  test("sadd and smembers", () => {
    const s = fresh();
    expect(s.sadd("s", ["a", "b", "a"])).toBe(2);
    expect(s.smembers("s").sort()).toEqual(["a", "b"]);
  });

  test("srem", () => {
    const s = fresh();
    s.sadd("s", ["a", "b", "c"]);
    expect(s.srem("s", ["b", "d"])).toBe(1);
    expect(s.smembers("s").sort()).toEqual(["a", "c"]);
  });

  test("sismember and scard", () => {
    const s = fresh();
    s.sadd("s", ["a", "b"]);
    expect(s.sismember("s", "a")).toBe(true);
    expect(s.sismember("s", "c")).toBe(false);
    expect(s.scard("s")).toBe(2);
  });

  test("sunion", () => {
    const s = fresh();
    s.sadd("a", ["1", "2"]);
    s.sadd("b", ["2", "3"]);
    expect(s.sunion(["a", "b"]).sort()).toEqual(["1", "2", "3"]);
  });

  test("sinter", () => {
    const s = fresh();
    s.sadd("a", ["1", "2"]);
    s.sadd("b", ["2", "3"]);
    expect(s.sinter(["a", "b"])).toEqual(["2"]);
  });

  test("sdiff", () => {
    const s = fresh();
    s.sadd("a", ["1", "2", "3"]);
    s.sadd("b", ["2"]);
    expect(s.sdiff(["a", "b"]).sort()).toEqual(["1", "3"]);
  });

  test("empty set is cleaned up", () => {
    const s = fresh();
    s.sadd("s", ["a"]);
    s.srem("s", ["a"]);
    expect(s.type("s")).toBe("none");
  });
});

describe("sorted set operations", () => {
  test("zadd and zscore", () => {
    const s = fresh();
    expect(
      s.zadd(
        "z",
        [
          [1, "a"],
          [2, "b"],
        ],
        {},
      ),
    ).toBe(2);
    expect(s.zscore("z", "a")).toBe(1);
    expect(s.zscore("z", "c")).toBeNull();
  });

  test("zrange", () => {
    const s = fresh();
    s.zadd(
      "z",
      [
        [3, "c"],
        [1, "a"],
        [2, "b"],
      ],
      {},
    );
    expect(s.zrange("z", 0, -1)).toEqual(["a", "b", "c"]);
  });

  test("zrange with scores", () => {
    const s = fresh();
    s.zadd(
      "z",
      [
        [1, "a"],
        [2, "b"],
      ],
      {},
    );
    expect(s.zrange("z", 0, -1, true)).toEqual(["a", "1", "b", "2"]);
  });

  test("zrevrange", () => {
    const s = fresh();
    s.zadd(
      "z",
      [
        [1, "a"],
        [2, "b"],
        [3, "c"],
      ],
      {},
    );
    expect(s.zrevrange("z", 0, 1)).toEqual(["c", "b"]);
  });

  test("zrangebyscore", () => {
    const s = fresh();
    s.zadd(
      "z",
      [
        [1, "a"],
        [2, "b"],
        [3, "c"],
      ],
      {},
    );
    expect(s.zrangebyscore("z", "1", "2")).toEqual(["a", "b"]);
    expect(s.zrangebyscore("z", "(1", "3")).toEqual(["b", "c"]);
  });

  test("zrank and zrevrank", () => {
    const s = fresh();
    s.zadd(
      "z",
      [
        [1, "a"],
        [2, "b"],
        [3, "c"],
      ],
      {},
    );
    expect(s.zrank("z", "a")).toBe(0);
    expect(s.zrevrank("z", "a")).toBe(2);
  });

  test("zrem", () => {
    const s = fresh();
    s.zadd(
      "z",
      [
        [1, "a"],
        [2, "b"],
      ],
      {},
    );
    expect(s.zrem("z", ["a"])).toBe(1);
    expect(s.zcard("z")).toBe(1);
  });

  test("zincrby", () => {
    const s = fresh();
    s.zadd("z", [[1, "a"]], {});
    expect(s.zincrby("z", 5, "a")).toBe("6");
    expect(s.zscore("z", "a")).toBe(6);
  });

  test("zpopmin and zpopmax", () => {
    const s = fresh();
    s.zadd(
      "z",
      [
        [1, "a"],
        [2, "b"],
        [3, "c"],
      ],
      {},
    );
    expect(s.zpopmin("z")).toEqual(["a", "1"]);
    expect(s.zpopmax("z")).toEqual(["c", "3"]);
    expect(s.zcard("z")).toBe(1);
  });

  test("zcount", () => {
    const s = fresh();
    s.zadd(
      "z",
      [
        [1, "a"],
        [2, "b"],
        [3, "c"],
      ],
      {},
    );
    expect(s.zcount("z", "1", "2")).toBe(2);
    expect(s.zcount("z", "-inf", "+inf")).toBe(3);
  });

  test("zunionstore", () => {
    const s = fresh();
    s.zadd(
      "a",
      [
        [1, "x"],
        [2, "y"],
      ],
      {},
    );
    s.zadd(
      "b",
      [
        [3, "y"],
        [4, "z"],
      ],
      {},
    );
    expect(s.zunionstore("out", ["a", "b"])).toBe(3);
    expect(s.zscore("out", "y")).toBe(5);
  });

  test("zinterstore", () => {
    const s = fresh();
    s.zadd(
      "a",
      [
        [1, "x"],
        [2, "y"],
      ],
      {},
    );
    s.zadd(
      "b",
      [
        [3, "y"],
        [4, "z"],
      ],
      {},
    );
    expect(s.zinterstore("out", ["a", "b"])).toBe(1);
    expect(s.zscore("out", "y")).toBe(5);
  });
});

describe("WRONGTYPE errors", () => {
  test("string op on list throws", () => {
    const s = fresh();
    s.lpush("l", ["a"]);
    expect(() => s.get("l")).toThrow("WRONGTYPE");
  });

  test("list op on string throws", () => {
    const s = fresh();
    s.set("k", "v");
    expect(() => s.lpush("k", ["a"])).toThrow("WRONGTYPE");
  });

  test("hash op on string throws", () => {
    const s = fresh();
    s.set("k", "v");
    expect(() => s.hset("k", [["f", "v"]])).toThrow("WRONGTYPE");
  });
});

describe("utility", () => {
  test("ping", () => {
    const s = fresh();
    expect(s.ping()).toBe("PONG");
    expect(s.ping("hello")).toBe("hello");
  });

  test("echo", () => {
    const s = fresh();
    expect(s.echo("hi")).toBe("hi");
  });

  test("time returns two strings", () => {
    const s = fresh();
    const [sec, micro] = s.time();
    expect(Number.parseInt(sec)).toBeGreaterThan(0);
    expect(Number.parseInt(micro)).toBeGreaterThanOrEqual(0);
  });
});
