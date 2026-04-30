import { describe, expect, test } from "bun:test";
import { openDb } from "../../src/db.ts";
import { createLogger } from "../../src/logger.ts";
import { createRedisStore } from "../../src/redis/store.ts";
import { createServer } from "../../src/server.ts";

function fresh() {
  const db = openDb(":memory:");
  const logger = createLogger("error");
  const redisStore = createRedisStore();
  const app = createServer({ db, logger, redisStore, redisToken: "test-token" });
  return { db, redisStore, app };
}

function req(
  url: string,
  opts: { method?: string; body?: unknown; headers?: Record<string, string> } = {},
) {
  const headers: Record<string, string> = {
    authorization: "Bearer test-token",
    ...opts.headers,
  };
  const init: RequestInit = { method: opts.method ?? "POST", headers };
  if (opts.body !== undefined) {
    init.body = JSON.stringify(opts.body);
    headers["content-type"] = "application/json";
  }
  return new Request(url, init);
}

describe("redis auth", () => {
  test("rejects missing auth", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      new Request("http://downstash/", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: '["PING"]',
      }),
    );
    expect(res.status).toBe(401);
    const json = (await res.json()) as { error: string };
    expect(json.error).toBe("Unauthorized");
  });

  test("rejects wrong token", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      req("http://downstash/", {
        body: ["PING"],
        headers: { authorization: "Bearer wrong-token" },
      }),
    );
    expect(res.status).toBe(401);
  });

  test("accepts correct token", async () => {
    const { app } = fresh();
    const res = await app.fetch(req("http://downstash/", { body: ["PING"] }));
    expect(res.status).toBe(200);
    const json = (await res.json()) as { result: string };
    expect(json.result).toBe("PONG");
  });
});

describe("POST / single command", () => {
  test("SET and GET", async () => {
    const { app } = fresh();

    const setRes = await app.fetch(req("http://downstash/", { body: ["SET", "k", "v"] }));
    expect(setRes.status).toBe(200);
    expect(await setRes.json()).toEqual({ result: "OK" });

    const getRes = await app.fetch(req("http://downstash/", { body: ["GET", "k"] }));
    expect(getRes.status).toBe(200);
    expect(await getRes.json()).toEqual({ result: "v" });
  });

  test("rejects empty array", async () => {
    const { app } = fresh();
    const res = await app.fetch(req("http://downstash/", { body: [] }));
    expect(res.status).toBe(400);
  });

  test("rejects non-array body", async () => {
    const { app } = fresh();
    const res = await app.fetch(req("http://downstash/", { body: { cmd: "SET" } }));
    expect(res.status).toBe(400);
  });

  test("returns WRONGTYPE error in response body, not HTTP error", async () => {
    const { app } = fresh();
    await app.fetch(req("http://downstash/", { body: ["LPUSH", "l", "a"] }));
    const res = await app.fetch(req("http://downstash/", { body: ["GET", "l"] }));
    expect(res.status).toBe(200);
    const json = (await res.json()) as { error: string };
    expect(json.error).toMatch(/WRONGTYPE/);
  });
});

describe("POST /pipeline", () => {
  test("executes multiple commands", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      req("http://downstash/pipeline", {
        body: [
          ["SET", "a", "1"],
          ["SET", "b", "2"],
          ["MGET", "a", "b"],
        ],
      }),
    );
    expect(res.status).toBe(200);
    const json = (await res.json()) as { result: unknown }[];
    expect(json).toEqual([{ result: "OK" }, { result: "OK" }, { result: ["1", "2"] }]);
  });

  test("individual error doesn't stop pipeline", async () => {
    const { app } = fresh();
    await app.fetch(req("http://downstash/", { body: ["LPUSH", "l", "a"] }));
    const res = await app.fetch(
      req("http://downstash/pipeline", {
        body: [
          ["GET", "l"],
          ["PING"],
        ],
      }),
    );
    expect(res.status).toBe(200);
    const json = (await res.json()) as ({ result?: unknown; error?: string })[];
    expect(json[0]!.error).toMatch(/WRONGTYPE/);
    expect(json[1]).toEqual({ result: "PONG" });
  });
});

describe("POST /multi-exec", () => {
  test("executes atomically", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      req("http://downstash/multi-exec", {
        body: [
          ["SET", "k", "v"],
          ["GET", "k"],
        ],
      }),
    );
    expect(res.status).toBe(200);
    const json = (await res.json()) as { result: unknown }[];
    expect(json).toEqual([{ result: "OK" }, { result: "v" }]);
  });

  test("error aborts the transaction", async () => {
    const { app } = fresh();
    await app.fetch(req("http://downstash/", { body: ["LPUSH", "l", "a"] }));
    const res = await app.fetch(
      req("http://downstash/multi-exec", {
        body: [
          ["GET", "l"],
          ["PING"],
        ],
      }),
    );
    expect(res.status).toBe(400);
    const json = (await res.json()) as { error: string };
    expect(json.error).toMatch(/WRONGTYPE/);
  });
});

describe("URL-path commands", () => {
  test("GET /get/:key", async () => {
    const { app } = fresh();
    await app.fetch(req("http://downstash/", { body: ["SET", "mykey", "myval"] }));

    const res = await app.fetch(
      req("http://downstash/get/mykey", { method: "GET" }),
    );
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ result: "myval" });
  });

  test("POST /set/:key/:value", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      req("http://downstash/set/foo/bar"),
    );
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ result: "OK" });

    const getRes = await app.fetch(req("http://downstash/", { body: ["GET", "foo"] }));
    expect(await getRes.json()).toEqual({ result: "bar" });
  });

  test("URL-decoded arguments", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      req(`http://downstash/set/${encodeURIComponent("my key")}/${encodeURIComponent("my value")}`),
    );
    expect(res.status).toBe(200);

    const getRes = await app.fetch(req("http://downstash/", { body: ["GET", "my key"] }));
    expect(await getRes.json()).toEqual({ result: "my value" });
  });

  test("no-arg commands like PING", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      req("http://downstash/ping", { method: "GET" }),
    );
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ result: "PONG" });
  });
});

describe("base64 encoding", () => {
  test("responses are base64 encoded when header is set", async () => {
    const { app } = fresh();
    await app.fetch(req("http://downstash/", { body: ["SET", "k", "hello"] }));

    const res = await app.fetch(
      req("http://downstash/", {
        body: ["GET", "k"],
        headers: { "upstash-encoding": "base64" },
      }),
    );
    expect(res.status).toBe(200);
    const json = (await res.json()) as { result: string };
    expect(json.result).toBe(btoa("hello"));
  });

  test("numbers are not base64 encoded", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      req("http://downstash/", {
        body: ["INCR", "counter"],
        headers: { "upstash-encoding": "base64" },
      }),
    );
    expect(res.status).toBe(200);
    const json = (await res.json()) as { result: number };
    expect(json.result).toBe(1);
  });
});

describe("QStash routes still work", () => {
  test("health endpoint", async () => {
    const { app } = fresh();
    const res = await app.fetch(new Request("http://downstash/health"));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ok" });
  });

  test("publish endpoint", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      new Request(
        `http://downstash/v2/publish/${encodeURIComponent("http://localhost:3000/x")}`,
        {
          method: "POST",
          headers: { authorization: "Bearer anything" },
          body: "{}",
        },
      ),
    );
    expect(res.status).toBe(200);
    const json = (await res.json()) as { messageId: string };
    expect(json.messageId).toMatch(/^msg_/);
  });
});
