import { describe, expect, test } from "bun:test";
import { Client } from "@upstash/qstash";
import { openDb } from "../src/db.ts";
import { createLogger } from "../src/logger.ts";
import { createServer } from "../src/server.ts";

function fresh() {
  const db = openDb(":memory:");
  const logger = createLogger("error");
  const app = createServer({ db, logger });
  return { db, app };
}

function makeFetch(app: ReturnType<typeof createServer>) {
  return ((req: Request | string | URL, init?: RequestInit) => {
    const request = req instanceof Request ? req : new Request(String(req), init);
    return app.fetch(request);
  }) as typeof fetch;
}

describe("batch route", () => {
  test("rejects requests with no Authorization", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      new Request("http://downstash/v2/batch", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "[]",
      }),
    );
    expect(res.status).toBe(401);
  });

  test("rejects non-array body", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      new Request("http://downstash/v2/batch", {
        method: "POST",
        headers: { authorization: "Bearer dev", "content-type": "application/json" },
        body: "{}",
      }),
    );
    expect(res.status).toBe(400);
  });

  test("empty batch returns empty array", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      new Request("http://downstash/v2/batch", {
        method: "POST",
        headers: { authorization: "Bearer dev", "content-type": "application/json" },
        body: "[]",
      }),
    );
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual([]);
  });

  test("@upstash/qstash Client batchJSON inserts one row per item", async () => {
    const { db, app } = fresh();
    const client = new Client({ baseUrl: "http://downstash", token: "dev" });
    const realFetch = global.fetch;
    global.fetch = makeFetch(app);

    try {
      const results = (await client.batchJSON([
        { url: "http://localhost:3000/a", body: { n: 1 } },
        { url: "http://localhost:3000/b", body: { n: 2 }, retries: 0 },
      ])) as { messageId: string }[];

      expect(results).toHaveLength(2);
      expect(results[0].messageId).toMatch(/^msg_/);
      expect(results[1].messageId).toMatch(/^msg_/);

      const rowA = db.getMessage(results[0].messageId)!;
      expect(rowA.destination).toBe("http://localhost:3000/a");
      expect(rowA.status).toBe("pending");
      expect(rowA.forwardHeaders["Content-Type"]).toBe("application/json");
      expect(new TextDecoder().decode(rowA.body)).toBe(JSON.stringify({ n: 1 }));

      const rowB = db.getMessage(results[1].messageId)!;
      expect(rowB.destination).toBe("http://localhost:3000/b");
      expect(rowB.retries).toBe(0);
    } finally {
      global.fetch = realFetch;
      db.close();
    }
  });

  test("rejects item with missing destination", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      new Request("http://downstash/v2/batch", {
        method: "POST",
        headers: { authorization: "Bearer dev", "content-type": "application/json" },
        body: JSON.stringify([{ headers: {}, body: "hi" }]),
      }),
    );
    expect(res.status).toBe(400);
    const json = (await res.json()) as { error: string };
    expect(json.error).toMatch(/item 0/);
  });

  test("upstash-forward-* headers in batch items are stripped and forwarded", async () => {
    const { db, app } = fresh();
    const res = await app.fetch(
      new Request("http://downstash/v2/batch", {
        method: "POST",
        headers: { authorization: "Bearer dev", "content-type": "application/json" },
        body: JSON.stringify([
          {
            destination: "http://localhost:3000/x",
            headers: { "upstash-forward-x-trace": "abc-123" },
            body: "raw",
          },
        ]),
      }),
    );
    expect(res.status).toBe(200);
    const [result] = (await res.json()) as { messageId: string }[];
    const row = db.getMessage(result.messageId)!;
    expect(row.forwardHeaders["x-trace"]).toBe("abc-123");
    db.close();
  });
});
