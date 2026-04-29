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

describe("publish route", () => {
  test("rejects requests with no Authorization", async () => {
    const { app } = fresh();
    const res = await app.fetch(
      new Request(`http://qstub/v2/publish/${encodeURIComponent("http://localhost:3000/x")}`, {
        method: "POST",
        body: "{}",
      }),
    );
    expect(res.status).toBe(401);
  });

  test("@upstash/qstash Client publishJSON results in a pending row", async () => {
    const { db, app } = fresh();
    const client = new Client({
      baseUrl: "http://qstub",
      token: "dev",
    });

    // Wire the Client's fetch through our in-process Hono app.
    const realFetch = global.fetch;
    global.fetch = ((req: Request | string | URL, init?: RequestInit) => {
      const request = req instanceof Request ? req : new Request(String(req), init);
      return app.fetch(request);
    }) as typeof fetch;

    try {
      const result = (await client.publishJSON({
        url: "http://localhost:3000/echo",
        body: { hello: "world" },
        retries: 5,
        delay: 0,
      })) as { messageId: string };

      expect(result.messageId).toMatch(/^msg_/);
      const row = db.getMessage(result.messageId);
      expect(row).not.toBeNull();
      expect(row!.destination).toBe("http://localhost:3000/echo");
      expect(row!.method).toBe("POST");
      expect(row!.retries).toBe(5);
      expect(row!.status).toBe("pending");
      expect(row!.forwardHeaders["Content-Type"]).toBe("application/json");
      expect(new TextDecoder().decode(row!.body)).toBe(JSON.stringify({ hello: "world" }));
    } finally {
      global.fetch = realFetch;
      db.close();
    }
  });

  test("Upstash-Forward-* headers are stripped of their prefix and forwarded", async () => {
    const { db, app } = fresh();
    const res = await app.fetch(
      new Request(`http://qstub/v2/publish/${encodeURIComponent("http://localhost:3000/x")}`, {
        method: "POST",
        body: "raw",
        headers: {
          authorization: "Bearer dev",
          "upstash-forward-x-trace": "abc-123",
          "upstash-forward-content-type": "text/plain",
        },
      }),
    );
    expect(res.status).toBe(200);
    const json = (await res.json()) as { messageId: string };
    const row = db.getMessage(json.messageId)!;
    expect(row.forwardHeaders["x-trace"]).toBe("abc-123");
    expect(row.forwardHeaders["content-type"]).toBe("text/plain");
    db.close();
  });

  test("Upstash-Delay schedules notBefore in the future", async () => {
    const { db, app } = fresh();
    const before = Date.now();
    const res = await app.fetch(
      new Request(`http://qstub/v2/publish/${encodeURIComponent("http://localhost:3000/x")}`, {
        method: "POST",
        body: "{}",
        headers: {
          authorization: "Bearer dev",
          "upstash-delay": "5s",
        },
      }),
    );
    expect(res.status).toBe(200);
    const json = (await res.json()) as { messageId: string };
    const row = db.getMessage(json.messageId)!;
    expect(row.notBeforeMs).toBeGreaterThanOrEqual(before + 5_000);
    db.close();
  });
});
