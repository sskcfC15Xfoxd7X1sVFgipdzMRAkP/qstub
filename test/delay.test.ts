import { describe, expect, test } from "bun:test";
import { openDb } from "../src/db.ts";
import { newMessageId } from "../src/ids.ts";
import { createLogger } from "../src/logger.ts";
import { createWorker } from "../src/worker/loop.ts";

describe("delay", () => {
  test("delayed messages are not delivered before notBefore", async () => {
    const db = openDb(":memory:");
    const logger = createLogger("error");
    let delivered = 0;
    const fakeFetch = (async () => {
      delivered += 1;
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch;

    const worker = createWorker({
      db,
      logger,
      currentSigningKey: "k",
      tickMs: 10,
      fetchImpl: fakeFetch,
    });

    const id = newMessageId();
    const notBeforeMs = Date.now() + 200;
    db.insertMessage({
      id,
      destination: "http://localhost/x",
      method: "POST",
      body: new Uint8Array(),
      forwardHeaders: {},
      retries: 0,
      notBeforeMs,
      timeoutMs: 1_000,
      callbackUrl: null,
      failureCallbackUrl: null,
    });

    await worker.tick();
    expect(delivered).toBe(0);
    expect(db.getMessage(id)!.status).toBe("pending");

    while (Date.now() < notBeforeMs + 20) {
      await new Promise((r) => setTimeout(r, 10));
    }
    await worker.tick();
    // Wait for in-flight delivery to settle.
    await worker.stop();

    expect(delivered).toBe(1);
    expect(db.getMessage(id)!.status).toBe("delivered");
    db.close();
  });
});
