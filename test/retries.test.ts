import { describe, expect, test } from "bun:test";
import { openDb } from "../src/db.ts";
import { newMessageId } from "../src/ids.ts";
import { createLogger } from "../src/logger.ts";
import { backoffMs } from "../src/worker/backoff.ts";
import { deliverMessage } from "../src/worker/deliver.ts";

describe("retries", () => {
  test("backoffMs grows exponentially and caps at one hour", () => {
    expect(backoffMs(1)).toBe(1_000);
    expect(backoffMs(2)).toBe(2_000);
    expect(backoffMs(3)).toBe(4_000);
    expect(backoffMs(4)).toBe(8_000);
    expect(backoffMs(20)).toBe(60 * 60 * 1000);
  });

  test("non-2xx schedules a retry with the right not_before until retries exhaust", async () => {
    const db = openDb(":memory:");
    const logger = createLogger("error");
    let calls = 0;
    const fakeFetch = (async () => {
      calls += 1;
      return new Response("nope", { status: 500 });
    }) as unknown as typeof fetch;

    const id = newMessageId();
    db.insertMessage({
      id,
      destination: "http://localhost/r",
      method: "POST",
      body: new Uint8Array(),
      forwardHeaders: {},
      retries: 2,
      notBeforeMs: 0,
      timeoutMs: 1_000,
      callbackUrl: null,
      failureCallbackUrl: null,
    });

    // Attempt 0
    let claimed = db.claimDue(10, Date.now())[0]!;
    await deliverMessage(claimed, { db, logger, currentSigningKey: "k", fetchImpl: fakeFetch });
    let row = db.getMessage(id)!;
    expect(row.status).toBe("pending");
    expect(row.attempt).toBe(1);
    expect(row.notBeforeMs).toBeGreaterThan(Date.now() - 100);

    // Attempt 1 (force not_before to now)
    db.rescheduleRetry(id, 1, 0, "force", Date.now());
    claimed = db.claimDue(10, Date.now())[0]!;
    await deliverMessage(claimed, { db, logger, currentSigningKey: "k", fetchImpl: fakeFetch });
    row = db.getMessage(id)!;
    expect(row.status).toBe("pending");
    expect(row.attempt).toBe(2);

    // Attempt 2 — exhausts retries (retries=2 → max attempt index 2 → next=3 > retries → fail)
    db.rescheduleRetry(id, 2, 0, "force", Date.now());
    claimed = db.claimDue(10, Date.now())[0]!;
    await deliverMessage(claimed, { db, logger, currentSigningKey: "k", fetchImpl: fakeFetch });
    row = db.getMessage(id)!;
    expect(row.status).toBe("failed");
    expect(calls).toBe(3);
    db.close();
  });

  test("eventual success after a transient failure marks delivered", async () => {
    const db = openDb(":memory:");
    const logger = createLogger("error");
    let calls = 0;
    const fakeFetch = (async () => {
      calls += 1;
      if (calls === 1) return new Response("err", { status: 500 });
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch;

    const id = newMessageId();
    db.insertMessage({
      id,
      destination: "http://localhost/r",
      method: "POST",
      body: new Uint8Array(),
      forwardHeaders: {},
      retries: 3,
      notBeforeMs: 0,
      timeoutMs: 1_000,
      callbackUrl: null,
      failureCallbackUrl: null,
    });
    let claimed = db.claimDue(10, Date.now())[0]!;
    await deliverMessage(claimed, { db, logger, currentSigningKey: "k", fetchImpl: fakeFetch });
    expect(db.getMessage(id)!.status).toBe("pending");

    db.rescheduleRetry(id, 1, 0, "force", Date.now());
    claimed = db.claimDue(10, Date.now())[0]!;
    await deliverMessage(claimed, { db, logger, currentSigningKey: "k", fetchImpl: fakeFetch });
    expect(db.getMessage(id)!.status).toBe("delivered");
    db.close();
  });
});
