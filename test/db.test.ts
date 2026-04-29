import { describe, expect, test } from "bun:test";
import { openDb } from "../src/db.ts";

function freshDb() {
  return openDb(":memory:");
}

describe("db", () => {
  test("insert + getMessage round-trips fields including raw bytes", () => {
    const db = freshDb();
    const body = new Uint8Array([0, 1, 2, 255, 254]);
    db.insertMessage({
      id: "msg_1",
      destination: "http://localhost:3000/x",
      method: "POST",
      body,
      forwardHeaders: { "x-trace": "abc" },
      retries: 3,
      notBeforeMs: 1000,
      timeoutMs: 30_000,
      callbackUrl: null,
      failureCallbackUrl: null,
    });
    const got = db.getMessage("msg_1");
    expect(got).not.toBeNull();
    expect(got!.body).toEqual(body);
    expect(got!.forwardHeaders).toEqual({ "x-trace": "abc" });
    expect(got!.status).toBe("pending");
    expect(got!.attempt).toBe(0);
    db.close();
  });

  test("claimDue claims pending due rows and flips them to in_flight", () => {
    const db = freshDb();
    db.insertMessage({
      id: "msg_a",
      destination: "http://localhost:3000/a",
      method: "POST",
      body: new Uint8Array(),
      forwardHeaders: {},
      retries: 0,
      notBeforeMs: 0,
      timeoutMs: 30_000,
      callbackUrl: null,
      failureCallbackUrl: null,
    });
    db.insertMessage({
      id: "msg_b",
      destination: "http://localhost:3000/b",
      method: "POST",
      body: new Uint8Array(),
      forwardHeaders: {},
      retries: 0,
      notBeforeMs: 9_999_999_999_999,
      timeoutMs: 30_000,
      callbackUrl: null,
      failureCallbackUrl: null,
    });

    const claimed = db.claimDue(10, 1000);
    expect(claimed.length).toBe(1);
    expect(claimed[0]!.id).toBe("msg_a");
    expect(claimed[0]!.status).toBe("in_flight");

    const second = db.claimDue(10, 1000);
    expect(second.length).toBe(0);
    db.close();
  });

  test("rescheduleRetry returns a row to pending and bumps attempt", () => {
    const db = freshDb();
    db.insertMessage({
      id: "msg_r",
      destination: "http://localhost:3000/r",
      method: "POST",
      body: new Uint8Array(),
      forwardHeaders: {},
      retries: 3,
      notBeforeMs: 0,
      timeoutMs: 30_000,
      callbackUrl: null,
      failureCallbackUrl: null,
    });
    db.claimDue(1, 1000);
    db.rescheduleRetry("msg_r", 1, 5000, "boom", 1000);
    const row = db.getMessage("msg_r");
    expect(row!.status).toBe("pending");
    expect(row!.attempt).toBe(1);
    expect(row!.notBeforeMs).toBe(5000);
    expect(row!.lastError).toBe("boom");
    db.close();
  });

  test("cancelMessage only cancels pending rows", () => {
    const db = freshDb();
    db.insertMessage({
      id: "msg_c",
      destination: "http://localhost:3000/c",
      method: "POST",
      body: new Uint8Array(),
      forwardHeaders: {},
      retries: 0,
      notBeforeMs: 9_999_999_999_999,
      timeoutMs: 30_000,
      callbackUrl: null,
      failureCallbackUrl: null,
    });
    expect(db.cancelMessage("msg_c")).toBe(true);
    expect(db.cancelMessage("msg_c")).toBe(false);
    expect(db.getMessage("msg_c")!.status).toBe("cancelled");
    db.close();
  });
});
