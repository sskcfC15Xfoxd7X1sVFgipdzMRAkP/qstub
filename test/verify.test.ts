import { describe, expect, test } from "bun:test";
import { Receiver } from "@upstash/qstash";
import { openDb } from "../src/db.ts";
import { createLogger } from "../src/logger.ts";
import { newMessageId } from "../src/ids.ts";
import { deliverMessage } from "../src/worker/deliver.ts";

const CURRENT = "sig_test_current";
const NEXT = "sig_test_next";

describe("worker delivery + receiver verification", () => {
  test("destination receives signed request that Receiver.verify accepts", async () => {
    const db = openDb(":memory:");
    const logger = createLogger("error");

    const seen: { url: string; signature: string; body: string; trace: string | null } = {
      url: "",
      signature: "",
      body: "",
      trace: null,
    };

    const fakeFetch = (async (input: Request | string | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      const headers = new Headers(init?.headers);
      const body = init?.body ? new TextDecoder().decode(init.body as Uint8Array) : "";
      seen.url = url;
      seen.signature = headers.get("upstash-signature") ?? "";
      seen.body = body;
      seen.trace = headers.get("x-trace");
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch;

    const id = newMessageId();
    const body = new TextEncoder().encode(JSON.stringify({ hello: "world" }));
    db.insertMessage({
      id,
      destination: "http://localhost:9999/echo",
      method: "POST",
      body,
      forwardHeaders: { "Content-Type": "application/json", "x-trace": "trace-1" },
      retries: 0,
      notBeforeMs: 0,
      timeoutMs: 5_000,
      callbackUrl: null,
      failureCallbackUrl: null,
    });
    const claimed = db.claimDue(10, Date.now() + 1000);
    expect(claimed.length).toBe(1);
    await deliverMessage(claimed[0]!, {
      db,
      logger,
      currentSigningKey: CURRENT,
      fetchImpl: fakeFetch,
    });

    expect(seen.url).toBe("http://localhost:9999/echo");
    expect(seen.body).toBe(JSON.stringify({ hello: "world" }));
    expect(seen.trace).toBe("trace-1");

    const receiver = new Receiver({ currentSigningKey: CURRENT, nextSigningKey: NEXT });
    const ok = await receiver.verify({
      signature: seen.signature,
      body: seen.body,
      url: "http://localhost:9999/echo",
    });
    expect(ok).toBe(true);
    expect(db.getMessage(id)!.status).toBe("delivered");
    db.close();
  });
});
