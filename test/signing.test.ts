import { describe, expect, test } from "bun:test";
import { Receiver } from "@upstash/qstash";
import { signRequest } from "../src/signing.ts";

describe("signing", () => {
  test("@upstash/qstash Receiver verifies our JWT", async () => {
    const currentSigningKey = "sig_test_current";
    const nextSigningKey = "sig_test_next";
    const destination = "http://localhost:3000/api/echo";
    const body = new TextEncoder().encode(JSON.stringify({ hello: "world" }));

    const jwt = await signRequest({
      destination,
      messageId: "msg_abc",
      body,
      signingKey: currentSigningKey,
    });

    const receiver = new Receiver({ currentSigningKey, nextSigningKey });
    const ok = await receiver.verify({
      signature: jwt,
      body: new TextDecoder().decode(body),
      url: destination,
    });
    expect(ok).toBe(true);
  });

  test("Receiver also verifies when we sign with the next key", async () => {
    const currentSigningKey = "sig_test_current";
    const nextSigningKey = "sig_test_next";
    const destination = "http://localhost:3000/api/echo";
    const body = new TextEncoder().encode("");

    const jwt = await signRequest({
      destination,
      messageId: "msg_xyz",
      body,
      signingKey: nextSigningKey,
    });

    const receiver = new Receiver({ currentSigningKey, nextSigningKey });
    const ok = await receiver.verify({
      signature: jwt,
      body: "",
      url: destination,
    });
    expect(ok).toBe(true);
  });
});
