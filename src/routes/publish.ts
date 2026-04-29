import { Hono } from "hono";
import type { Context } from "hono";
import type { Db } from "../db.ts";
import { parseDurationMs } from "../duration.ts";
import { newMessageId } from "../ids.ts";
import type { Logger } from "../logger.ts";

export interface PublishDeps {
  db: Db;
  logger: Logger;
}

const FORWARD_PREFIX = "upstash-forward-";

export function publishRoute({ db, logger }: PublishDeps): Hono {
  const app = new Hono();

  app.post("/v2/publish/:destination{.+}", (c) =>
    handlePublish(c, { db, logger, forceJson: false }),
  );
  app.post("/v2/publishJSON/:destination{.+}", (c) =>
    handlePublish(c, { db, logger, forceJson: true }),
  );

  return app;
}

interface HandleArgs extends PublishDeps {
  forceJson: boolean;
}

async function handlePublish(c: Context, args: HandleArgs): Promise<Response> {
  const { db, logger, forceJson } = args;

  const auth = c.req.header("authorization") ?? "";
  if (!/^Bearer\s+\S+/i.test(auth)) {
    return c.json({ error: "missing or empty Authorization bearer token" }, 401);
  }

  const rawDest = c.req.param("destination");
  if (!rawDest) {
    return c.json({ error: "destination is required" }, 400);
  }
  const destination = decodeURIComponent(rawDest);
  let parsedUrl: URL;
  try {
    parsedUrl = new URL(destination);
  } catch {
    return c.json({ error: `invalid destination URL: ${destination}` }, 400);
  }
  if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
    return c.json({ error: `unsupported destination protocol: ${parsedUrl.protocol}` }, 400);
  }

  const body = new Uint8Array(await c.req.arrayBuffer());

  const method = (c.req.header("upstash-method") ?? "POST").toUpperCase();
  const retries = parseIntHeader(c.req.header("upstash-retries"), 3, "Upstash-Retries");
  if (retries instanceof Error) return c.json({ error: retries.message }, 400);

  const timeoutMs = parseDurationHeader(c.req.header("upstash-timeout"), 30_000, "Upstash-Timeout");
  if (timeoutMs instanceof Error) return c.json({ error: timeoutMs.message }, 400);

  const notBeforeMs = computeNotBefore(
    c.req.header("upstash-delay"),
    c.req.header("upstash-not-before"),
  );
  if (notBeforeMs instanceof Error) return c.json({ error: notBeforeMs.message }, 400);

  const forwardHeaders = collectForwardHeaders(c.req.raw.headers, forceJson);

  const id = newMessageId();
  db.insertMessage({
    id,
    destination,
    method,
    body,
    forwardHeaders,
    retries,
    notBeforeMs,
    timeoutMs,
    callbackUrl: c.req.header("upstash-callback") ?? null,
    failureCallbackUrl: c.req.header("upstash-failure-callback") ?? null,
  });

  logger.info("publish accepted", {
    messageId: id,
    destination,
    method,
    notBeforeMs,
    bodyBytes: body.byteLength,
  });

  return c.json({ messageId: id, url: destination });
}

function parseIntHeader(
  value: string | undefined,
  fallback: number,
  name: string,
): number | Error {
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return new Error(`invalid ${name}: ${value}`);
  }
  return parsed;
}

function parseDurationHeader(
  value: string | undefined,
  fallback: number,
  name: string,
): number | Error {
  if (value === undefined) return fallback;
  try {
    return parseDurationMs(value);
  } catch {
    return new Error(`invalid ${name}: ${value}`);
  }
}

function computeNotBefore(
  delay: string | undefined,
  notBefore: string | undefined,
): number | Error {
  const now = Date.now();
  if (notBefore !== undefined) {
    const seconds = Number.parseInt(notBefore, 10);
    if (!Number.isFinite(seconds) || seconds < 0) {
      return new Error(`invalid Upstash-Not-Before: ${notBefore}`);
    }
    return seconds * 1000;
  }
  if (delay !== undefined) {
    try {
      return now + parseDurationMs(delay);
    } catch {
      return new Error(`invalid Upstash-Delay: ${delay}`);
    }
  }
  return now;
}

function collectForwardHeaders(headers: Headers, forceJson: boolean): Record<string, string> {
  const out: Record<string, string> = {};

  // Pass-through Content-Type from the publish request so destinations get
  // the same encoding the publisher sent. Upstash-Forward-Content-Type wins.
  const inboundContentType = headers.get("content-type");
  if (inboundContentType) {
    out["Content-Type"] = inboundContentType;
  }

  headers.forEach((value, key) => {
    const lower = key.toLowerCase();
    if (lower.startsWith(FORWARD_PREFIX)) {
      const forwarded = key.slice(FORWARD_PREFIX.length);
      out[forwarded] = value;
    }
  });

  if (forceJson) {
    const hasContentType = Object.keys(out).some((k) => k.toLowerCase() === "content-type");
    if (!hasContentType) {
      out["Content-Type"] = "application/json";
    }
  }
  return out;
}
