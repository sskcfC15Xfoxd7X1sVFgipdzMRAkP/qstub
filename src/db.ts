import { Database } from "bun:sqlite";
import { dirname } from "node:path";
import { mkdirSync } from "node:fs";

export type MessageStatus = "pending" | "in_flight" | "delivered" | "failed" | "cancelled";

export interface MessageRow {
  id: string;
  destination: string;
  method: string;
  body: Uint8Array;
  forwardHeaders: Record<string, string>;
  retries: number;
  attempt: number;
  notBeforeMs: number;
  timeoutMs: number;
  callbackUrl: string | null;
  failureCallbackUrl: string | null;
  status: MessageStatus;
  lastError: string | null;
  createdMs: number;
  updatedMs: number;
}

export interface InsertMessage {
  id: string;
  destination: string;
  method: string;
  body: Uint8Array;
  forwardHeaders: Record<string, string>;
  retries: number;
  notBeforeMs: number;
  timeoutMs: number;
  callbackUrl: string | null;
  failureCallbackUrl: string | null;
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS messages (
  id                     TEXT PRIMARY KEY,
  destination            TEXT NOT NULL,
  method                 TEXT NOT NULL,
  body                   BLOB NOT NULL,
  forward_headers_json   TEXT NOT NULL,
  retries                INTEGER NOT NULL,
  attempt                INTEGER NOT NULL DEFAULT 0,
  not_before_ms          INTEGER NOT NULL,
  timeout_ms             INTEGER NOT NULL,
  callback_url           TEXT,
  failure_callback_url   TEXT,
  status                 TEXT NOT NULL,
  last_error             TEXT,
  created_ms             INTEGER NOT NULL,
  updated_ms             INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS messages_pending_idx
  ON messages(status, not_before_ms);
`;

export interface Db {
  insertMessage: (msg: InsertMessage) => void;
  getMessage: (id: string) => MessageRow | null;
  cancelMessage: (id: string) => boolean;
  claimDue: (limit: number, now: number) => MessageRow[];
  markDelivered: (id: string, now: number) => void;
  markFailed: (id: string, error: string, now: number) => void;
  rescheduleRetry: (id: string, attempt: number, notBeforeMs: number, error: string, now: number) => void;
  reset: () => void;
  close: () => void;
}

export function openDb(path: string): Db {
  const isMemory = path === ":memory:";
  if (!isMemory) {
    mkdirSync(dirname(path), { recursive: true });
  }
  const db = new Database(path);
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA foreign_keys = ON;");
  db.exec("PRAGMA busy_timeout = 5000;");
  db.exec(SCHEMA);

  const insertStmt = db.prepare<
    void,
    {
      $id: string;
      $destination: string;
      $method: string;
      $body: Uint8Array;
      $forward_headers_json: string;
      $retries: number;
      $not_before_ms: number;
      $timeout_ms: number;
      $callback_url: string | null;
      $failure_callback_url: string | null;
      $status: MessageStatus;
      $created_ms: number;
      $updated_ms: number;
    }
  >(`
    INSERT INTO messages (
      id, destination, method, body, forward_headers_json,
      retries, attempt, not_before_ms, timeout_ms,
      callback_url, failure_callback_url, status,
      created_ms, updated_ms
    ) VALUES (
      $id, $destination, $method, $body, $forward_headers_json,
      $retries, 0, $not_before_ms, $timeout_ms,
      $callback_url, $failure_callback_url, $status,
      $created_ms, $updated_ms
    )
  `);

  const getStmt = db.prepare<RawRow, { $id: string }>(`SELECT * FROM messages WHERE id = $id`);

  const cancelStmt = db.prepare<void, { $id: string; $now: number }>(`
    UPDATE messages SET status = 'cancelled', updated_ms = $now
    WHERE id = $id AND status = 'pending'
  `);

  const claimSelectStmt = db.prepare<RawRow, { $now: number; $limit: number }>(`
    SELECT * FROM messages
    WHERE status = 'pending' AND not_before_ms <= $now
    ORDER BY not_before_ms
    LIMIT $limit
  `);

  const claimUpdateStmt = db.prepare<void, { $id: string; $now: number }>(`
    UPDATE messages SET status = 'in_flight', updated_ms = $now WHERE id = $id
  `);

  const markDeliveredStmt = db.prepare<void, { $id: string; $now: number }>(`
    UPDATE messages SET status = 'delivered', updated_ms = $now, last_error = NULL WHERE id = $id
  `);

  const markFailedStmt = db.prepare<void, { $id: string; $err: string; $now: number }>(`
    UPDATE messages SET status = 'failed', last_error = $err, updated_ms = $now WHERE id = $id
  `);

  const rescheduleStmt = db.prepare<
    void,
    { $id: string; $attempt: number; $not_before_ms: number; $err: string; $now: number }
  >(`
    UPDATE messages
    SET status = 'pending',
        attempt = $attempt,
        not_before_ms = $not_before_ms,
        last_error = $err,
        updated_ms = $now
    WHERE id = $id
  `);

  const resetStmt = db.prepare(`DELETE FROM messages`);

  function rowToMessage(row: RawRow): MessageRow {
    return {
      id: row.id,
      destination: row.destination,
      method: row.method,
      body: toUint8(row.body),
      forwardHeaders: JSON.parse(row.forward_headers_json) as Record<string, string>,
      retries: row.retries,
      attempt: row.attempt,
      notBeforeMs: row.not_before_ms,
      timeoutMs: row.timeout_ms,
      callbackUrl: row.callback_url,
      failureCallbackUrl: row.failure_callback_url,
      status: row.status,
      lastError: row.last_error,
      createdMs: row.created_ms,
      updatedMs: row.updated_ms,
    };
  }

  return {
    insertMessage(msg) {
      const now = Date.now();
      insertStmt.run({
        $id: msg.id,
        $destination: msg.destination,
        $method: msg.method,
        $body: msg.body,
        $forward_headers_json: JSON.stringify(msg.forwardHeaders),
        $retries: msg.retries,
        $not_before_ms: msg.notBeforeMs,
        $timeout_ms: msg.timeoutMs,
        $callback_url: msg.callbackUrl,
        $failure_callback_url: msg.failureCallbackUrl,
        $status: "pending",
        $created_ms: now,
        $updated_ms: now,
      });
    },
    getMessage(id) {
      const row = getStmt.get({ $id: id });
      return row ? rowToMessage(row) : null;
    },
    cancelMessage(id) {
      const result = cancelStmt.run({ $id: id, $now: Date.now() });
      return result.changes > 0;
    },
    claimDue(limit, now) {
      const claimTx = db.transaction((): MessageRow[] => {
        const rows = claimSelectStmt.all({ $now: now, $limit: limit });
        const claimed: MessageRow[] = [];
        for (const row of rows) {
          claimUpdateStmt.run({ $id: row.id, $now: now });
          claimed.push(rowToMessage({ ...row, status: "in_flight", updated_ms: now }));
        }
        return claimed;
      });
      return claimTx.immediate();
    },
    markDelivered(id, now) {
      markDeliveredStmt.run({ $id: id, $now: now });
    },
    markFailed(id, error, now) {
      markFailedStmt.run({ $id: id, $err: error, $now: now });
    },
    rescheduleRetry(id, attempt, notBeforeMs, error, now) {
      rescheduleStmt.run({
        $id: id,
        $attempt: attempt,
        $not_before_ms: notBeforeMs,
        $err: error,
        $now: now,
      });
    },
    reset() {
      resetStmt.run();
    },
    close() {
      db.close();
    },
  };
}

interface RawRow {
  id: string;
  destination: string;
  method: string;
  body: Uint8Array | Buffer;
  forward_headers_json: string;
  retries: number;
  attempt: number;
  not_before_ms: number;
  timeout_ms: number;
  callback_url: string | null;
  failure_callback_url: string | null;
  status: MessageStatus;
  last_error: string | null;
  created_ms: number;
  updated_ms: number;
}

function toUint8(value: Uint8Array | Buffer): Uint8Array {
  if (value instanceof Uint8Array) return value;
  return new Uint8Array(value);
}
