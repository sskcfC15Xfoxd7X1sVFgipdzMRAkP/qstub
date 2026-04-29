# qstub

A local development server that mocks the [Upstash QStash](https://upstash.com/docs/qstash) API for fast, offline testing.

`qstub` runs on your laptop and speaks the same HTTP API as production QStash, so the official [`@upstash/qstash`](https://www.npmjs.com/package/@upstash/qstash) SDK keeps working with no code changes — point its `baseUrl` at `qstub` and you have a complete publish + signed-callback round-trip without an internet round-trip.

## Why

Working with QStash locally is normally painful because QStash's servers can't reach `localhost`. The standard workaround is to expose your dev server through ngrok, update environment variables, and remember to undo all of that before committing. With `qstub`:

- No tunnels. `qstub` is a process on your machine and can call `http://localhost:3000/...` directly.
- No env shuffling per session — signing keys are stable defaults you put in `.env.local` once.
- Works offline. Plane, train, hotel wifi — fine.
- CI-friendly. Spin it up in a workflow step and run integration tests against a real signed-request pipeline.

## Install

Requires [Bun](https://bun.com/) `>= 1.1.0`.

```bash
git clone https://github.com/sskcfC15Xfoxd7X1sVFgipdzMRAkP/qstub
cd qstub
bun install
bun link            # makes the `qstub` command available globally
```

Or run it without installing globally:

```bash
bun /path/to/qstub/src/cli.ts
```

## Quick start

In one terminal, start qstub:

```bash
qstub
# 2026-04-29T12:00:00.000Z INFO  qstub listening port=8080 db=.qstub/db.sqlite tickMs=250
```

In another terminal, publish a message that round-trips back to your dev server:

```bash
curl -X POST \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"hello":"world"}' \
  'http://localhost:8080/v2/publishJSON/http://localhost:3000/api/echo'
# {"messageId":"msg_2j9a...","url":"http://localhost:3000/api/echo"}
```

`qstub` will sign the request and POST it to `http://localhost:3000/api/echo` within a few hundred milliseconds. Your handler can verify it with `@upstash/qstash`'s `Receiver` exactly as it would in production.

## Configure your app

For any code already using `@upstash/qstash`, add this `.env.local` block:

```env
QSTASH_URL=http://localhost:8080
QSTASH_TOKEN=dev
QSTASH_CURRENT_SIGNING_KEY=sig_qstub_current_dev_key_do_not_use_in_prod
QSTASH_NEXT_SIGNING_KEY=sig_qstub_next_dev_key_do_not_use_in_prod
```

Print the current signing keys at any time with:

```bash
qstub keys
```

The `Client` and `Receiver` constructors then work unchanged:

```ts
import { Client, Receiver } from "@upstash/qstash";

const client = new Client({
  baseUrl: process.env.QSTASH_URL!,
  token: process.env.QSTASH_TOKEN!,
});

await client.publishJSON({
  url: "http://localhost:3000/api/echo",
  body: { hello: "world" },
  delay: 5,        // seconds
  retries: 3,
});

const receiver = new Receiver({
  currentSigningKey: process.env.QSTASH_CURRENT_SIGNING_KEY!,
  nextSigningKey: process.env.QSTASH_NEXT_SIGNING_KEY!,
});

// inside your route handler:
const ok = await receiver.verify({
  signature: req.headers.get("upstash-signature")!,
  body: await req.text(),
  url: req.url,
});
```

## Supported features

| Capability | Status | Notes |
|---|---|---|
| `POST /v2/publish/:dest` | Implemented | Raw body forwarded as-is |
| `POST /v2/publishJSON/:dest` | Implemented | Defaults `Content-Type: application/json` |
| `Upstash-Method` | Implemented | Per-message HTTP verb |
| `Upstash-Delay` / `Upstash-Not-Before` | Implemented | Schedules `not_before` |
| `Upstash-Retries` | Implemented | Exponential backoff capped at 1h |
| `Upstash-Timeout` | Implemented | Per-attempt fetch timeout |
| `Upstash-Forward-*` | Implemented | Prefix stripped on the way out |
| `Upstash-Callback` | Implemented | Success envelope re-enqueued through the same pipeline |
| `Upstash-Failure-Callback` | Implemented | Fired once retries are exhausted |
| Signed `Upstash-Signature` JWT | Implemented | HS256, verified by real `@upstash/qstash` `Receiver` |
| `GET /v2/messages/:id` / `DELETE /v2/messages/:id` | Implemented | Inspect or cancel pending messages |
| Schedules (cron), Queues, DLQ, URL Groups, Batch publish, Events log, Web console | Not yet | Reserved for v2+ |

## Inspecting state

Every accepted publish prints a single info line. Watch them in the qstub terminal, or query the API directly:

```bash
curl http://localhost:8080/v2/messages/msg_2j9a...
# {"messageId":"msg_...","url":"...","method":"POST","state":"delivered",...}
```

Wipe the message store without restarting:

```bash
qstub reset
```

The SQLite database lives at `./.qstub/db.sqlite` by default. Override with `--db` or `QSTUB_DB`.

## CLI reference

```
qstub                            start the server (default port 8080)
qstub serve                      explicit serve subcommand
qstub reset                      truncate the messages table
qstub keys                       print signing keys for .env.local
qstub help                       show this help

flags:
  --port <n>                     HTTP port                          (env: QSTUB_PORT,            default 8080)
  --db <path>                    SQLite db file                     (env: QSTUB_DB,              default .qstub/db.sqlite)
  --tick-ms <n>                  delivery loop interval             (env: QSTUB_TICK_MS,         default 250)
  --current-signing-key <s>      override current key               (env: QSTUB_CURRENT_SIGNING_KEY)
  --next-signing-key <s>         override next key                  (env: QSTUB_NEXT_SIGNING_KEY)
  --log-level <level>            debug | info | warn | error       (env: QSTUB_LOG_LEVEL)
  --quiet                        shorthand for --log-level=warn
```

Flags always win over env vars.

## How signing works

`qstub` signs every outbound delivery with a JWT in the `Upstash-Signature` header. The JWT is HMAC-SHA256 over `<base64url(header)>.<base64url(payload)>` with these claims:

| Claim | Value |
|---|---|
| `iss` | `"Upstash"` |
| `sub` | The destination URL |
| `iat` / `nbf` | Now (unix seconds) |
| `exp` | Now + 5 minutes |
| `jti` | The message ID |
| `body` | `base64url(sha256(rawBody))` |

`@upstash/qstash`'s `Receiver` verifies with both the **current** and **next** signing keys. Override them per-environment with `--current-signing-key` / `--next-signing-key` or the matching `QSTUB_*` env vars; otherwise qstub uses the stable defaults shown by `qstub keys` so your `.env.local` doesn't need to change between machines.

## Roadmap

- Schedules (cron) — `POST /v2/schedules/:dest` and friends
- Queues with parallelism — `POST /v2/queues` + `POST /v2/enqueue/:queue/:dest`
- Dead-letter queue — `GET /v2/dlq`, requeue, delete
- URL Groups (topics) — fan-out
- Batch publish — `POST /v2/batch`
- Events log — `GET /v2/events`
- Web console UI

## Limitations

- Single process per developer. Not for shared/staging use.
- The bearer token is not validated against any registry — any non-empty `Authorization: Bearer <anything>` is accepted.
- `qstub` mirrors QStash's wire shape closely but is not a perfect bug-for-bug clone of production. File issues if your code path depends on a corner that we don't yet match.

## License

MIT
