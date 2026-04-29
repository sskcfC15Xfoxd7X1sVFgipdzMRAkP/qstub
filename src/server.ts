import { Hono } from "hono";
import type { Db } from "./db.ts";
import type { Logger } from "./logger.ts";
import { healthRoute } from "./routes/health.ts";
import { messagesRoute } from "./routes/messages.ts";
import { publishRoute } from "./routes/publish.ts";

export interface ServerDeps {
  db: Db;
  logger: Logger;
}

export function createServer({ db, logger }: ServerDeps): Hono {
  const app = new Hono();
  app.onError((err, c) => {
    logger.error("unhandled server error", { error: String(err) });
    return c.json({ error: "internal_error" }, 500);
  });

  app.route("/", healthRoute());
  app.route("/", publishRoute({ db, logger }));
  app.route("/", messagesRoute({ db }));

  return app;
}
