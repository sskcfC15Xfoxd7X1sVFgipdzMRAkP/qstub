import { Hono } from "hono";

export function healthRoute(): Hono {
  const app = new Hono();
  app.get("/health", (c) => c.json({ status: "ok" }));
  return app;
}
