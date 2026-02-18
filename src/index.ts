import { Hono } from "hono";
import { runBackfill } from "./jobs/backfill";
import { runReconcile } from "./jobs/reconcile";
import { validateStudent } from "./services/validator";
import { logSync } from "./services/logger";

type Env = {
  SUPABASE_URL_DB1: string;
  SUPABASE_KEY_DB1: string;
  SUPABASE_URL_DB2: string;
  SUPABASE_KEY_DB2: string;
  cl_queue: Queue; 
};

const app = new Hono<{ Bindings: Env }>();


app.get("/", (c) => {
  return c.text("DB Sync Worker Running ");
});


app.post("/sync", async (c) => {
  const payload = await c.req.json();
  await c.env.cl_queue.send(payload);
  return c.json({ queued: true });
});


app.post("/backfill", async (c) => {
  const result = await runBackfill(c.env);
  return c.json(result);
});


app.post("/reconcile", async (c) => {
  const result = await runReconcile(c.env);
  return c.json(result);
});

export default {
  fetch: app.fetch, 

  async queue(batch: MessageBatch<any>, env: Env) {
    for (const message of batch.messages) {
      const start = Date.now();
      const { type, record, old_record } = message.body;

      const studentId = record?.id || old_record?.id;

      const headersDB2 = {
        apikey: env.SUPABASE_KEY_DB2,
        Authorization: `Bearer ${env.SUPABASE_KEY_DB2}`,
      };

      try {
        
        if (type === "DELETE") {
          await fetch(
            `${env.SUPABASE_URL_DB2}/rest/v1/students_sync?id=eq.${old_record.id}`,
            {
              method: "DELETE",
              headers: headersDB2,
            },
          );

          await logSync(
            env,
            studentId,
            "DELETE",
            "SUCCESS",
            null,
            Date.now() - start,
          );

          message.ack();
          continue;
        }

        
        const validationError = validateStudent(record);

        if (validationError) {
          await logSync(env, studentId, type, "INVALID", validationError, null);

          message.ack();
          continue;
        }

        // INSERT / UPDATE
        const response = await fetch(
          `${env.SUPABASE_URL_DB2}/rest/v1/students_sync`,
          {
            method: "POST",
            headers: {
              ...headersDB2,
              "Content-Type": "application/json",
              Prefer: "resolution=merge-duplicates",
            },
            body: JSON.stringify({
              id: record.id,
              data: record,
            }),
          },
        );

        if (!response.ok) {
          throw new Error(await response.text());
        }

        await logSync(
          env,
          studentId,
          type,
          "SUCCESS",
          null,
          Date.now() - start,
        );

        message.ack();
      } catch (error: any) {
        await logSync(env, studentId, type, "FAILED", error.message, null);

        // backoff retry
        if (message.attempts < 5) {
          const delay = Math.min(60, Math.pow(2, message.attempts));
          message.retry({ delaySeconds: delay });
        } else {
          message.ack(); 
      }
    }
  },
};
