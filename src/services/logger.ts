type Env = {
  SUPABASE_URL_DB2: string;
  SUPABASE_KEY_DB2: string;
};

export async function logSync(
  env: Env,
  recordId: string | null,
  operation: string,
  status: string,
  error: string | null,
  latency: number | null,
) {
  await fetch(`${env.SUPABASE_URL_DB2}/rest/v1/sync_logs`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_KEY_DB2,
      Authorization: `Bearer ${env.SUPABASE_KEY_DB2}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      record_id: recordId,
      operation_type: operation,
      status,
      error_message: error,
      latency_ms: latency,
    }),
  });
}
