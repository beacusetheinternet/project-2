type Env = {
  SUPABASE_URL_DB1: string;
  SUPABASE_KEY_DB1: string;
  SUPABASE_URL_DB2: string;
  SUPABASE_KEY_DB2: string;
};

export async function runReconcile(env: Env) {
  const headersDB1 = {
    apikey: env.SUPABASE_KEY_DB1,
    Authorization: `Bearer ${env.SUPABASE_KEY_DB1}`,
  };

  const headersDB2 = {
    apikey: env.SUPABASE_KEY_DB2,
    Authorization: `Bearer ${env.SUPABASE_KEY_DB2}`,
  };

  const db1Res = await fetch(`${env.SUPABASE_URL_DB1}/rest/v1/students`, {
    headers: headersDB1,
  });

  if (!db1Res.ok) throw new Error(await db1Res.text());
  const students = await db1Res.json();

  const db2Res = await fetch(`${env.SUPABASE_URL_DB2}/rest/v1/students_sync`, {
    headers: headersDB2,
  });

  if (!db2Res.ok) throw new Error(await db2Res.text());
  const synced = await db2Res.json();

  const syncedIds = new Set(synced.map((s: any) => s.id));
  let repaired = 0;

  for (const student of students) {
    if (!syncedIds.has(student.id)) {
      await fetch(`${env.SUPABASE_URL_DB2}/rest/v1/students_sync`, {
        method: "POST",
        headers: {
          ...headersDB2,
          "Content-Type": "application/json",
          Prefer: "resolution=merge-duplicates",
        },
        body: JSON.stringify({
          id: student.id,
          data: student,
        }),
      });

      repaired++;
    }
  }

  return { repaired };
}
