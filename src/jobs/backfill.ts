type Env = {
  SUPABASE_URL_DB1: string;
  SUPABASE_KEY_DB1: string;
  SUPABASE_URL_DB2: string;
  SUPABASE_KEY_DB2: string;
};

export async function runBackfill(env: Env) {
  const headersDB1 = {
    apikey: env.SUPABASE_KEY_DB1,
    Authorization: `Bearer ${env.SUPABASE_KEY_DB1}`,
  };

  const headersDB2 = {
    apikey: env.SUPABASE_KEY_DB2,
    Authorization: `Bearer ${env.SUPABASE_KEY_DB2}`,
  };

  const res = await fetch(`${env.SUPABASE_URL_DB1}/rest/v1/students`, {
    headers: headersDB1,
  });

  if (!res.ok) throw new Error(await res.text());

  const students = await res.json();
  if (!Array.isArray(students)) throw new Error("Invalid DB1 response");

  let count = 0;

  for (const student of students) {
    const insertRes = await fetch(
      `${env.SUPABASE_URL_DB2}/rest/v1/students_sync`,
      {
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
      },
    );

    if (!insertRes.ok) throw new Error(await insertRes.text());
    count++;
  }

  return { backfilled: count };
}
