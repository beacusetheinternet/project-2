export function validateStudent(record: any) {
  if (!record?.id) return "Missing ID";
  if (!record?.email || !record.email.includes("@")) return "Invalid email";
  return null;
}
