export function transformStudent(record: any) {
  return {
    id: record.id,
    data: {
      full_name: `${record.fname} ${record.lname}`,
      email: record.email,
      updated_at: record.updated_at,
    },
  };
}
