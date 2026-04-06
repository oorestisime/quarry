export function escapeSingleQuotedString(value: string): string {
  return value.replaceAll("'", "''");
}
