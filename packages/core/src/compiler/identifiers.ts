// ClickHouse accepts ordinary identifiers without quoting. Quote syntax-bearing
// names and SQL clause words so generated schemas can use them without raw SQL.
const keywords = new Set(
  "select from where prewhere group order by having limit offset join left right inner full cross on as with union all distinct final settings insert into values format null true false and or not in between case when then else end table database window over partition rows range current row".split(
    " ",
  ),
);

export function quoteIdentifier(name: string): string {
  if (!name || name.includes("\0")) {
    throw new Error("SQL identifiers must be non-empty and cannot contain NUL.");
  }
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) && !keywords.has(name.toLowerCase())) {
    return name;
  }
  return `\`${name.replaceAll("\\", "\\\\").replaceAll("`", "\\`")}\``;
}

export function quoteTable(name: string): string {
  return name.split(".").map(quoteIdentifier).join(".");
}
