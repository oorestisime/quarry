export function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

export function stripIdentifierQuotes(value: string): string {
  const trimmed = value.trim();
  if (
    (trimmed.startsWith("`") && trimmed.endsWith("`")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    return trimmed.slice(1, -1);
  }

  return trimmed;
}

export function unquoteIdentifier(value: string): string {
  const trimmed = value.trim();
  const segments = trimmed.split(".");
  return stripIdentifierQuotes(segments[segments.length - 1]!);
}

export function isIdentifierLike(value: string): boolean {
  return /^(?:`?[A-Za-z_][A-Za-z0-9_]*`?\.)*`?[A-Za-z_][A-Za-z0-9_]*`?$/.test(value.trim());
}

export function splitTopLevel(value: string, delimiter: string): string[] {
  const parts: string[] = [];
  let current = "";
  let depth = 0;
  let quote: "'" | '"' | "`" | null = null;

  for (let index = 0; index < value.length; index += 1) {
    const character = value[index]!;

    if (quote) {
      current += character;
      if (character === quote && value[index - 1] !== "\\") {
        quote = null;
      }
      continue;
    }

    if (character === "'" || character === '"' || character === "`") {
      quote = character;
      current += character;
      continue;
    }

    if (character === "(") {
      depth += 1;
      current += character;
      continue;
    }

    if (character === ")") {
      depth -= 1;
      current += character;
      continue;
    }

    if (character === delimiter && depth === 0) {
      parts.push(current.trim());
      current = "";
      continue;
    }

    current += character;
  }

  if (current.trim().length > 0) {
    parts.push(current.trim());
  }

  return parts;
}

export function splitStatements(sql: string): string[] {
  const statements: string[] = [];
  let current = "";
  let depth = 0;
  let quote: "'" | '"' | "`" | null = null;

  for (let index = 0; index < sql.length; index += 1) {
    const character = sql[index]!;

    if (quote) {
      current += character;
      if (character === quote && sql[index - 1] !== "\\") {
        quote = null;
      }
      continue;
    }

    if (character === "'" || character === '"' || character === "`") {
      quote = character;
      current += character;
      continue;
    }

    if (character === "(") {
      depth += 1;
      current += character;
      continue;
    }

    if (character === ")") {
      depth -= 1;
      current += character;
      continue;
    }

    if (character === ";" && depth === 0) {
      if (current.trim().length > 0) {
        statements.push(current.trim());
      }
      current = "";
      continue;
    }

    current += character;
  }

  if (current.trim().length > 0) {
    statements.push(current.trim());
  }

  return statements;
}

export function findTopLevelKeyword(value: string, keyword: string): number {
  const upperValue = value.toUpperCase();
  const upperKeyword = keyword.toUpperCase();
  let depth = 0;
  let quote: "'" | '"' | "`" | null = null;

  for (let index = 0; index <= value.length - upperKeyword.length; index += 1) {
    const character = value[index]!;

    if (quote) {
      if (character === quote && value[index - 1] !== "\\") {
        quote = null;
      }
      continue;
    }

    if (character === "'" || character === '"' || character === "`") {
      quote = character;
      continue;
    }

    if (character === "(") {
      depth += 1;
      continue;
    }

    if (character === ")") {
      depth -= 1;
      continue;
    }

    if (depth !== 0) {
      continue;
    }

    if (upperValue.slice(index, index + upperKeyword.length) !== upperKeyword) {
      continue;
    }

    const before = upperValue[index - 1];
    const after = upperValue[index + upperKeyword.length];
    const validBefore = before === undefined || /\s/.test(before);
    const validAfter = after === undefined || /\s/.test(after);

    if (validBefore && validAfter) {
      return index;
    }
  }

  return -1;
}

export function splitTopLevelByKeyword(value: string, keyword: string): string[] {
  const parts: string[] = [];
  let remaining = value.trim();

  while (remaining.length > 0) {
    const index = findTopLevelKeyword(remaining, keyword);
    if (index === -1) {
      parts.push(remaining.trim());
      break;
    }

    parts.push(remaining.slice(0, index).trim());
    remaining = remaining.slice(index + keyword.length).trim();
  }

  return parts.filter((part) => part.length > 0);
}

export function findMatchingParen(value: string, openIndex: number): number {
  let depth = 0;
  let quote: "'" | '"' | "`" | null = null;

  for (let index = openIndex; index < value.length; index += 1) {
    const character = value[index]!;

    if (quote) {
      if (character === quote && value[index - 1] !== "\\") {
        quote = null;
      }
      continue;
    }

    if (character === "'" || character === '"' || character === "`") {
      quote = character;
      continue;
    }

    if (character === "(") {
      depth += 1;
      continue;
    }

    if (character === ")") {
      depth -= 1;
      if (depth === 0) {
        return index;
      }
    }
  }

  throw new Error("Unbalanced parentheses in DDL.");
}
