function padNumber(value: number, width = 2): string {
  return globalThis.String(value).padStart(width, "0");
}

function formatDateValue(value: Date): string {
  return `${value.getUTCFullYear()}-${padNumber(value.getUTCMonth() + 1)}-${padNumber(value.getUTCDate())}`;
}

function formatDateTimeValue(value: Date, precision = 0): string {
  const base = `${formatDateValue(value)} ${padNumber(value.getUTCHours())}:${padNumber(value.getUTCMinutes())}:${padNumber(value.getUTCSeconds())}`;

  if (precision <= 0) {
    return base;
  }

  const milliseconds = padNumber(value.getUTCMilliseconds(), 3);
  const fractional =
    precision <= 3 ? milliseconds.slice(0, precision) : milliseconds.padEnd(precision, "0");

  return `${base}.${fractional}`;
}

export function normalizeClickHouseInputValue(value: unknown, clickhouseType: string): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  if (clickhouseType.startsWith("LowCardinality(") && clickhouseType.endsWith(")")) {
    return normalizeClickHouseInputValue(value, clickhouseType.slice("LowCardinality(".length, -1));
  }

  if (clickhouseType.startsWith("Nullable(") && clickhouseType.endsWith(")")) {
    return normalizeClickHouseInputValue(value, clickhouseType.slice("Nullable(".length, -1));
  }

  if (clickhouseType.startsWith("Array(") && clickhouseType.endsWith(")")) {
    const memberType = clickhouseType.slice("Array(".length, -1);
    return globalThis.Array.isArray(value)
      ? value.map((member) => normalizeClickHouseInputValue(member, memberType))
      : value;
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value instanceof globalThis.Date) {
    if (clickhouseType === "Date" || clickhouseType === "Date32") {
      return formatDateValue(value);
    }

    if (clickhouseType === "DateTime") {
      return formatDateTimeValue(value);
    }

    const dateTime64Match = /^DateTime64\((\d+)\)$/.exec(clickhouseType);
    if (dateTime64Match) {
      return formatDateTimeValue(value, Number(dateTime64Match[1]));
    }
  }

  return value;
}

export function normalizeInsertValue(value: unknown): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (globalThis.Array.isArray(value)) {
    return value.map((entry) => normalizeInsertValue(entry));
  }

  if (value instanceof globalThis.Date) {
    return value;
  }

  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, normalizeInsertValue(entry)]),
    );
  }

  return value;
}
