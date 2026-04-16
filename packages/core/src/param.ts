export class ClickHouseParam<T> {
  constructor(
    readonly value: T,
    readonly clickhouseType: string,
  ) {}
}

/**
 * Use an explicit ClickHouse type when inference is ambiguous or unsafe.
 *
 * In particular, prefer `param(...)` for query-side date/time values and other cases where
 * the runtime value shape should not rely on automatic inference alone.
 */
export function param<T>(value: T, clickhouseType: string): ClickHouseParam<T> {
  return new ClickHouseParam(value, clickhouseType);
}

export function isClickHouseParam(value: unknown): value is ClickHouseParam<unknown> {
  return value instanceof ClickHouseParam;
}
