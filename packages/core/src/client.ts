export type ClickHouseSettingValue = string | number | boolean;

export type ClickHouseSettings = Record<string, ClickHouseSettingValue>;

interface ClickHouseQueryResult {
  json<T>(): Promise<T[]>;
  stream?<T>(): AsyncIterable<readonly ClickHouseResultRow<T>[]>;
}

interface ClickHouseJSONQueryResult {
  json<T>(): Promise<ClickHouseJSONResponse<T>>;
}

interface ClickHouseJSONResponse<T> {
  data: T[];
  totals?: T;
}

interface ClickHouseResultRow<T> {
  json(): T;
}

interface ClickHouseBaseParams {
  clickhouse_settings?: ClickHouseSettings;
  query_id?: string;
  abort_signal?: AbortSignal;
}

// Separate interfaces let TypeScript check generic drivers against each format independently.
interface ClickHouseJSONQueryClient {
  query(
    params: ClickHouseBaseParams & {
      query: string;
      query_params?: Record<string, unknown>;
      format: "JSON";
    },
  ): Promise<ClickHouseJSONQueryResult>;
}

interface ClickHouseQueryClient {
  query(
    params: ClickHouseBaseParams & {
      query: string;
      query_params?: Record<string, unknown>;
      format: "JSONEachRow";
    },
  ): Promise<ClickHouseQueryResult>;
}

export interface ClickHouseInsertResult {
  executed: boolean;
  query_id: string;
}

interface ClickHouseCommandResult {
  query_id: string;
}

interface ClickHouseInsertClient {
  insert<T>(
    params: ClickHouseBaseParams & {
      table: string;
      values: T[];
      format: "JSONEachRow";
      columns?: [string, ...string[]];
    },
  ): Promise<ClickHouseInsertResult>;
}

interface ClickHouseCommandClient {
  command(
    params: ClickHouseBaseParams & {
      query: string;
      query_params?: Record<string, unknown>;
    },
  ): Promise<ClickHouseCommandResult>;
}

export type ClickHouseClient = ClickHouseJSONQueryClient &
  ClickHouseQueryClient &
  Partial<ClickHouseInsertClient> &
  Partial<ClickHouseCommandClient>;

export interface ClickHouseExecutionOptions {
  client?: ClickHouseClient;
  queryId?: string;
  clickhouse_settings?: ClickHouseSettings;
  abortSignal?: AbortSignal;
}

export interface ClickHouseRetryOptions {
  attempts: number;
  delayMs: number;
}

export function toClickHouseExecutionParams(
  options: ClickHouseExecutionOptions,
): ClickHouseBaseParams {
  const params: ClickHouseBaseParams = {};

  if (options.abortSignal !== undefined) {
    params.abort_signal = options.abortSignal;
  }

  if (options.queryId !== undefined) {
    params.query_id = options.queryId;
  }

  if (options.clickhouse_settings !== undefined) {
    params.clickhouse_settings = options.clickhouse_settings;
  }

  return params;
}

export type QueryCapableClickHouseClient = ClickHouseJSONQueryClient & ClickHouseQueryClient;

export type InsertCapableClickHouseClient = ClickHouseInsertClient;

export type CommandCapableClickHouseClient = ClickHouseCommandClient;
