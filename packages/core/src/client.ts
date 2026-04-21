export type ClickHouseSettingValue = string | number | boolean;
export type ClickHouseSettings = Record<string, ClickHouseSettingValue>;

interface ClickHouseQueryResult {
  json<T>(): Promise<T[]>;
}

interface ClickHouseBaseParams {
  clickhouse_settings?: ClickHouseSettings;
  query_id?: string;
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

export type ClickHouseClient = ClickHouseQueryClient &
  Partial<ClickHouseInsertClient> &
  Partial<ClickHouseCommandClient>;

export interface ClickHouseExecutionOptions {
  client?: ClickHouseClient;
  queryId?: string;
  clickhouse_settings?: ClickHouseSettings;
}

export function toClickHouseExecutionParams(
  options: ClickHouseExecutionOptions,
): ClickHouseBaseParams {
  return {
    ...(options.queryId === undefined ? {} : { query_id: options.queryId }),
    ...(options.clickhouse_settings === undefined
      ? {}
      : { clickhouse_settings: options.clickhouse_settings }),
  };
}

export type QueryCapableClickHouseClient = ClickHouseQueryClient;
export type InsertCapableClickHouseClient = ClickHouseInsertClient;
export type CommandCapableClickHouseClient = ClickHouseCommandClient;
