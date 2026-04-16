interface ClickHouseQueryResult {
  json<T>(): Promise<T[]>;
}

interface ClickHouseQueryClient {
  query(params: {
    query: string;
    query_params?: Record<string, unknown>;
    format: "JSONEachRow";
  }): Promise<ClickHouseQueryResult>;
}

export interface ClickHouseInsertResult {
  executed: boolean;
  query_id: string;
}

interface ClickHouseCommandResult {
  query_id: string;
}

interface ClickHouseInsertClient {
  insert<T>(params: {
    table: string;
    values: T[];
    format: "JSONEachRow";
    columns?: [string, ...string[]];
  }): Promise<ClickHouseInsertResult>;
}

interface ClickHouseCommandClient {
  command(params: {
    query: string;
    query_params?: Record<string, unknown>;
  }): Promise<ClickHouseCommandResult>;
}

export type ClickHouseClient = ClickHouseQueryClient &
  Partial<ClickHouseInsertClient> &
  Partial<ClickHouseCommandClient>;

export type QueryCapableClickHouseClient = ClickHouseQueryClient;
export type InsertCapableClickHouseClient = ClickHouseInsertClient;
export type CommandCapableClickHouseClient = ClickHouseCommandClient;
