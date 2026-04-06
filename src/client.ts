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

interface ClickHouseInsertClient {
  insert<T>(params: {
    table: string;
    values: T[];
    format: "JSONEachRow";
  }): Promise<ClickHouseInsertResult>;
}

export type ClickHouseClient = ClickHouseQueryClient & Partial<ClickHouseInsertClient>;

export type QueryCapableClickHouseClient = ClickHouseQueryClient;
export type InsertCapableClickHouseClient = ClickHouseInsertClient;
