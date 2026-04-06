import type {
  ClickHouseClient,
  ClickHouseInsertResult,
  InsertCapableClickHouseClient,
} from "../client";

export interface CompiledInsertQuery<Row> {
  query: string;
  values: Row[];
}

export class InsertQueryBuilder<Table extends string, Row extends object> {
  constructor(
    private readonly table: Table,
    private readonly rows: Row[] = [],
    private readonly client?: ClickHouseClient,
  ) {}

  values(rows: readonly Row[]): InsertQueryBuilder<Table, Row> {
    if (this.rows.length > 0) {
      throw new Error("values() can only be called once per insert query.");
    }

    return new InsertQueryBuilder(this.table, [...rows], this.client);
  }

  toSQL(): CompiledInsertQuery<Row> {
    if (this.rows.length === 0) {
      throw new Error("Cannot compile an insert without any values");
    }

    return {
      query: `INSERT INTO ${this.table} FORMAT JSONEachRow`,
      values: structuredClone(this.rows),
    };
  }

  private getClient(client?: ClickHouseClient): InsertCapableClickHouseClient {
    const resolvedClient = client ?? this.client;

    if (!resolvedClient || typeof resolvedClient.insert !== "function") {
      throw new Error(
        "No ClickHouse insert client configured. Pass one to execute() or createClickHouseDB().",
      );
    }

    return resolvedClient as InsertCapableClickHouseClient;
  }

  async execute(): Promise<ClickHouseInsertResult>;
  async execute(client: ClickHouseClient): Promise<ClickHouseInsertResult>;
  async execute(client?: ClickHouseClient): Promise<ClickHouseInsertResult> {
    if (this.rows.length === 0) {
      throw new Error("Cannot execute an insert without any values");
    }

    const resolvedClient = this.getClient(client);

    return resolvedClient.insert({
      table: this.table,
      values: this.rows,
      format: "JSONEachRow",
    });
  }
}
