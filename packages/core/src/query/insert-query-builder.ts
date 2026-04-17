import type { InsertQueryNode } from "../ast/query";
import { compileInsertQuery, type CompiledInsertQuery } from "../compiler/query-compiler";
import type {
  ClickHouseClient,
  ClickHouseInsertResult,
  CommandCapableClickHouseClient,
  InsertCapableClickHouseClient,
} from "../client";
import { normalizeInsertValue } from "../input-normalization";
import type { Simplify } from "../type-utils";
import type { SelectQueryBuilder } from "./select-query-builder";

export type { CompiledInsertQuery } from "../compiler/query-compiler";

export class InsertQueryBuilder<Table extends string, Row extends object> {
  constructor(
    private readonly node: InsertQueryNode,
    private readonly client?: ClickHouseClient,
  ) {}

  private next<NextRow extends object = Row>(
    nextNode: InsertQueryNode,
  ): InsertQueryBuilder<Table, NextRow> {
    return new InsertQueryBuilder(nextNode, this.client);
  }

  columns<
    const Columns extends readonly [Extract<keyof Row, string>, ...Extract<keyof Row, string>[]],
  >(...columns: Columns): InsertQueryBuilder<Table, Simplify<Pick<Row, Columns[number]>>> {
    return this.next<Simplify<Pick<Row, Columns[number]>>>({
      ...this.node,
      columns: [...columns],
    });
  }

  values(rows: readonly Row[]): InsertQueryBuilder<Table, Row> {
    if (this.node.source) {
      throw new Error("Insert source has already been set for this query.");
    }

    return this.next({
      ...this.node,
      source: {
        kind: "values",
        rows: [...rows],
      },
    });
  }

  fromSelect(query: SelectQueryBuilder<any, any, any>): InsertQueryBuilder<Table, Row> {
    if (this.node.source) {
      throw new Error("Insert source has already been set for this query.");
    }

    return this.next({
      ...this.node,
      source: {
        kind: "select",
        query: query.toAST(),
      },
    });
  }

  toSQL(): CompiledInsertQuery<Row> {
    return compileInsertQuery<Row>(this.node);
  }

  private getInsertClient(client?: ClickHouseClient): InsertCapableClickHouseClient {
    const resolvedClient = client ?? this.client;

    if (!resolvedClient || typeof resolvedClient.insert !== "function") {
      throw new Error(
        "No ClickHouse insert client configured. Pass one to execute() or createClickHouseDB().",
      );
    }

    return resolvedClient as InsertCapableClickHouseClient;
  }

  private getCommandClient(client?: ClickHouseClient): CommandCapableClickHouseClient {
    const resolvedClient = client ?? this.client;

    if (!resolvedClient || typeof resolvedClient.command !== "function") {
      throw new Error(
        "No ClickHouse command client configured. Pass one to execute() or createClickHouseDB().",
      );
    }

    return resolvedClient as CommandCapableClickHouseClient;
  }

  async execute(): Promise<ClickHouseInsertResult>;
  async execute(client: ClickHouseClient): Promise<ClickHouseInsertResult>;
  async execute(client?: ClickHouseClient): Promise<ClickHouseInsertResult> {
    if (!this.node.source) {
      throw new Error("Cannot execute an insert without a source");
    }

    if (this.node.source.kind === "values") {
      const resolvedClient = this.getInsertClient(client);
      const values = this.node.source.rows.map((row) => normalizeInsertValue(row)) as Row[];

      return resolvedClient.insert({
        table: this.node.table,
        values,
        format: "JSONEachRow",
        columns: this.node.columns as [string, ...string[]] | undefined,
      });
    }

    const resolvedClient = this.getCommandClient(client);
    const compiled = this.toSQL();
    const result = await resolvedClient.command({
      query: compiled.query,
      query_params: compiled.params,
    });

    return {
      executed: true,
      query_id: result.query_id,
    };
  }

  toAST(): InsertQueryNode {
    return structuredClone(this.node);
  }
}
