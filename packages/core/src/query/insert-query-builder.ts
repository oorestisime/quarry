import type { InsertQueryNode } from "../ast/query";
import { compileInsertQuery, type CompiledInsertQuery } from "../compiler/query-compiler";
import {
  toClickHouseExecutionParams,
  type ClickHouseClient,
  type ClickHouseInsertResult,
  type CommandCapableClickHouseClient,
  type ClickHouseExecutionOptions,
  type InsertCapableClickHouseClient,
} from "../client";
import { normalizeInsertValue } from "../input-normalization";
import type { Simplify } from "../type-utils";
import { quoteIdentifier, quoteTable } from "../compiler/identifiers";
import { selectionCount } from "./selection-count";

export type { CompiledInsertQuery } from "../compiler/query-compiler";

export class InsertQueryBuilder<
  Table extends string,
  Row extends object,
  Target extends readonly unknown[] = readonly unknown[],
> {
  constructor(
    private readonly node: InsertQueryNode,
    private readonly client?: ClickHouseClient | undefined,
  ) {}

  private next<NextRow extends object = Row, NextTarget extends readonly unknown[] = Target>(
    nextNode: InsertQueryNode,
  ): InsertQueryBuilder<Table, NextRow, NextTarget> {
    return new InsertQueryBuilder(nextNode, this.client);
  }

  columns<
    const Columns extends readonly [Extract<keyof Row, string>, ...Extract<keyof Row, string>[]],
  >(
    ...columns: Columns
  ): InsertQueryBuilder<
    Table,
    Simplify<Pick<Row, Columns[number]>>,
    { [Index in keyof Columns]: Row[Columns[Index]] }
  > {
    if (this.node.source) throw new Error("Set insert columns before the insert source.");
    if (new Set(columns).size !== columns.length) throw new Error("Insert columns must be unique.");
    return this.next<
      Simplify<Pick<Row, Columns[number]>>,
      { [Index in keyof Columns]: Row[Columns[Index]] }
    >({
      ...this.node,
      columns: [...columns],
    });
  }

  values(rows: readonly Row[]): InsertQueryBuilder<Table, Row, Target> {
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

  fromSelect<
    Query extends {
      toAST(): import("../ast/query").SelectQueryNode;
      readonly __selectionTypes: readonly unknown[];
    },
  >(
    query: Query &
      (number extends Target["length"]
        ? never
        : Query["__selectionTypes"] extends Target
          ? unknown
          : never),
  ): InsertQueryBuilder<Table, Row, Target> {
    if (this.node.source) {
      throw new Error("Insert source has already been set for this query.");
    }

    const select = query.toAST();
    if (!this.node.columns || selectionCount(select) !== this.node.columns.length) {
      throw new Error("INSERT SELECT column count must match the explicit target columns.");
    }
    return this.next({
      ...this.node,
      source: {
        kind: "select",
        query: select,
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

  async execute(options?: ClickHouseExecutionOptions): Promise<ClickHouseInsertResult> {
    if (!this.node.source) {
      throw new Error("Cannot execute an insert without a source");
    }

    if (this.node.source.kind === "values") {
      const resolvedClient = this.getInsertClient(options?.client);
      const values = this.node.source.rows.map((row) => normalizeInsertValue(row)) as Row[];

      return resolvedClient.insert({
        table: quoteTable(this.node.table),
        values,
        format: "JSONEachRow",
        columns: this.node.columns?.map(quoteIdentifier) as [string, ...string[]] | undefined,
        ...toClickHouseExecutionParams(options ?? {}),
      });
    }

    const resolvedClient = this.getCommandClient(options?.client);
    const compiled = this.toSQL();
    const result = await resolvedClient.command({
      query: compiled.query,
      query_params: compiled.params,
      ...toClickHouseExecutionParams(options ?? {}),
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
