import type { CteNode } from "../ast/query";
import { createEmptyInsertQueryNode, createEmptySelectQueryNode } from "../ast/query";
import type { ClickHouseClient } from "../client";
import type { DatabaseSchema, InferResult, Simplify, TableName, TableRow } from "../type-utils";
import { parseSourceExpression } from "./helpers";
import { InsertQueryBuilder } from "./insert-query-builder";
import { SelectQueryBuilder } from "./select-query-builder";
import { TableSourceBuilder } from "./source-builder";
import type { ScopeFromSourceExpression, SourceExpression } from "./types";

export interface CreateClickHouseDBOptions {
  client?: ClickHouseClient;
}

export class ClickHouseDB<DB extends DatabaseSchema, Sources extends DatabaseSchema = DB> {
  constructor(
    private readonly client?: ClickHouseClient,
    private readonly withs: CteNode[] = [],
  ) {}

  table<Table extends TableName<DB>>(table: Table): TableSourceBuilder<DB, Table> {
    return new TableSourceBuilder(table);
  }

  with<Name extends string, Query extends SelectQueryBuilder<any, any, any>>(
    name: Name,
    callback: (db: ClickHouseDB<DB, Sources>) => Query,
  ): ClickHouseDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>> {
    const query = callback(new ClickHouseDB<DB, Sources>(this.client));

    return new ClickHouseDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>>(
      this.client,
      [...this.withs, { name, query: query.toAST() }],
    );
  }

  selectFrom<Source extends SourceExpression<Sources>>(
    source: Source,
  ): SelectQueryBuilder<Sources, ScopeFromSourceExpression<Sources, Source>, {}> {
    const node = createEmptySelectQueryNode();
    node.with = structuredClone(this.withs);
    node.from = parseSourceExpression(source);

    return new SelectQueryBuilder(node, this.client);
  }

  insertInto<Table extends TableName<DB>>(
    table: Table,
  ): InsertQueryBuilder<Table, TableRow<DB, Table>> {
    return new InsertQueryBuilder(createEmptyInsertQueryNode(table), this.client);
  }
}

export function createClickHouseDB<DB extends DatabaseSchema>(
  options?: CreateClickHouseDBOptions,
): ClickHouseDB<DB> {
  return new ClickHouseDB<DB>(options?.client);
}
