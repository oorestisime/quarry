import type { CteNode } from "../ast/query";
import { createEmptyInsertQueryNode, createEmptySelectQueryNode } from "../ast/query";
import type { ClickHouseClient, ClickHouseRetryOptions } from "../client";
import type {
  DatabaseSchema,
  InferResult,
  InsertRow,
  Simplify,
  SourceName,
  ValidInsertableSourceName,
} from "../type-utils";
import { parseSourceExpression, resolveSourceColumns } from "./helpers";
import { InsertQueryBuilder } from "./insert-query-builder";
import { SelectQueryBuilder } from "./select-query-builder";
import { TableSourceBuilder } from "./source-builder";
import type { ScopeFromSourceExpression, SourceExpression, ValidSourceExpression } from "./types";

export interface CreateClickHouseDBOptions {
  client?: ClickHouseClient;
  retries?: ClickHouseRetryOptions;
}

export class ClickHouseDB<DB extends DatabaseSchema, Sources extends DatabaseSchema = DB> {
  constructor(
    private readonly client?: ClickHouseClient,
    private readonly withs: CteNode[] = [],
    private readonly retries?: ClickHouseRetryOptions,
  ) {}

  table<Table extends SourceName<DB>>(
    table: Table & ValidInsertableSourceName<DB, Table>,
  ): TableSourceBuilder<DB, Table> {
    return new TableSourceBuilder<DB, Table>(table, undefined, false);
  }

  with<Name extends string, Query extends SelectQueryBuilder<any, any, any, any>>(
    name: Name,
    query: Query,
  ): ClickHouseDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>>;
  with<Name extends string, Query extends SelectQueryBuilder<any, any, any, any>>(
    name: Name,
    callback: (db: ClickHouseDB<DB, Sources>) => Query,
  ): ClickHouseDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>>;
  with<Name extends string, Query extends SelectQueryBuilder<any, any, any, any>>(
    name: Name,
    callbackOrQuery: ((db: ClickHouseDB<DB, Sources>) => Query) | Query,
  ): ClickHouseDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>> {
    const query =
      typeof callbackOrQuery === "function"
        ? callbackOrQuery(new ClickHouseDB<DB, Sources>(this.client, [], this.retries))
        : callbackOrQuery;

    return new ClickHouseDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>>(
      this.client,
      [...this.withs, { name, query: query.toAST() }],
      this.retries,
    );
  }

  selectFrom<Source extends SourceExpression<Sources>>(
    source: Source & ValidSourceExpression<Sources, Source>,
  ): SelectQueryBuilder<Sources, ScopeFromSourceExpression<Sources, Source>, {}, {}> {
    const node = createEmptySelectQueryNode();
    node.with = structuredClone(this.withs);
    node.from = parseSourceExpression(source);
    const resolvedSource = resolveSourceColumns(source);
    const scopeColumns = resolvedSource
      ? { [resolvedSource.alias]: resolvedSource.columns }
      : undefined;

    return new SelectQueryBuilder(node, this.client, scopeColumns, {}, this.retries);
  }

  insertInto<Table extends SourceName<DB>>(
    table: Table & ValidInsertableSourceName<DB, Table>,
  ): InsertQueryBuilder<Table, InsertRow<DB, Table>> {
    return new InsertQueryBuilder(createEmptyInsertQueryNode(table), this.client);
  }
}

export function createClickHouseDB<DB extends DatabaseSchema>(options?: {
  client?: ClickHouseClient;
  retries?: ClickHouseRetryOptions;
}): ClickHouseDB<DB>;
export function createClickHouseDB<DB extends DatabaseSchema>(options?: CreateClickHouseDBOptions) {
  return new ClickHouseDB<DB>(options?.client, [], options?.retries);
}
