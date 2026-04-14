import type { CteNode } from "../ast/query";
import { createEmptyInsertQueryNode, createEmptySelectQueryNode } from "../ast/query";
import type { ClickHouseClient } from "../client";
import {
  normalizeSchema,
  resolveSchemaDefinition,
  type NormalizedSchema,
  type SchemaBuilder,
  type SchemaDefinition,
} from "../schema";
import type {
  DatabaseSchema,
  InferResult,
  InsertRow,
  InsertableSourceName,
  Simplify,
  TableName,
} from "../type-utils";
import { parseSourceExpression, resolveSourceColumns } from "./helpers";
import { InsertQueryBuilder } from "./insert-query-builder";
import { SelectQueryBuilder } from "./select-query-builder";
import { TableSourceBuilder } from "./source-builder";
import type { ScopeFromSourceExpression, SourceExpression } from "./types";

export interface CreateClickHouseDBOptions {
  client?: ClickHouseClient;
  schema?: SchemaDefinition | SchemaBuilder<SchemaDefinition>;
}

export class ClickHouseDB<DB extends DatabaseSchema, Sources extends DatabaseSchema = DB> {
  constructor(
    private readonly client?: ClickHouseClient,
    private readonly withs: CteNode[] = [],
    private readonly schema?: NormalizedSchema,
  ) {}

  table<Table extends TableName<DB>>(table: Table): TableSourceBuilder<DB, Table> {
    return new TableSourceBuilder<DB, Table>(table, undefined, false, this.schema?.[table]);
  }

  with<Name extends string, Query extends SelectQueryBuilder<any, any, any, any>>(
    name: Name,
    callback: (db: ClickHouseDB<DB, Sources>) => Query,
  ): ClickHouseDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>> {
    const query = callback(new ClickHouseDB<DB, Sources>(this.client, [], this.schema));

    return new ClickHouseDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>>(
      this.client,
      [...this.withs, { name, query: query.toAST() }],
      this.schema,
    );
  }

  selectFrom<Source extends SourceExpression<Sources>>(
    source: Source,
  ): SelectQueryBuilder<Sources, ScopeFromSourceExpression<Sources, Source>, {}, {}> {
    const node = createEmptySelectQueryNode();
    node.with = structuredClone(this.withs);
    node.from = parseSourceExpression(source);
    const resolvedSource = resolveSourceColumns(source, this.schema);
    const scopeColumns = resolvedSource
      ? { [resolvedSource.alias]: resolvedSource.columns }
      : undefined;

    return new SelectQueryBuilder(node, this.client, this.schema, scopeColumns, {});
  }

  insertInto<Table extends InsertableSourceName<DB>>(
    table: Table,
  ): InsertQueryBuilder<Table, InsertRow<DB, Table>> {
    if (this.schema?.[table] && !this.schema[table].insertable) {
      throw new Error(`Source '${table}' is not insertable.`);
    }

    return new InsertQueryBuilder(createEmptyInsertQueryNode(table), this.client, this.schema?.[table]);
  }
}

export function createClickHouseDB<DB extends DatabaseSchema>(options?: {
  client?: ClickHouseClient;
}): ClickHouseDB<DB>;
export function createClickHouseDB<const Schema extends SchemaDefinition>(options: {
  client?: ClickHouseClient;
  schema: Schema;
}): ClickHouseDB<Schema>;
export function createClickHouseDB<const Schema extends SchemaDefinition>(options: {
  client?: ClickHouseClient;
  schema: SchemaBuilder<Schema>;
}): ClickHouseDB<Schema>;
export function createClickHouseDB<DB extends DatabaseSchema>(options?: CreateClickHouseDBOptions) {
  if (options?.schema) {
    const definition = resolveSchemaDefinition(options.schema);
    return new ClickHouseDB(options.client, [], normalizeSchema(definition));
  }

  return new ClickHouseDB<DB>(options?.client);
}
