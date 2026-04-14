import type { SelectQueryNode, TableNode } from "../ast/query";
import type { NormalizedSchemaSource } from "../schema";
import type { DatabaseSchema, TableName } from "../type-utils";

export class TableSourceBuilder<
  DB extends DatabaseSchema,
  Table extends TableName<DB>,
  Alias extends string = Table,
> {
  constructor(
    readonly table: Table,
    readonly alias?: Alias,
    readonly isFinal = false,
    private readonly schemaSource?: NormalizedSchemaSource,
  ) {}

  as<NextAlias extends string>(alias: NextAlias): TableSourceBuilder<DB, Table, NextAlias> {
    return new TableSourceBuilder(this.table, alias, this.isFinal, this.schemaSource);
  }

  final(): TableSourceBuilder<DB, Table, Alias> {
    if (this.schemaSource && !this.schemaSource.finalCapable) {
      throw new Error(`FINAL is not supported for source '${this.table}'.`);
    }

    return new TableSourceBuilder(this.table, this.alias, true, this.schemaSource);
  }

  toSourceNode(): TableNode {
    return {
      kind: "table",
      name: this.table,
      alias: this.alias,
      final: this.isFinal,
    };
  }
}

export class AliasedQuery<_Output extends object, Alias extends string> {
  constructor(
    private readonly query: SelectQueryNode,
    readonly alias: Alias,
  ) {}

  toAST(): SelectQueryNode {
    return structuredClone(this.query);
  }
}
