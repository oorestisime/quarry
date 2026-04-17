import type { SelectQueryNode, TableNode } from "../ast/query";
import type { QueryColumnMap } from "../column-metadata";
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
  ) {}

  as<NextAlias extends string>(alias: NextAlias): TableSourceBuilder<DB, Table, NextAlias> {
    return new TableSourceBuilder(this.table, alias, this.isFinal);
  }

  final(): TableSourceBuilder<DB, Table, Alias> {
    return new TableSourceBuilder(this.table, this.alias, true);
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

export class AliasedQuery<_Output extends object, Alias extends string, _OutputColumns = {}> {
  constructor(
    private readonly query: SelectQueryNode,
    readonly alias: Alias,
    private readonly outputColumns?: QueryColumnMap,
  ) {}

  toAST(): SelectQueryNode {
    return structuredClone(this.query);
  }

  getOutputColumns(): QueryColumnMap | undefined {
    return this.outputColumns ? { ...this.outputColumns } : undefined;
  }
}
