import type { ExprNode, RefNode, SelectQueryNode, SelectionNode } from "../ast/query";
import type { QueryColumn, QueryColumnMap } from "../column-metadata";
import { compileSelectQuery, type CompiledQuery } from "../compiler/query-compiler";
import type { ClickHouseClient, QueryCapableClickHouseClient } from "../client";
import type { DatabaseSchema, ScopeMap, Simplify } from "../type-utils";
import { Expression, AliasedExpression, ExpressionBuilder } from "./expression-builder";
import {
  appendCondition,
  createValueNode,
  isQueryLike,
  parseSourceExpression,
  resolveSourceColumns,
  toSubqueryExpr,
} from "./helpers";
import { AliasedQuery } from "./source-builder";
import type {
  AllScopeSelectionColumns,
  ColumnRef,
  ExpressionPredicateValue,
  GroupByExpression,
  HavingValue,
  HavingRef,
  OrderByRef,
  PredicateOperator,
  PredicateValue,
  QueryLike,
  RefPredicateOperator,
  ResolvePredicateColumnType,
  ResolveHavingType,
  OnlyScopeAlias,
  ScopeFromSourceExpression,
  ScopeSelectionOutput,
  ScopeAlias,
  SelectionExpression,
  SelectionOutput,
  SelectionOutputColumns,
  AllScopeSelectionOutput,
  ScopeSelectionColumns,
  SourceExpression,
} from "./types";

type SelectAllResult<
  Scope extends ScopeMap,
  Output extends object,
  Alias extends ScopeAlias<Scope> | undefined,
> = Simplify<
  Output &
    (Alias extends ScopeAlias<Scope>
      ? ScopeSelectionOutput<Scope, Alias>
      : AllScopeSelectionOutput<Scope>)
>;

type SelectAllColumns<
  Scope extends ScopeMap,
  OutputColumns extends QueryColumnMap,
  Alias extends ScopeAlias<Scope> | undefined,
> = Simplify<
  OutputColumns &
    (Alias extends ScopeAlias<Scope>
      ? ScopeSelectionColumns<Scope, Alias>
      : AllScopeSelectionColumns<Scope>)
>;

type ScopeColumnMap = Record<string, QueryColumnMap>;

function parseSelectionParts(selection: string): { expr: string; alias?: string } {
  const match = selection.match(/^(.*?)\s+as\s+(.*?)$/i);

  if (!match) {
    return { expr: selection.trim() };
  }

  return {
    expr: match[1].trim(),
    alias: match[2].trim(),
  };
}

export interface ExecutableQuery<Output> {
  toSQL(): CompiledQuery;
  execute(client?: ClickHouseClient): Promise<Output[]>;
  executeTakeFirst(client?: ClickHouseClient): Promise<Output | undefined>;
  executeTakeFirstOrThrow(client?: ClickHouseClient): Promise<Output>;
}

function parseSelectionString(selection: string): SelectionNode {
  const match = selection.match(/^(.*?)\s+as\s+(.*?)$/i);

  if (!match) {
    return { expr: { kind: "ref", name: selection.trim() } };
  }

  return {
    expr: { kind: "ref", name: match[1].trim() },
    alias: match[2].trim(),
  };
}

export class SelectQueryBuilder<
  Sources extends DatabaseSchema,
  Scope extends ScopeMap,
  Output extends object,
  OutputColumns extends QueryColumnMap = {},
> implements ExecutableQuery<Output> {
  declare readonly __resultType: Output;
  declare readonly __outputColumns: OutputColumns;

  constructor(
    private readonly node: SelectQueryNode,
    private readonly client?: ClickHouseClient,
    private readonly scopeColumns: ScopeColumnMap = {},
    private readonly outputColumns?: QueryColumnMap,
  ) {}

  private next<
    NextScope extends ScopeMap = Scope,
    NextOutput extends object = Output,
    NextOutputColumns extends QueryColumnMap = OutputColumns,
  >(
    nextNode: SelectQueryNode,
    nextScopeColumns: ScopeColumnMap = this.scopeColumns,
    nextOutputColumns: QueryColumnMap | undefined = this.outputColumns,
  ): SelectQueryBuilder<Sources, NextScope, NextOutput, NextOutputColumns> {
    return new SelectQueryBuilder(nextNode, this.client, nextScopeColumns, nextOutputColumns);
  }

  private getPredicateClickHouseType(ref: ColumnRef<Scope>): string | undefined {
    return this.resolveScopeColumn(ref)?.clickhouseType;
  }

  private getBoundPredicateClickHouseType(
    ref: ColumnRef<Scope>,
    operator: PredicateOperator,
    value: unknown,
  ): string | undefined {
    const columnType = this.getPredicateClickHouseType(ref);
    if (!columnType) {
      return undefined;
    }

    if ((operator === "in" || operator === "not in") && Array.isArray(value)) {
      const hasDateMember = value.some((member) => member instanceof globalThis.Date);
      if (!hasDateMember) {
        return undefined;
      }

      return `Array(${columnType})`;
    }

    return value instanceof globalThis.Date ? columnType : undefined;
  }

  private resolveScopeColumn(ref: ColumnRef<Scope>): QueryColumn | undefined {
    if (ref.includes(".")) {
      const [alias, column] = ref.split(".");
      return alias && column ? this.scopeColumns[alias]?.[column] : undefined;
    }

    const aliases = Object.keys(this.scopeColumns);
    if (aliases.length !== 1) {
      return undefined;
    }

    return this.scopeColumns[aliases[0]]?.[ref];
  }

  private resolveSelectionColumns(
    selections: readonly SelectionExpression<Scope>[],
  ): QueryColumnMap | undefined {
    if (this.outputColumns === undefined && this.node.selections.length > 0) {
      return undefined;
    }

    const resolved = { ...this.outputColumns };

    for (const selection of selections) {
      if (typeof selection === "string") {
        const { expr, alias } = parseSelectionParts(selection);
        const column = this.resolveScopeColumn(expr as ColumnRef<Scope>);
        const outputName = alias ?? expr.split(".").at(-1);

        if (!column || !outputName) {
          return undefined;
        }

        resolved[outputName] = column;
        continue;
      }

      if (!selection.clickhouseType) {
        return undefined;
      }

      resolved[selection.alias] = { clickhouseType: selection.clickhouseType };
    }

    return resolved;
  }

  private resolveSelectAllColumns(table?: ScopeAlias<Scope>): QueryColumnMap | undefined {
    if (this.outputColumns === undefined && this.node.selections.length > 0) {
      return undefined;
    }

    const resolved = { ...this.outputColumns };

    if (table) {
      const columns = this.scopeColumns[table];
      if (!columns) {
        return undefined;
      }

      Object.assign(resolved, columns);
      return resolved;
    }

    for (const columns of Object.values(this.scopeColumns)) {
      Object.assign(resolved, columns);
    }

    return resolved;
  }

  getOutputColumns(): QueryColumnMap | undefined {
    return this.outputColumns ? { ...this.outputColumns } : undefined;
  }

  as<Alias extends string>(alias: Alias): AliasedQuery<Simplify<Output>, Alias, OutputColumns> {
    return new AliasedQuery(this.toAST(), alias, this.getOutputColumns());
  }

  select<const Selections extends readonly SelectionExpression<Scope>[]>(
    ...selections: Selections
  ): SelectQueryBuilder<
    Sources,
    Scope,
    Simplify<Output & SelectionOutput<Scope, Selections>>,
    Simplify<OutputColumns & SelectionOutputColumns<Scope, Selections>>
  > {
    return this.next<
      Scope,
      Simplify<Output & SelectionOutput<Scope, Selections>>,
      Simplify<OutputColumns & SelectionOutputColumns<Scope, Selections>>
    >(
      {
        ...this.node,
        selections: [
          ...this.node.selections,
          ...selections.map((selection) => {
            if (selection instanceof AliasedExpression) {
              return {
                expr: selection.node,
                alias: selection.alias,
              };
            }

            return parseSelectionString(selection);
          }),
        ],
      },
      this.scopeColumns,
      this.resolveSelectionColumns(selections),
    );
  }

  selectAll(): SelectQueryBuilder<
    Sources,
    Scope,
    SelectAllResult<Scope, Output, OnlyScopeAlias<Scope>>,
    SelectAllColumns<Scope, OutputColumns, OnlyScopeAlias<Scope>>
  >;
  selectAll<Alias extends ScopeAlias<Scope>>(
    table: Alias,
  ): SelectQueryBuilder<
    Sources,
    Scope,
    SelectAllResult<Scope, Output, Alias>,
    SelectAllColumns<Scope, OutputColumns, Alias>
  >;
  selectAll<Alias extends ScopeAlias<Scope> | OnlyScopeAlias<Scope> = OnlyScopeAlias<Scope>>(
    table?: Alias,
  ): SelectQueryBuilder<
    Sources,
    Scope,
    SelectAllResult<Scope, Output, Alias>,
    SelectAllColumns<Scope, OutputColumns, Alias>
  > {
    return this.next<
      Scope,
      SelectAllResult<Scope, Output, Alias>,
      SelectAllColumns<Scope, OutputColumns, Alias>
    >(
      {
        ...this.node,
        selections: [
          ...this.node.selections,
          {
            expr: {
              kind: "raw",
              sql: table ? `${table}.*` : "*",
            },
          },
        ],
      },
      this.scopeColumns,
      this.resolveSelectAllColumns(table),
    );
  }

  selectExpr<const Selections extends readonly SelectionExpression<Scope>[]>(
    selectionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Selections,
  ): SelectQueryBuilder<
    Sources,
    Scope,
    Simplify<Output & SelectionOutput<Scope, Selections>>,
    Simplify<OutputColumns & SelectionOutputColumns<Scope, Selections>>
  > {
    return this.select(...selectionFactory(new ExpressionBuilder<Scope>(this.scopeColumns)));
  }

  where(
    predicateFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  where<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: PredicateValue<ResolvePredicateColumnType<Scope, Ref>, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  where<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  where<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  where<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: ExpressionPredicateValue<Value, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  where(
    input:
      | ColumnRef<Scope>
      | ((expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>),
    operator?: PredicateOperator,
    value?: unknown,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    if (arguments.length === 1) {
      return this.addExpressionCondition(
        "where",
        input as (eb: ExpressionBuilder<Scope>) => Expression<unknown>,
      );
    }

    return this.addPredicate("where", input, operator!, value);
  }

  whereRef<Left extends ColumnRef<Scope>, Right extends ColumnRef<Scope>>(
    left: Left,
    operator: RefPredicateOperator,
    right: Right,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    return this.next({
      ...this.node,
      where: appendCondition(this.node.where, {
        kind: "binary",
        left: { kind: "ref", name: left },
        op: operator,
        right: { kind: "ref", name: right },
      }),
    });
  }

  prewhere(
    predicateFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  prewhere<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: PredicateValue<ResolvePredicateColumnType<Scope, Ref>, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  prewhere<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  prewhere<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  prewhere<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: ExpressionPredicateValue<Value, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  prewhere(
    input:
      | ColumnRef<Scope>
      | ((expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>),
    operator?: PredicateOperator,
    value?: unknown,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    if (arguments.length === 1) {
      return this.addExpressionCondition(
        "prewhere",
        input as (eb: ExpressionBuilder<Scope>) => Expression<unknown>,
      );
    }

    return this.addPredicate("prewhere", input, operator!, value);
  }

  prewhereRef<Left extends ColumnRef<Scope>, Right extends ColumnRef<Scope>>(
    left: Left,
    operator: RefPredicateOperator,
    right: Right,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    return this.next({
      ...this.node,
      prewhere: appendCondition(this.node.prewhere, {
        kind: "binary",
        left: { kind: "ref", name: left },
        op: operator,
        right: { kind: "ref", name: right },
      }),
    });
  }

  whereNull<Ref extends ColumnRef<Scope>>(
    column: Ref,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    return this.next({
      ...this.node,
      where: appendCondition(this.node.where, {
        kind: "binary",
        left: { kind: "ref", name: column },
        op: "IS",
        right: { kind: "raw", sql: "NULL" },
      }),
    });
  }

  whereNotNull<Ref extends ColumnRef<Scope>>(
    column: Ref,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    return this.next({
      ...this.node,
      where: appendCondition(this.node.where, {
        kind: "binary",
        left: { kind: "ref", name: column },
        op: "IS NOT",
        right: { kind: "raw", sql: "NULL" },
      }),
    });
  }

  having(
    predicateFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  having<Ref extends HavingRef<Scope, Output>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: HavingValue<ResolveHavingType<Scope, Output, Ref>, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  having<Ref extends HavingRef<Scope, Output>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  having<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  having<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: HavingValue<Value, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns>;
  having(
    input:
      | HavingRef<Scope, Output>
      | ((expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>),
    operator?: PredicateOperator,
    value?: unknown,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    if (arguments.length === 1) {
      return this.addExpressionCondition(
        "having",
        input as (eb: ExpressionBuilder<Scope>) => Expression<unknown>,
      );
    }

    const expressionBuilder = new ExpressionBuilder<Scope>(this.scopeColumns);
    const leftExpr =
      typeof input === "function"
        ? input(expressionBuilder).node
        : ({ kind: "ref", name: input } satisfies RefNode);

    return this.next({
      ...this.node,
      having: appendCondition(this.node.having, {
        kind: "binary",
        left: leftExpr,
        op: operator!.toUpperCase(),
        right: isQueryLike(value) ? toSubqueryExpr(value) : createValueNode(value),
      }),
    });
  }

  groupBy<const Expressions extends readonly GroupByExpression<Scope>[]>(
    ...expressions: Expressions
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    return this.next({
      ...this.node,
      groupBy: [
        ...this.node.groupBy,
        ...expressions.map((expression) => {
          if (typeof expression === "function") {
            return expression(new ExpressionBuilder<Scope>(this.scopeColumns)).node;
          }

          return { kind: "ref", name: expression } satisfies RefNode;
        }),
      ],
    });
  }

  private addPredicate(
    key: "where" | "prewhere",
    input:
      | ColumnRef<Scope>
      | ((expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>),
    operator: PredicateOperator | undefined,
    value: unknown,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    const expressionBuilder = new ExpressionBuilder<Scope>(this.scopeColumns);
    const leftExpr =
      typeof input === "function"
        ? input(expressionBuilder).node
        : ({ kind: "ref", name: input } satisfies RefNode);

    const rightExpr = isQueryLike(value)
      ? toSubqueryExpr(value)
      : createValueNode(
          value,
          typeof input === "string"
            ? this.getBoundPredicateClickHouseType(input, operator!, value)
            : undefined,
        );

    const nextCondition = {
      kind: "binary",
      left: leftExpr,
      op: operator!.toUpperCase(),
      right: rightExpr,
    } as const;

    return this.next({
      ...this.node,
      [key]: appendCondition(this.node[key], nextCondition),
    });
  }

  private addExpressionCondition(
    key: "where" | "prewhere" | "having",
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    return this.next({
      ...this.node,
      [key]: appendCondition(
        this.node[key],
        expressionFactory(new ExpressionBuilder<Scope>(this.scopeColumns)).node,
      ),
    });
  }

  innerJoin<Source extends SourceExpression<Sources>>(
    source: Source,
    left: ColumnRef<Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>>,
    right: ColumnRef<Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>>,
  ): SelectQueryBuilder<
    Sources,
    Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>,
    Output
  >;
  innerJoin<Source extends SourceExpression<Sources>>(
    source: Source,
    callback: (
      expressionBuilder: ExpressionBuilder<
        Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>
      >,
    ) => Expression<unknown>,
  ): SelectQueryBuilder<
    Sources,
    Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>,
    Output
  >;
  innerJoin<Source extends SourceExpression<Sources>>(
    source: Source,
    leftOrCallback:
      | ColumnRef<Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>>
      | ((
          expressionBuilder: ExpressionBuilder<
            Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>
          >,
        ) => Expression<unknown>),
    right?: ColumnRef<Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>>,
  ): SelectQueryBuilder<
    Sources,
    Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>,
    Output
  > {
    return this.addJoin("INNER", source, leftOrCallback, right);
  }

  /**
   * ClickHouse LEFT JOIN semantics differ from databases that default unmatched joins to nulls.
   * Unless the query/session enables `join_use_nulls = 1`, unmatched right-side columns are
   * returned as type defaults such as `0`, `''`, or `false` instead of `null`.
   *
   * The current builder typing follows that default ClickHouse runtime behavior.
   */
  leftJoin<Source extends SourceExpression<Sources>>(
    source: Source,
    left: ColumnRef<Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>>,
    right: ColumnRef<Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>>,
  ): SelectQueryBuilder<
    Sources,
    Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>,
    Output
  >;
  leftJoin<Source extends SourceExpression<Sources>>(
    source: Source,
    callback: (
      expressionBuilder: ExpressionBuilder<
        Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>
      >,
    ) => Expression<unknown>,
  ): SelectQueryBuilder<
    Sources,
    Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>,
    Output
  >;
  leftJoin<Source extends SourceExpression<Sources>>(
    source: Source,
    leftOrCallback:
      | ColumnRef<Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>>
      | ((
          expressionBuilder: ExpressionBuilder<
            Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>
          >,
        ) => Expression<unknown>),
    right?: ColumnRef<Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>>,
  ): SelectQueryBuilder<
    Sources,
    Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>,
    Output
  > {
    return this.addJoin("LEFT", source, leftOrCallback, right);
  }

  private addJoin<Source extends SourceExpression<Sources>>(
    joinType: "INNER" | "LEFT",
    source: Source,
    leftOrCallback:
      | string
      | ((
          expressionBuilder: ExpressionBuilder<
            Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>
          >,
        ) => Expression<unknown>),
    right?: string,
  ): SelectQueryBuilder<
    Sources,
    Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>,
    Output,
    OutputColumns
  > {
    const resolvedSource = resolveSourceColumns(source);
    const nextScopeColumns = resolvedSource
      ? { ...this.scopeColumns, [resolvedSource.alias]: resolvedSource.columns }
      : this.scopeColumns;
    const joinedScopeBuilder = new ExpressionBuilder<
      Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>
    >(nextScopeColumns);
    const on: ExprNode =
      typeof leftOrCallback === "function"
        ? leftOrCallback(joinedScopeBuilder).node
        : {
            kind: "binary",
            left: { kind: "ref", name: leftOrCallback },
            op: "=",
            right: { kind: "ref", name: right! },
          };

    return this.next<
      Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>,
      Output,
      OutputColumns
    >(
      {
        ...this.node,
        joins: [
          ...this.node.joins,
          {
            joinType,
            source: parseSourceExpression(source),
            on,
          },
        ],
      },
      nextScopeColumns,
    );
  }

  orderBy<Ref extends OrderByRef<Scope, Output>>(
    column: Ref,
    direction: "asc" | "desc" = "asc",
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    return this.next({
      ...this.node,
      orderBy: [
        ...this.node.orderBy,
        {
          expr: { kind: "ref", name: column },
          direction: direction.toUpperCase() as "ASC" | "DESC",
        },
      ],
    });
  }

  limit(limit: number): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    assertValidPaginationValue("LIMIT", limit);

    return this.next({
      ...this.node,
      limit,
    });
  }

  offset(offset: number): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    assertValidPaginationValue("OFFSET", offset);

    return this.next({
      ...this.node,
      offset,
    });
  }

  settings(
    settings: Record<string, string | number | boolean>,
  ): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    return this.next({
      ...this.node,
      settings: {
        ...this.node.settings,
        ...settings,
      },
    });
  }

  final(): SelectQueryBuilder<Sources, Scope, Output, OutputColumns> {
    if (!this.node.from || this.node.from.kind !== "table") {
      throw new Error("FINAL can only be applied to table sources.");
    }

    return this.next({
      ...this.node,
      from: {
        ...this.node.from,
        final: true,
      },
    });
  }

  toSQL(): CompiledQuery {
    return compileSelectQuery(this.node);
  }

  private getClient(client?: ClickHouseClient): QueryCapableClickHouseClient {
    const resolvedClient = client ?? this.client;

    if (!resolvedClient || typeof resolvedClient.query !== "function") {
      throw new Error(
        "No ClickHouse client configured. Pass one to execute() or createClickHouseDB().",
      );
    }

    return resolvedClient;
  }

  async execute(): Promise<Output[]>;
  async execute(client: ClickHouseClient): Promise<Output[]>;
  async execute(client = this.client): Promise<Output[]> {
    const resolvedClient = this.getClient(client);
    const compiled = this.toSQL();
    const result = await resolvedClient.query({
      query: compiled.query,
      query_params: compiled.params,
      format: "JSONEachRow",
    });

    return result.json<Output>();
  }

  async executeTakeFirst(): Promise<Output | undefined>;
  async executeTakeFirst(client: ClickHouseClient): Promise<Output | undefined>;
  async executeTakeFirst(client = this.client): Promise<Output | undefined> {
    const rows = client ? await this.execute(client) : await this.execute();
    return rows[0];
  }

  async executeTakeFirstOrThrow(): Promise<Output>;
  async executeTakeFirstOrThrow(client: ClickHouseClient): Promise<Output>;
  async executeTakeFirstOrThrow(client = this.client): Promise<Output> {
    const row = client ? await this.executeTakeFirst(client) : await this.executeTakeFirst();

    if (row === undefined) {
      throw new Error("Query returned no rows.");
    }

    return row;
  }

  toAST(): SelectQueryNode {
    return structuredClone(this.node);
  }
}

function assertValidPaginationValue(kind: "LIMIT" | "OFFSET", value: number): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${kind} must be a non-negative integer.`);
  }
}
