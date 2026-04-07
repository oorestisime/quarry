import type { ExprNode, RefNode, SelectQueryNode, SelectionNode } from "../ast/query";
import { compileSelectQuery, type CompiledQuery } from "../compiler/query-compiler";
import type { ClickHouseClient, QueryCapableClickHouseClient } from "../client";
import type { DatabaseSchema, ScopeMap, Simplify } from "../type-utils";
import { Expression, AliasedExpression, ExpressionBuilder } from "./expression-builder";
import {
  appendCondition,
  createValueNode,
  isQueryLike,
  parseSourceExpression,
  toSubqueryExpr,
} from "./helpers";
import { AliasedQuery } from "./source-builder";
import type {
  ColumnRef,
  ExpressionPredicateValue,
  HavingValue,
  HavingRef,
  OrderByRef,
  PredicateOperator,
  PredicateValue,
  QueryLike,
  RefPredicateOperator,
  ResolveColumnType,
  ResolveHavingType,
  OnlyScopeAlias,
  ScopeFromSourceExpression,
  ScopeSelectionOutput,
  ScopeAlias,
  SelectionExpression,
  SelectionOutput,
  AllScopeSelectionOutput,
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
> implements ExecutableQuery<Output> {
  declare readonly __resultType: Output;

  constructor(
    private readonly node: SelectQueryNode,
    private readonly client?: ClickHouseClient,
  ) {}

  private next<NextScope extends ScopeMap = Scope, NextOutput extends object = Output>(
    nextNode: SelectQueryNode,
  ): SelectQueryBuilder<Sources, NextScope, NextOutput> {
    return new SelectQueryBuilder(nextNode, this.client);
  }

  as<Alias extends string>(alias: Alias): AliasedQuery<Simplify<Output>, Alias> {
    return new AliasedQuery(this.toAST(), alias);
  }

  select<const Selections extends readonly SelectionExpression<Scope>[]>(
    ...selections: Selections
  ): SelectQueryBuilder<Sources, Scope, Simplify<Output & SelectionOutput<Scope, Selections>>> {
    return this.next<Scope, Simplify<Output & SelectionOutput<Scope, Selections>>>({
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
    });
  }

  selectAll(): SelectQueryBuilder<
    Sources,
    Scope,
    SelectAllResult<Scope, Output, OnlyScopeAlias<Scope>>
  >;
  selectAll<Alias extends ScopeAlias<Scope>>(
    table: Alias,
  ): SelectQueryBuilder<Sources, Scope, SelectAllResult<Scope, Output, Alias>>;
  selectAll<Alias extends ScopeAlias<Scope> | OnlyScopeAlias<Scope> = OnlyScopeAlias<Scope>>(
    table?: Alias,
  ): SelectQueryBuilder<Sources, Scope, SelectAllResult<Scope, Output, Alias>> {
    return this.next<Scope, SelectAllResult<Scope, Output, Alias>>({
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
    });
  }

  selectExpr<const Selections extends readonly SelectionExpression<Scope>[]>(
    selectionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Selections,
  ): SelectQueryBuilder<Sources, Scope, Simplify<Output & SelectionOutput<Scope, Selections>>> {
    return this.select(...selectionFactory(new ExpressionBuilder<Scope>()));
  }

  where(
    predicateFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  where<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: PredicateValue<ResolveColumnType<Scope, Ref>, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  where<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  where<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: ExpressionPredicateValue<Value, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  where<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  where(
    input:
      | ColumnRef<Scope>
      | ((expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>),
    operator?: PredicateOperator,
    value?: unknown,
  ): SelectQueryBuilder<Sources, Scope, Output> {
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
  ): SelectQueryBuilder<Sources, Scope, Output> {
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
  ): SelectQueryBuilder<Sources, Scope, Output>;
  prewhere<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: PredicateValue<ResolveColumnType<Scope, Ref>, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  prewhere<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  prewhere<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: ExpressionPredicateValue<Value, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  prewhere<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  prewhere(
    input:
      | ColumnRef<Scope>
      | ((expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>),
    operator?: PredicateOperator,
    value?: unknown,
  ): SelectQueryBuilder<Sources, Scope, Output> {
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
  ): SelectQueryBuilder<Sources, Scope, Output> {
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

  whereNull<Ref extends ColumnRef<Scope>>(column: Ref): SelectQueryBuilder<Sources, Scope, Output> {
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
  ): SelectQueryBuilder<Sources, Scope, Output> {
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
  ): SelectQueryBuilder<Sources, Scope, Output>;
  having<Ref extends HavingRef<Scope, Output>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: HavingValue<ResolveHavingType<Scope, Output, Ref>, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  having<Ref extends HavingRef<Scope, Output>, Operator extends PredicateOperator>(
    column: Ref,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  having<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: HavingValue<Value, Operator>,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  having<Value, Operator extends PredicateOperator>(
    expressionFactory: (expressionBuilder: ExpressionBuilder<Scope>) => Expression<Value>,
    operator: Operator,
    value: QueryLike,
  ): SelectQueryBuilder<Sources, Scope, Output>;
  having(
    input:
      | HavingRef<Scope, Output>
      | ((expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>),
    operator?: PredicateOperator,
    value?: unknown,
  ): SelectQueryBuilder<Sources, Scope, Output> {
    if (arguments.length === 1) {
      return this.addExpressionCondition(
        "having",
        input as (eb: ExpressionBuilder<Scope>) => Expression<unknown>,
      );
    }

    const expressionBuilder = new ExpressionBuilder<Scope>();
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

  groupBy<Refs extends readonly ColumnRef<Scope>[]>(
    ...columns: Refs
  ): SelectQueryBuilder<Sources, Scope, Output> {
    return this.next({
      ...this.node,
      groupBy: [
        ...this.node.groupBy,
        ...columns.map((column) => ({ kind: "ref", name: column }) satisfies RefNode),
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
  ): SelectQueryBuilder<Sources, Scope, Output> {
    const expressionBuilder = new ExpressionBuilder<Scope>();
    const leftExpr =
      typeof input === "function"
        ? input(expressionBuilder).node
        : ({ kind: "ref", name: input } satisfies RefNode);

    const rightExpr = isQueryLike(value) ? toSubqueryExpr(value) : createValueNode(value);

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
  ): SelectQueryBuilder<Sources, Scope, Output> {
    return this.next({
      ...this.node,
      [key]: appendCondition(
        this.node[key],
        expressionFactory(new ExpressionBuilder<Scope>()).node,
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
    Output
  > {
    const joinedScopeBuilder = new ExpressionBuilder<
      Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>
    >();
    const on: ExprNode =
      typeof leftOrCallback === "function"
        ? leftOrCallback(joinedScopeBuilder).node
        : {
            kind: "binary",
            left: { kind: "ref", name: leftOrCallback },
            op: "=",
            right: { kind: "ref", name: right! },
          };

    return this.next<Simplify<Scope & ScopeFromSourceExpression<Sources, Source>>, Output>({
      ...this.node,
      joins: [
        ...this.node.joins,
        {
          joinType,
          source: parseSourceExpression(source),
          on,
        },
      ],
    });
  }

  orderBy<Ref extends OrderByRef<Scope, Output>>(
    column: Ref,
    direction: "asc" | "desc" = "asc",
  ): SelectQueryBuilder<Sources, Scope, Output> {
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

  limit(limit: number): SelectQueryBuilder<Sources, Scope, Output> {
    assertValidPaginationValue("LIMIT", limit);

    return this.next({
      ...this.node,
      limit,
    });
  }

  offset(offset: number): SelectQueryBuilder<Sources, Scope, Output> {
    assertValidPaginationValue("OFFSET", offset);

    return this.next({
      ...this.node,
      offset,
    });
  }

  settings(
    settings: Record<string, string | number | boolean>,
  ): SelectQueryBuilder<Sources, Scope, Output> {
    return this.next({
      ...this.node,
      settings: {
        ...this.node.settings,
        ...settings,
      },
    });
  }

  final(): SelectQueryBuilder<Sources, Scope, Output> {
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
