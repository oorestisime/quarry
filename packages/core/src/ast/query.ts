export interface RefNode {
  kind: "ref";
  name: string;
}

export interface ValueNode {
  kind: "value";
  value: unknown;
  clickhouseType?: string;
}

export interface RawNode {
  kind: "raw";
  sql: string;
}

export interface FunctionNode {
  kind: "function";
  name: string;
  args: ExprNode[];
}

export interface SubqueryExprNode {
  kind: "subqueryExpr";
  query: SelectQueryNode;
}

export interface BinaryNode {
  kind: "binary";
  left: ExprNode;
  op: string;
  right: ExprNode;
}

export interface LogicalNode {
  kind: "logical";
  op: "AND" | "OR";
  conditions: ExprNode[];
}

export type ExprNode =
  | RefNode
  | ValueNode
  | RawNode
  | FunctionNode
  | SubqueryExprNode
  | BinaryNode
  | LogicalNode;

export interface TableNode {
  kind: "table";
  name: string;
  alias?: string;
  final?: boolean;
}

export interface SubqueryNode {
  kind: "subquery";
  query: SelectQueryNode;
  alias: string;
}

export type SourceNode = TableNode | SubqueryNode;

export interface SelectionNode {
  expr: ExprNode;
  alias?: string;
}

export interface JoinNode {
  joinType: "INNER" | "LEFT" | "LEFT ANTI";
  source: SourceNode;
  on: ExprNode;
}

export interface OrderByNode {
  expr: ExprNode;
  direction: "ASC" | "DESC";
}

export interface CteNode {
  name: string;
  query: SelectQueryNode;
}

export interface InsertValuesNode {
  kind: "values";
  rows: object[];
}

export interface InsertSelectNode {
  kind: "select";
  query: SelectQueryNode;
}

export type InsertSourceNode = InsertValuesNode | InsertSelectNode;

export interface InsertQueryNode {
  table: string;
  columns?: string[];
  source?: InsertSourceNode;
}

export interface SelectQueryNode {
  with: CteNode[];
  distinct: boolean;
  distinctOn: ExprNode[];
  from?: SourceNode;
  selections: SelectionNode[];
  joins: JoinNode[];
  prewhere?: ExprNode;
  where?: ExprNode;
  having?: ExprNode;
  groupBy: ExprNode[];
  orderBy: OrderByNode[];
  limit?: number;
  offset?: number;
  settings: Record<string, string | number | boolean>;
}

export function createEmptySelectQueryNode(): SelectQueryNode {
  return {
    with: [],
    distinct: false,
    distinctOn: [],
    selections: [],
    joins: [],
    groupBy: [],
    orderBy: [],
    settings: {},
  };
}

export function createEmptyInsertQueryNode(table: string): InsertQueryNode {
  return { table };
}
