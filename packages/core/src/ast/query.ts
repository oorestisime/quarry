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

export interface FragmentNode {
  kind: "fragment";
  strings: string[];
  values: ExprNode[];
}

export interface IdentifierNode {
  kind: "identifier";
  parts: string[];
}

export interface WindowNode {
  kind: "window";
  expression: ExprNode;
  partitionBy: ExprNode[];
  orderBy: OrderByNode[];
  rows?: {
    start: number | "unbounded preceding" | "current row";
    end: number | "unbounded following" | "current row";
  };
}

export interface FunctionNode {
  kind: "function";
  name: string;
  parameters?: ExprNode[];
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
  | FragmentNode
  | IdentifierNode
  | WindowNode
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
  nullable?: boolean;
}

export interface OrderByNode {
  expr: ExprNode;
  direction: "ASC" | "DESC";
}

export interface ArrayJoinNode {
  kind: "ARRAY" | "LEFT ARRAY";
  expr: ExprNode;
}

export interface LimitByNode {
  limit: number;
  offset?: number;
  expressions: ExprNode[];
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
  unionAll?: SelectQueryNode[];
  with: CteNode[];
  distinct: boolean;
  distinctOn: ExprNode[];
  from?: SourceNode;
  selections: SelectionNode[];
  arrayJoins: ArrayJoinNode[];
  joins: JoinNode[];
  prewhere?: ExprNode;
  where?: ExprNode;
  having?: ExprNode;
  groupBy: ExprNode[];
  withTotals: boolean;
  orderBy: OrderByNode[];
  limitBy?: LimitByNode;
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
    arrayJoins: [],
    joins: [],
    groupBy: [],
    withTotals: false,
    orderBy: [],
    settings: {},
  };
}

export function createEmptyInsertQueryNode(table: string): InsertQueryNode {
  return { table };
}
