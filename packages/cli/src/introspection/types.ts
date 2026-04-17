export type QuarryColumnClauseKind = "default" | "materialized" | "alias";

export interface QuarryColumnClause {
  readonly kind: QuarryColumnClauseKind;
  readonly sql: string;
}

export type ParsedColumn = {
  readonly name: string;
  readonly clickhouseType: string;
  readonly codecs?: readonly string[];
  readonly clause?: QuarryColumnClause;
};

export type ParsedTable = {
  readonly name: string;
  readonly columns: readonly ParsedColumn[];
  readonly engineName: string;
  readonly engineOptions: Record<string, unknown>;
};

export type ParsedSelection =
  | {
      readonly kind: "column";
      readonly column: string;
    }
  | {
      readonly kind: "function";
      readonly functionName: string;
      readonly alias: string;
      readonly args: readonly ParsedExpressionArg[];
    };

export type ParsedExpressionArg =
  | {
      readonly kind: "column";
      readonly column: string;
    }
  | {
      readonly kind: "boolean";
      readonly value: boolean;
    }
  | {
      readonly kind: "string";
      readonly value: string;
    }
  | {
      readonly kind: "number";
      readonly value: number;
    };

export type ParsedWhereCondition =
  | {
      readonly kind: "binary";
      readonly column: string;
      readonly operator: "=" | "!=" | ">" | ">=" | "<" | "<=";
      readonly value: ParsedExpressionArg;
    }
  | {
      readonly kind: "null";
      readonly column: string;
      readonly negated: boolean;
    };

export type ParsedView =
  | {
      readonly kind: "selectAll";
      readonly name: string;
      readonly sourceTable: string;
      readonly final?: boolean;
      readonly where?: readonly ParsedWhereCondition[];
    }
  | {
      readonly kind: "selectExpr";
      readonly name: string;
      readonly sourceTable: string;
      readonly selections: readonly ParsedSelection[];
      readonly groupBy?: readonly string[];
      readonly final?: boolean;
      readonly where?: readonly ParsedWhereCondition[];
    };

export type ParsedSchema = {
  readonly tables: readonly ParsedTable[];
  readonly views: readonly ParsedView[];
};

export interface IntrospectionObjectDescriptor {
  readonly name: string;
  readonly kind: "table" | "view";
  readonly dependencies: readonly string[];
}

export type ImportSpec =
  | "Array as CHArray"
  | "Bool"
  | "Date as CHDate"
  | "Date32"
  | "Decimal"
  | "DateTime"
  | "DateTime64"
  | "FixedString"
  | "Float32"
  | "Float64"
  | "IPv4"
  | "IPv6"
  | "Int8"
  | "Int16"
  | "Int32"
  | "Int64"
  | "LowCardinality"
  | "Nullable"
  | "String as CHString"
  | "UInt8"
  | "UInt16"
  | "UInt32"
  | "UInt64"
  | "UUID"
  | "defineSchema"
  | "table"
  | "type SchemaBuilder"
  | "view";
