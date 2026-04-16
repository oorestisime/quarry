import { execFileSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import process from "node:process";
import { createClient } from "@clickhouse/client";
import { generateSchemaModuleFromDDL } from "@oorestisime/quarry/introspection";

export interface IntrospectArgs {
  readonly url?: string;
  readonly user?: string;
  readonly password?: string;
  readonly database?: string;
  readonly out?: string;
}

export interface IntrospectionConnectionOptions {
  readonly url: string;
  readonly user?: string;
  readonly password?: string;
  readonly database: string;
}

export interface IntrospectionObject {
  readonly name: string;
  readonly engine: string;
  readonly createTableQuery: string;
}

interface SystemTableRow {
  readonly name: string;
  readonly engine: string;
  readonly create_table_query: string;
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function normalizeCreateStatement(statement: string): string {
  const trimmed = statement.trim();
  return trimmed.endsWith(";") ? trimmed : `${trimmed};`;
}

function sortObjects(left: IntrospectionObject, right: IntrospectionObject): number {
  const leftRank = left.engine.endsWith("View") ? 1 : 0;
  const rightRank = right.engine.endsWith("View") ? 1 : 0;

  if (leftRank !== rightRank) {
    return leftRank - rightRank;
  }

  return left.name.localeCompare(right.name);
}

function tryFormatTypeScript(source: string): string {
  try {
    return execFileSync("oxfmt", ["--stdin-filepath", "schema.ts"], {
      input: source,
      encoding: "utf8",
    });
  } catch {
    return source;
  }
}

export function resolveConnectionOptions(
  args: IntrospectArgs,
  env: NodeJS.ProcessEnv = process.env,
): IntrospectionConnectionOptions {
  const url = args.url ?? env.CLICKHOUSE_URL;
  if (!url) {
    throw new Error("Missing ClickHouse URL. Pass --url or set CLICKHOUSE_URL in the environment.");
  }

  return {
    url,
    user: args.user ?? env.CLICKHOUSE_USER,
    password: args.password ?? env.CLICKHOUSE_PASSWORD,
    database: args.database ?? env.CLICKHOUSE_DATABASE ?? "default",
  };
}

export function buildSchemaModule(objects: readonly IntrospectionObject[]): string {
  if (objects.length === 0) {
    throw new Error("No tables or views were available to introspect.");
  }

  const orderedObjects = [...objects].sort(sortObjects);
  const failures = orderedObjects.flatMap((object) => {
    try {
      generateSchemaModuleFromDDL(normalizeCreateStatement(object.createTableQuery));
      return [];
    } catch (error) {
      return [
        {
          name: object.name,
          engine: object.engine,
          message: getErrorMessage(error),
        },
      ];
    }
  });

  if (failures.length > 0) {
    const details = failures
      .map((failure) => `- ${failure.name} (${failure.engine}): ${failure.message}`)
      .join("\n");
    throw new Error(`Could not generate a trusted Quarry schema for:\n${details}`);
  }

  const ddl = orderedObjects
    .map((object) => normalizeCreateStatement(object.createTableQuery))
    .join("\n\n");

  return tryFormatTypeScript(generateSchemaModuleFromDDL(ddl));
}

export async function fetchDatabaseObjects(
  options: IntrospectionConnectionOptions,
): Promise<IntrospectionObject[]> {
  const client = createClient({
    url: options.url,
    username: options.user,
    password: options.password,
    request_timeout: 30_000,
  });

  try {
    const result = await client.query({
      query: `
        SELECT
          name,
          engine,
          create_table_query
        FROM system.tables
        WHERE database = {database:String}
      `,
      query_params: {
        database: options.database,
      },
      format: "JSONEachRow",
    });

    const rows = await result.json<SystemTableRow>();
    return rows.map((row) => ({
      name: row.name,
      engine: row.engine,
      createTableQuery: row.create_table_query,
    }));
  } finally {
    await client.close();
  }
}

export async function writeSchemaModule(source: string, outPath: string): Promise<void> {
  await mkdir(dirname(outPath), { recursive: true });
  await writeFile(outPath, source, "utf8");
}

export async function introspectDatabase(args: IntrospectArgs): Promise<string> {
  const options = resolveConnectionOptions(args);
  const objects = await fetchDatabaseObjects(options);
  if (objects.length === 0) {
    throw new Error(`No tables or views found in database '${options.database}'.`);
  }

  return buildSchemaModule(objects);
}
