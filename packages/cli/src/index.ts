#!/usr/bin/env node

import { defineCommand, runMain } from "citty";
import process from "node:process";
import {
  formatIntrospectionFailureReport,
  introspectDatabase,
  writeSchemaModule,
} from "./introspect";

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

const introspectCommand = defineCommand({
  meta: {
    name: "introspect",
    description: "Read ClickHouse metadata and generate a Quarry schema module.",
  },
  args: {
    url: {
      type: "string",
      description: "ClickHouse HTTP URL. Falls back to CLICKHOUSE_URL.",
    },
    user: {
      type: "string",
      description: "ClickHouse username. Falls back to CLICKHOUSE_USER.",
    },
    password: {
      type: "string",
      description: "ClickHouse password. Falls back to CLICKHOUSE_PASSWORD.",
    },
    database: {
      type: "string",
      alias: "d",
      description: "Database name. Falls back to CLICKHOUSE_DATABASE or 'default'.",
    },
    out: {
      type: "string",
      alias: "o",
      description: "Write the generated module to a file instead of stdout.",
    },
    tablesOnly: {
      type: "boolean",
      description:
        "Only introspect table-like objects and skip views, dictionaries, and materialized views.",
    },
  },
  async run({ args }) {
    try {
      const result = await introspectDatabase(args);

      if (args.out) {
        await writeSchemaModule(result.source, args.out);
        process.stderr.write(`Wrote Quarry schema to ${args.out}\n`);
      } else {
        process.stdout.write(result.source);
      }

      if (result.failures.length > 0) {
        const report = `${formatIntrospectionFailureReport(result.failures)}\n`;
        if (args.out) {
          process.stdout.write(report);
        } else {
          process.stderr.write(`\n WARN  ${report}`);
        }
      }
    } catch (error) {
      process.stderr.write(`\n ERROR  ${getErrorMessage(error)}\n`);
      process.exitCode = 1;
    }
  },
});

const main = defineCommand({
  meta: {
    name: "quarry",
    description: "Workspace CLI for Quarry tooling.",
  },
  subCommands: {
    introspect: introspectCommand,
  },
});

runMain(main);
