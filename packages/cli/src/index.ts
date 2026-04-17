#!/usr/bin/env node

import { defineCommand, runMain } from "citty";
import process from "node:process";
import {
  formatIntrospectionFailureReport,
  formatIntrospectionSummaryReport,
  introspectDatabase,
  writeSchemaModule,
} from "./introspect";

const SPINNER_FRAMES = ["-", "\\", "|", "/"] as const;

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function startSpinner(label: string): () => void {
  if (!process.stderr.isTTY) {
    return () => {};
  }

  let frameIndex = 0;
  const render = () => {
    process.stderr.write(`\r${SPINNER_FRAMES[frameIndex]} ${label}`);
    frameIndex = (frameIndex + 1) % SPINNER_FRAMES.length;
  };

  render();
  const timer = setInterval(render, 80);
  timer.unref();

  return () => {
    clearInterval(timer);
    process.stderr.write("\r\u001B[2K");
  };
}

const introspectCommand = defineCommand({
  meta: {
    name: "introspect",
    description: "Read ClickHouse metadata and generate plain TypeScript DB types.",
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
      description: "Write the generated TypeScript module to a file instead of stdout.",
    },
    tablesOnly: {
      type: "boolean",
      description:
        "Only introspect table-like objects and skip views, dictionaries, and materialized views.",
    },
    includePattern: {
      type: "string",
      description: "Regex include filter for object names.",
    },
    excludePattern: {
      type: "string",
      description: "Regex blocklist for object names.",
    },
  },
  async run({ args }) {
    const stopSpinner = startSpinner("Introspecting ClickHouse schema...");

    try {
      const result = await introspectDatabase(args);
      stopSpinner();

      if (args.out) {
        await writeSchemaModule(result.source, args.out);
        process.stderr.write(`Wrote generated TypeScript DB types to ${args.out}\n`);
      } else {
        process.stdout.write(result.source);
      }

      process.stderr.write(`${formatIntrospectionSummaryReport(result.summary)}\n`);

      if (result.failures.length > 0) {
        const report = `${formatIntrospectionFailureReport(result.failures)}\n`;
        process.stderr.write(` WARN  ${report}`);
      }
    } catch (error) {
      stopSpinner();
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
