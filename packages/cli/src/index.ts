#!/usr/bin/env node

import { defineCommand, runMain } from "citty";
import process from "node:process";
import { introspectDatabase, writeSchemaModule } from "./introspect";

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
  },
  async run({ args }) {
    const source = await introspectDatabase(args);

    if (args.out) {
      await writeSchemaModule(source, args.out);
      process.stderr.write(`Wrote Quarry schema to ${args.out}\n`);
      return;
    }

    process.stdout.write(source);
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
