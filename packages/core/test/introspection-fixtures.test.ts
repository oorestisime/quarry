import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { generateSchemaModuleFromDDL } from "../src/introspection/schema-generator";

const currentDir = dirname(fileURLToPath(import.meta.url));
const fixturesDir = resolve(currentDir, "./introspect/fixtures");
const projectRoot = resolve(currentDir, "..");

function formatTypeScript(value: string): string {
  return execFileSync("pnpm", ["exec", "oxfmt", "--stdin-filepath", "schema.ts"], {
    cwd: projectRoot,
    input: value,
    encoding: "utf8",
  });
}

describe("DDL introspection fixtures", () => {
  for (const fixtureName of readdirSync(fixturesDir).sort()) {
    it(`generates Quarry schema for ${fixtureName}`, () => {
      const fixtureDir = resolve(fixturesDir, fixtureName);
      const input = readFileSync(resolve(fixtureDir, "schema.sql"), "utf8");
      const expected = readFileSync(resolve(fixtureDir, "expected.ts"), "utf8");

      expect(formatTypeScript(generateSchemaModuleFromDDL(input))).toBe(formatTypeScript(expected));
    });
  }

  it("omits default clauses while preserving the column type", () => {
    expect(
      formatTypeScript(
        generateSchemaModuleFromDDL(`
          CREATE TABLE default.users (
            id UInt32,
            created_at DateTime64(3) DEFAULT now64(3)
          )
          ENGINE = MergeTree
          ORDER BY id;
        `),
      ),
    ).toBe(
      formatTypeScript(`
        import { DateTime64, defineSchema, table, UInt32 } from "@oorestisime/quarry";

        export const schema = defineSchema({
          users: table.mergeTree(
            {
              id: UInt32(),
              created_at: DateTime64(3),
            },
            {
              orderBy: ["id"],
            },
          ),
        });
      `),
    );
  });

  it("rejects unsupported table clauses instead of misparsing them", () => {
    expect(() =>
      generateSchemaModuleFromDDL(`
        CREATE TABLE default.sampled_events (
          id UInt32,
          created_at DateTime64(3)
        )
        ENGINE = MergeTree
        ORDER BY id
        SAMPLE BY id;
      `),
    ).toThrow("Unsupported table clause 'SAMPLE BY' in table 'sampled_events'.");
  });

  it("rejects unsupported view functions instead of generating invalid Quarry code", () => {
    expect(() =>
      generateSchemaModuleFromDDL(`
        CREATE TABLE default.users (
          id UInt32,
          email String
        )
        ENGINE = Memory;

        CREATE VIEW default.user_labels AS
        SELECT
          id,
          hex(id) AS id_hex
        FROM default.users;
      `),
    ).toThrow("Unsupported view function 'hex' in view 'user_labels'.");
  });
});
