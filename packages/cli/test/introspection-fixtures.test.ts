import { execFileSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
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

  it("preserves default clauses while generating schema output", () => {
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
        import { DateTime64, defineSchema, table, type SchemaBuilder, UInt32 } from "@oorestisime/quarry";

        const tables = {
          users: table.mergeTree(
            {
              id: UInt32(),
              created_at: DateTime64(3).defaultSql("now64(3)"),
            },
            {
              orderBy: ["id"],
            },
          ),
        };

        export const schema: SchemaBuilder<typeof tables> = defineSchema(tables);
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

  it("preserves boolean literals in generated views", () => {
    const source = formatTypeScript(
      generateSchemaModuleFromDDL(`
        CREATE TABLE default.users (
          id UInt32,
          is_active Bool
        )
        ENGINE = Memory;

        CREATE VIEW default.active_user_labels AS
        SELECT
          id,
          nullIf(is_active, true) AS maybe_active
        FROM default.users
        WHERE is_active = false;
      `),
    );

    expect(source).toContain('eb.fn.nullIf("is_active", true).as("maybe_active")');
    expect(source).toContain('.where("is_active", "=", false)');
  });

  it("supports declaration emit for large generated schemas", () => {
    const tempDir = mkdtempSync(resolve(projectRoot, ".tmp-introspect-types-"));

    try {
      const ddl = [
        ...Array.from(
          { length: 400 },
          (_, index) => `
          CREATE TABLE default.generated_${index} (
            id UInt32
          )
          ENGINE = MergeTree
          ORDER BY id;
        `,
        ),
        `
          CREATE VIEW default.generated_labels AS
          SELECT id
          FROM default.generated_0;
        `,
      ].join("\n");

      const schemaFile = resolve(tempDir, "schema.ts");
      const tsconfigFile = resolve(tempDir, "tsconfig.json");

      writeFileSync(schemaFile, generateSchemaModuleFromDDL(ddl), "utf8");
      writeFileSync(
        tsconfigFile,
        JSON.stringify(
          {
            compilerOptions: {
              target: "ES2022",
              module: "ESNext",
              moduleResolution: "Bundler",
              strict: true,
              skipLibCheck: true,
              declaration: true,
              emitDeclarationOnly: true,
              outDir: "./dist",
            },
            include: ["./schema.ts"],
          },
          null,
          2,
        ),
        "utf8",
      );

      execFileSync("pnpm", ["--filter", "@oorestisime/quarry", "build"], {
        cwd: resolve(projectRoot, "..", ".."),
        stdio: "pipe",
      });

      try {
        execFileSync("pnpm", ["exec", "tsc", "-p", tsconfigFile], {
          cwd: projectRoot,
          stdio: "pipe",
        });
      } catch (error) {
        const message =
          error instanceof Error && "stdout" in error && "stderr" in error
            ? `${String(error.stdout)}${String(error.stderr)}`
            : String(error);
        throw new Error(message);
      }
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });
});
