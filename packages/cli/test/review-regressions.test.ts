import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";
import { buildTypeScriptModuleResult } from "../src/introspect";

// Compile a consumer of freshly generated types, without depending on a built package.
async function checkGeneratedConsumer(schema: string, consumer: string): Promise<string[]> {
  const directory = await mkdtemp(join(tmpdir(), "quarry-review-types-"));
  try {
    const consumerPath = join(directory, "consumer.ts");
    await writeFile(join(directory, "generated.ts"), schema);
    await writeFile(consumerPath, consumer);

    const program = ts.createProgram([consumerPath], {
      noEmit: true,
      strict: true,
      skipLibCheck: true,
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.ESNext,
      moduleResolution: ts.ModuleResolutionKind.Bundler,
      types: [],
      paths: {
        quarry: [fileURLToPath(new URL("../../core/src/index.ts", import.meta.url))],
      },
    });

    return ts
      .getPreEmitDiagnostics(program)
      .map((diagnostic) => ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n"));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

describe("introspection review regressions", () => {
  // Finding 7: native JSON reads and inserts use objects, not strings.
  it.each(["JSON", "JSON(max_dynamic_paths=1024, key String)"])(
    "generates object types for %s",
    async (clickhouseType) => {
      const result = buildTypeScriptModuleResult(
        [{ name: "events", engine: "Memory" }],
        [{ objectName: "events", name: "payload", clickhouseType, position: 1 }],
      );

      const diagnostics = await checkGeneratedConsumer(
        result.source,
        `
        import { createClickHouseDB, type Selectable } from "quarry";
        import type { DB } from "./generated";

        const db = createClickHouseDB<DB>();
        const row: Selectable<DB["events"]> = { payload: { key: "value" } };
        db.insertInto("events").values([{ payload: { key: "value" } }]);

        // @ts-expect-error a JSON object is not a string
        row.payload.toUpperCase();
      `,
      );

      expect(diagnostics).toEqual([]);
    },
  );

  // Finding 9: the row interface must not merge with the generated Tables map.
  it("keeps a table named tables separate from the generated schema interface", async () => {
    const result = buildTypeScriptModuleResult(
      [{ name: "tables", engine: "Memory" }],
      [{ objectName: "tables", name: "id", clickhouseType: "UInt32", position: 1 }],
    );

    const diagnostics = await checkGeneratedConsumer(
      result.source,
      `
        import { createClickHouseDB } from "quarry";
        import type { DB } from "./generated";

        const db = createClickHouseDB<DB>();
        db.selectFrom("tables").select("id");
        db.insertInto("tables").values([{ id: 1 }]);
      `,
    );

    expect(diagnostics).toEqual([]);
  });

  it("allocates unique row names across schema maps, imports, and numeric suffixes", async () => {
    const names = [
      "tables",
      "views",
      "dictionaries",
      "DB",
      "generated",
      "typed_table",
      "foo-bar",
      "foo_bar",
      "foo_bar2",
      "imported_row",
      "array",
      "record",
    ];
    const result = buildTypeScriptModuleResult(
      names.map((name) => ({ name, engine: "Memory" })),
      names.map((name, index) => ({
        objectName: name,
        name: `field${index}`,
        clickhouseType: name === "array" ? "Array(UInt32)" : name === "record" ? "JSON" : "UInt32",
        position: 1,
      })),
      undefined,
      undefined,
      { imports: [{ from: "quarry", name: "Expression", as: "ImportedRow" }] },
    );
    const diagnostics = await checkGeneratedConsumer(
      result.source,
      `
      import { createClickHouseDB } from "quarry";
      import type { DB } from "./generated";
      const db = createClickHouseDB<DB>();
      ${names.map((name, index) => `db.insertInto(${JSON.stringify(name)}).values([{ field${index}: ${name === "array" ? "[1]" : name === "record" ? "{ key: 1 }" : "1"} }]);`).join("\n")}
    `,
    );
    expect(diagnostics).toEqual([]);
  });
});
