import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const entry = resolve(process.argv[2] ?? join(root, "packages/core/dist/index.mjs"));
// Native TS 7 has no compatible language-service API; exercise the classic API.
const ts = createRequire(import.meta.url)(process.argv[3] ?? "typescript");
const temporary = await mkdtemp(join(tmpdir(), "quarry-editor-"));
const header = `import {createClickHouseDB,type InferResult,type TypedTable,type TypedView} from ${JSON.stringify(entry)};
interface DB {
  users: TypedTable<{id:number;email:string}>;
  events: {user_id:number;event_type:string};
  archive: TypedView<{id:number}>;
}
const db=createClickHouseDB<DB>();
`;
const chain =
  'db.selectFrom("users as u")' +
  Array.from(
    { length: 10 },
    (_, index) => `.innerJoin("events as e${index}","u.id","e${index}.user_id")`,
  ).join("");
const results = {};
try {
  for (const [name, code, expected] of [
    ["tables", 'db.selectFrom("/*cursor*/")', ["users", "events", "archive"]],
    ["joinSources", 'db.selectFrom("users as u").innerJoin("/*cursor*/")', ["users", "events"]],
    [
      "joinedColumns",
      'db.selectFrom("users as u").innerJoin("events as e","u.id","e.user_id").select("/*cursor*/")',
      ["u.id", "u.email", "e.user_id", "e.event_type"],
    ],
    [
      "joinCallback",
      'db.selectFrom("users as u").innerJoin("events as e", eb=>eb.ref("/*cursor*/"))',
      ["u.id", "e.user_id"],
    ],
    [
      "scopedTables",
      'createClickHouseDB<Pick<DB,"users"|"events">>().selectFrom("/*cursor*/")',
      ["users", "events"],
    ],
    ["chainedColumns", `${chain}.select("/*cursor*/")`, ["u.email", "e0.event_type", "e9.user_id"]],
    [
      "cteColumns",
      'db.with("recent", db=>db.selectFrom("users").select("id","email")).selectFrom("recent as r").select("/*cursor*/")',
      ["r.id", "r.email"],
    ],
    [
      "selectionAlias",
      'db.selectFrom("users as u").select("u.email as label").orderBy("/*cursor*/")',
      ["label", "u.id"],
    ],
    [
      "resultHover",
      'const query=db.selectFrom("users as u").leftJoinNullable("events as e","u.id","e.user_id").select("u.email","e.event_type");\ndeclare const row: InferResult<typeof query>;\nconst /*cursor*/value = row.event_type;',
      "const value: string | null",
    ],
  ]) {
    const raw = header + code;
    const position = raw.indexOf("/*cursor*/");
    const file = join(temporary, `${name}.ts`);
    await writeFile(file, raw.replace("/*cursor*/", ""));
    const options = {
      strict: true,
      noEmit: true,
      skipLibCheck: true,
      types: [],
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.ESNext,
      moduleResolution: ts.ModuleResolutionKind.Bundler,
    };
    const host = {
      getScriptFileNames: () => [file],
      getScriptVersion: () => "0",
      getScriptSnapshot: (path) => {
        const source = ts.sys.readFile(path);
        return source === undefined ? undefined : ts.ScriptSnapshot.fromString(source);
      },
      getCurrentDirectory: () => temporary,
      getCompilationSettings: () => options,
      getDefaultLibFileName: ts.getDefaultLibFilePath,
      fileExists: ts.sys.fileExists,
      readFile: ts.sys.readFile,
      readDirectory: ts.sys.readDirectory,
    };
    const service = ts.createLanguageService(host);
    try {
      if (Array.isArray(expected)) {
        const entries =
          service
            .getCompletionsAtPosition(file, position, {})
            ?.entries.map((entry) => entry.name)
            .sort() ?? [];
        for (const value of expected)
          assert.ok(entries.includes(value), `${name}: missing completion ${value}`);
        if (name === "scopedTables")
          assert.ok(!entries.includes("archive"), "Scoped schema must exclude archive");
        results[name] = entries;
      } else {
        assert.equal(
          service.getSemanticDiagnostics(file).length,
          0,
          "Result hover fixture must typecheck",
        );
        const display = ts.displayPartsToString(
          service.getQuickInfoAtPosition(file, position)?.displayParts,
        );
        assert.equal(display, expected);
        results[name] = display;
      }
    } finally {
      service.dispose();
    }
  }
  console.log(JSON.stringify(results, null, 2));
} finally {
  await rm(temporary, { recursive: true, force: true });
}
