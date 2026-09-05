import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, symlink, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const temporary = await mkdtemp(join(tmpdir(), "quarry-types-"));
const compiler = join(
  root,
  process.argv.includes("--native")
    ? "node_modules/typescript-native/bin/tsc"
    : "node_modules/typescript/bin/tsc",
);
const native = process.argv.includes("--native");
try {
  await mkdir(join(temporary, "node_modules"));
  await symlink(join(root, "packages/core"), join(temporary, "node_modules/quarry"), "dir");
  const large = (
    await readFile(join(root, "packages/core/test/performance/large-schema.typecheck.ts"), "utf8")
  ).replace('from "../../src"', 'from "quarry"');
  let wide = 'import {createClickHouseDB,type InferResult} from "quarry";\ninterface DB {\n';
  for (let table = 0; table < 1000; table++) {
    wide += `table_${table}: {id:number;tag:"t${table}";name:string;tags:string[];`;
    for (let col = 0; col < 40; col++) wide += `col_${col}:${col % 2 ? "number" : "string"};`;
    wide += "};\n";
  }
  wide += "}\nconst db=createClickHouseDB<DB>();\n";
  for (let query = 0; query < 10; query++)
    wide += `const q${query}=db.selectFrom("table_${query} as a").innerJoin("table_${query + 1} as b","a.id","b.id").select("a.id","b.name").where("a.id","=",1);\nconst r${query}:InferResult<typeof q${query}>={id:1,name:"a"};\n`;
  const scoped = wide.replace(
    "createClickHouseDB<DB>()",
    `createClickHouseDB<Pick<DB, ${Array.from({ length: 11 }, (_, index) => JSON.stringify(`table_${index}`)).join(" | ")}>>()`,
  );
  let chains =
    'import {createClickHouseDB,type InferResult} from "quarry";\nconst db=createClickHouseDB<{events:{id:number;tags:string[];name:string}}>();\n';
  chains += 'const joined=db.selectFrom("events as e0")';
  for (let index = 1; index <= 20; index++)
    chains += `.innerJoin("events as e${index}","e0.id","e${index}.id").select("e${index}.name as name${index}")`;
  chains +=
    ";\nconst row:InferResult<typeof joined> = " +
    JSON.stringify(
      Object.fromEntries(Array.from({ length: 20 }, (_, i) => [`name${i + 1}`, "x"])),
    ) +
    ";\n";
  chains += "const ctes=db";
  for (let index = 0; index < 10; index++)
    chains += `.with("c${index}",db=>db.selectFrom("${index === 0 ? "events" : `c${index - 1}`} as e").select("e.id","e.name","e.tags"))`;
  chains += '.selectFrom("c9").arrayJoin("tags").select("id","tags");\n';
  chains += 'const output:InferResult<typeof ctes>={id:1,tags:"a"};\n';
  const results = [];
  // Counts are pinned to TS 6.0.2. Wall time is reported, not gated on noisy CI hosts.
  for (const [name, source, budget] of [
    ["schema710", large, 52000],
    ["heterogeneous1000", wide, 150000],
    ["serviceSchema", scoped, 75000],
    ["chains", chains, 165000],
  ]) {
    await writeFile(join(temporary, "consumer.ts"), source);
    await writeFile(
      join(temporary, "tsconfig.json"),
      JSON.stringify({
        compilerOptions: {
          strict: true,
          noEmit: true,
          skipLibCheck: true,
          module: "ESNext",
          moduleResolution: "Bundler",
          target: "ES2022",
          types: [],
        },
        files: ["consumer.ts"],
      }),
    );
    const output = execFileSync(
      process.execPath,
      [compiler, "-p", join(temporary, "tsconfig.json"), "--extendedDiagnostics"],
      { encoding: "utf8" },
    );
    const instantiations = Number(output.match(/^Instantiations:\s+(\d+)/m)?.[1]);
    if (!Number.isFinite(instantiations)) throw new Error("Compiler did not report instantiations");
    const total = Number(output.match(/^Total time:\s+([\d.]+)/m)?.[1]);
    console.log(
      `${name}: ${instantiations} instantiations, ${total}s total (${native ? "TS 7" : "TS 6"})`,
    );
    if (!native && instantiations > budget)
      throw new Error(
        `${name} exceeded its ${budget} instantiation budget; profile and review the change before adjusting it.`,
      );
    results.push({ name, instantiations, totalSeconds: total });
  }
  if (process.env.GITHUB_STEP_SUMMARY) {
    const { appendFile } = await import("node:fs/promises");
    await appendFile(
      process.env.GITHUB_STEP_SUMMARY,
      `\nTypeScript consumer performance (${native ? "7" : "6"})\n\n|Fixture|Instantiations|Total seconds|\n|---|---:|---:|\n${results.map((result) => `|${result.name}|${result.instantiations}|${result.totalSeconds}|`).join("\n")}\n`,
    );
  }
} finally {
  await rm(temporary, { recursive: true, force: true });
}
