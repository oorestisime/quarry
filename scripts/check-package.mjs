import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const compilers = {
  5.9: "docs/node_modules/typescript/bin/tsc",
  6: "node_modules/typescript/bin/tsc",
  7: "node_modules/typescript-native/bin/tsc",
};
const version = process.argv[2] ?? "6";
if (!compilers[version]) throw new Error("Choose compiler 5.9, 6, or 7.");
const temporary = await mkdtemp(join(tmpdir(), "quarry-package-"));
try {
  if (
    (await readFile(join(root, "README.md"), "utf8")) !==
    (await readFile(join(root, "packages/core/README.md"), "utf8"))
  )
    throw new Error("Root and published READMEs must match.");
  const packed = JSON.parse(
    execFileSync("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", temporary], {
      cwd: join(root, "packages/core"),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "inherit"],
    }),
  )[0];
  for (const required of [
    "dist/index.mjs",
    "dist/index.d.mts",
    "dist/index.d.mts.map",
    "src/index.ts",
    "README.md",
    "LICENSE",
  ]) {
    if (!packed.files.some((file) => file.path === required))
      throw new Error(`Package is missing ${required}`);
  }
  await writeFile(
    join(temporary, "package.json"),
    JSON.stringify({ private: true, type: "module" }),
  );
  execFileSync(
    "npm",
    ["install", "--ignore-scripts", "--no-audit", "--no-fund", join(temporary, packed.filename)],
    { cwd: temporary, stdio: "inherit" },
  );
  const source = await readFile(join(root, "packages/core/test/adoption.typecheck.ts"), "utf8");
  await writeFile(join(temporary, "consumer.ts"), source.replace('from "../src"', 'from "quarry"'));
  for (const [module, moduleResolution] of [
    ["NodeNext", "NodeNext"],
    ["ESNext", "Bundler"],
  ]) {
    await writeFile(
      join(temporary, "tsconfig.json"),
      JSON.stringify({
        compilerOptions: {
          strict: true,
          noEmit: true,
          skipLibCheck: false,
          target: "ES2022",
          module,
          moduleResolution,
          types: [],
        },
        files: ["consumer.ts"],
      }),
    );
    execFileSync(
      process.execPath,
      [resolve(root, compilers[version]), "-p", join(temporary, "tsconfig.json")],
      { stdio: "inherit" },
    );
  }
  await writeFile(
    join(temporary, "runtime.mjs"),
    `import {createClickHouseDB,sql} from "quarry";
const query=createClickHouseDB().selectFrom("events").select(sql\`\${42}\`.as("value")).toSQL();
if(query.params.p0!==42) throw new Error("Packed runtime parameter binding failed");`,
  );
  execFileSync(process.execPath, [join(temporary, "runtime.mjs")], { stdio: "inherit" });
  const mapPath = join(temporary, "node_modules/quarry/dist/index.d.mts.map");
  const map = JSON.parse(await readFile(mapPath, "utf8"));
  for (const sourcePath of map.sources) {
    await readFile(resolve(mapPath, "..", map.sourceRoot ?? "", sourcePath));
  }
  console.log(
    `Packed package: TypeScript ${version}, NodeNext, Bundler, runtime imports, and declaration maps passed.`,
  );
} finally {
  await rm(temporary, { recursive: true, force: true });
}
