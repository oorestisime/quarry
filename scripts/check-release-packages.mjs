import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createClient } from "@clickhouse/client";
import { GenericContainer, Wait } from "testcontainers";

const root = fileURLToPath(new URL("../", import.meta.url));
const temporary = await mkdtemp(join(tmpdir(), "quarry-release-"));
const run = (command, args) =>
  execFileSync(command, args, { cwd: temporary, stdio: "inherit", timeout: 120_000 });
let container;
let client;
try {
  const tarballs = [];
  const versions = [];
  for (const name of ["core", "cli"]) {
    const directory = join(root, "packages", name);
    const manifest = JSON.parse(await readFile(join(directory, "package.json"), "utf8"));
    versions.push(manifest.version);
    const [packed] = JSON.parse(
      execFileSync("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", temporary], {
        cwd: directory,
        encoding: "utf8",
        timeout: 120_000,
      }),
    );
    tarballs.push(join(temporary, packed.filename));
  }
  await writeFile(
    join(temporary, "package.json"),
    JSON.stringify({ private: true, type: "module" }),
  );
  run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", ...tarballs]);
  for (const [index, name] of ["quarry", "@oorestisime/quarry-cli"].entries()) {
    const installed = JSON.parse(
      await readFile(join(temporary, "node_modules", name, "package.json"), "utf8"),
    );
    assert.equal(installed.version, versions[index]);
  }
  const executable = join(temporary, "node_modules/.bin/quarry");
  run(executable, ["introspect", "--help"]);

  container = await new GenericContainer(
    process.env.CLICKHOUSE_IMAGE ?? "clickhouse/clickhouse-server:25.8",
  )
    .withEnvironment({ CLICKHOUSE_USER: "test", CLICKHOUSE_PASSWORD: "test" })
    .withExposedPorts(8123)
    .withWaitStrategy(Wait.forHttp("/ping", 8123))
    .withStartupTimeout(120_000)
    .start();
  const connection = {
    url: `http://${container.getHost()}:${container.getMappedPort(8123)}`,
    username: "test",
    password: "test",
  };
  client = createClient(connection);
  await client.command({
    query: `CREATE TABLE events (
      id UInt32,
      user_id UInt64,
      label String DEFAULT 'default',
      doubled UInt32 MATERIALIZED id * 2,
      alias_id UInt32 ALIAS id
    ) ENGINE = Memory`,
  });
  await writeFile(
    join(temporary, "quarry.introspect.json"),
    JSON.stringify({
      url: connection.url,
      user: connection.username,
      password: connection.password,
      database: "default",
      out: "db.ts",
    }),
  );
  run(executable, ["introspect", "--config", "quarry.introspect.json"]);
  const schema = await readFile(join(temporary, "db.ts"), "utf8");
  assert.match(schema, /Generated</);
  assert.match(schema, /GeneratedAlways</);
  await writeFile(
    join(temporary, "consumer.ts"),
    `import { createClickHouseDB, type InferResult } from "quarry";
import type { DB } from "./db.js";
export const db = createClickHouseDB<DB>();
export const insert = db.insertInto("events").values([{ id: 2, user_id: "9007199254740993" }]);
export const query = db.selectFrom("events").select("id", "user_id", "label", "doubled", "alias_id").orderBy("id");
const expected: InferResult<typeof query> = { id: 2, user_id: "9007199254740993", label: "default", doubled: 4, alias_id: 2 };
function invalidGeneratedInsert() {
  // @ts-expect-error server-generated columns cannot be supplied
  db.insertInto("events").values([{ id: 2, user_id: "1", doubled: 4 }]);
}
function invalidResult() {
  // @ts-expect-error UInt64 results must preserve their string representation
  const invalid: InferResult<typeof query> = { ...expected, user_id: 9007199254740993 };
}
`,
  );
  await writeFile(
    join(temporary, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        strict: true,
        skipLibCheck: false,
        target: "ES2022",
        module: "NodeNext",
        moduleResolution: "NodeNext",
        types: [],
        outDir: "dist",
      },
      files: ["consumer.ts", "db.ts"],
    }),
  );
  for (const compiler of [
    "docs/node_modules/typescript/bin/tsc",
    "node_modules/typescript/bin/tsc",
    "node_modules/typescript-native/bin/tsc",
  ]) {
    run(process.execPath, [join(root, compiler), "-p", "tsconfig.json", "--noEmit"]);
  }
  run(process.execPath, [join(root, "node_modules/typescript/bin/tsc"), "-p", "tsconfig.json"]);
  await writeFile(
    join(temporary, "runtime.mjs"),
    `import assert from "node:assert/strict";
import { createClient } from "@clickhouse/client";
import { insert, query } from "./dist/consumer.js";
const client = createClient(${JSON.stringify(connection)});
try {
  await insert.execute({ client });
  assert.deepEqual(await query.executeTakeFirstOrThrow({ client }), {
    id: 2, user_id: "9007199254740993", label: "default", doubled: 4, alias_id: 2,
  });
} finally { await client.close(); }
`,
  );
  run(process.execPath, ["runtime.mjs"]);
  console.log(
    `Core ${versions[0]} / CLI ${versions[1]}: fresh installation, generated schema on TS 5.9/6/7, and ClickHouse insert/select passed.`,
  );
} finally {
  try {
    await client?.close();
  } finally {
    try {
      await container?.stop();
    } finally {
      await rm(temporary, { recursive: true, force: true });
    }
  }
}
