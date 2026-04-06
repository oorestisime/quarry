import { createClient } from "@clickhouse/client";
import { createClickHouseDB, param } from "@oorestisime/quarry";
import { GenericContainer, Wait } from "testcontainers";

interface DB {
  event_logs: {
    user_id: number;
    event_type: string;
    created_at: string;
  };
  users: {
    id: number;
    email: string;
    status: string;
  };
}

const CLICKHOUSE_PORT = 8123;
const CLICKHOUSE_USER = "test";
const CLICKHOUSE_PASSWORD = "test";

const container = await new GenericContainer("clickhouse/clickhouse-server:24.8")
  .withEnvironment({
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1",
  })
  .withExposedPorts(CLICKHOUSE_PORT)
  .withWaitStrategy(Wait.forHttp("/ping", CLICKHOUSE_PORT))
  .withStartupTimeout(120_000)
  .start();

const client = createClient({
  url: `http://${container.getHost()}:${container.getMappedPort(CLICKHOUSE_PORT)}`,
  username: CLICKHOUSE_USER,
  password: CLICKHOUSE_PASSWORD,
  request_timeout: 30_000,
});

const db = createClickHouseDB<DB>({ client });

try {
  await client.command({
    query: `
      CREATE TABLE users (
        id UInt32,
        email String,
        status String
      )
      ENGINE = Memory
    `,
  });

  await client.command({
    query: `
      CREATE TABLE event_logs (
        user_id UInt32,
        event_type String,
        created_at DateTime
      )
      ENGINE = Memory
    `,
  });

  await db.insertInto("users").values([
    { id: 1, email: "alice@example.com", status: "active" },
    { id: 2, email: "bruno@example.com", status: "inactive" },
  ]).execute();

  await db.insertInto("event_logs").values([
    { user_id: 1, event_type: "signup", created_at: "2025-01-01 10:00:00" },
    { user_id: 1, event_type: "purchase", created_at: "2025-01-02 11:00:00" },
    { user_id: 2, event_type: "signup", created_at: "2025-01-03 12:00:00" },
  ]).execute();

  const compiled = db
    .selectFrom("users as u")
    .innerJoin("event_logs as e", "u.id", "e.user_id")
    .select("u.id", "u.email", "e.event_type")
    .where("e.created_at", ">=", param("2025-01-01 00:00:00", "DateTime"))
    .where("u.status", "=", "active")
    .orderBy("u.id", "asc")
    .toSQL();

  console.log("Compiled SQL:", compiled.query);
  console.log("Params:", compiled.params);

  const rows = await db
    .selectFrom("users as u")
    .innerJoin("event_logs as e", "u.id", "e.user_id")
    .select("u.id", "u.email", "e.event_type")
    .where("e.created_at", ">=", param("2025-01-01 00:00:00", "DateTime"))
    .where("u.status", "=", "active")
    .orderBy("u.id", "asc")
    .execute();

  console.log("Joined rows:");
  console.table(rows);

  const grouped = await db
    .selectFrom("event_logs as e")
    .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
    .groupBy("e.user_id")
    .having("event_count", ">", param(1, "Int64"))
    .orderBy("event_count", "desc")
    .execute();

  console.log("Grouped rows:");
  console.table(grouped);
} finally {
  await client.close();
  await container.stop();
}
