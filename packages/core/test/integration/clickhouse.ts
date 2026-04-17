import { createClient } from "@clickhouse/client";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";

const CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.8";
const CLICKHOUSE_PORT = 8123;
const CLICKHOUSE_USERNAME = "test";
const CLICKHOUSE_PASSWORD = "test";

const usersFixture = [
  { id: 1, email: "alice@example.com", status: "active" },
  { id: 2, email: "bruno@example.com", status: "active" },
  { id: 3, email: "cory@example.com", status: "inactive" },
  ...Array.from({ length: 40 }, (_, index) => {
    const id = index + 4;

    return {
      id,
      email: `user${id}@example.com`,
      status: "active",
    };
  }),
];

const eventLogsFixture = [
  {
    user_id: 1,
    event_type: "signup",
    created_at: "2025-01-01 10:00:00",
    event_date: "2025-01-01",
    properties: '{"source":"organic"}',
    version: 1,
  },
  {
    user_id: 2,
    event_type: "purchase",
    created_at: "2025-01-02 09:00:00",
    event_date: "2025-01-02",
    properties: '{"source":"paid-search"}',
    version: 1,
  },
  {
    user_id: 3,
    event_type: "signup",
    created_at: "2025-01-03 08:00:00",
    event_date: "2025-01-03",
    properties: '{"source":"paid-search"}',
    version: 1,
  },
  {
    user_id: 1,
    event_type: "browse",
    created_at: "2024-12-31 23:00:00",
    event_date: "2024-12-31",
    properties: '{"source":"referral"}',
    version: 1,
  },
];

const inquiryDownloadsFixture = [
  {
    user_id: 1,
    created_at: "2025-01-01 08:00:00",
    version: 1,
  },
  {
    user_id: 1,
    created_at: "2025-01-02 08:00:00",
    version: 1,
  },
  {
    user_id: 2,
    created_at: "2025-01-02 12:00:00",
    version: 1,
  },
  {
    user_id: 2,
    created_at: "2024-12-31 23:00:00",
    version: 1,
  },
];

const typedSamplesFixture = [
  {
    id: 1,
    big_user_id: "9007199254740993",
    label: "alpha",
    status: "active",
    nickname: null,
    tags: ["new", "trial"],
    amount: "123.45",
    created_at: "2025-01-01 10:11:12.123",
    location: [1.25, 9.5],
    attributes: { source: "ads", campaign: "winter" },
    "metrics.name": ["clicks", "opens"],
    "metrics.score": [10, 5],
  },
  {
    id: 2,
    big_user_id: "42",
    label: "beta",
    status: "pending",
    nickname: "bee",
    tags: [],
    amount: "0.10",
    created_at: "2025-01-02 03:04:05.678",
    location: [0, 1.5],
    attributes: { source: "email" },
    "metrics.name": ["views"],
    "metrics.score": [99],
  },
];

const jsonSamplesFixture = [
  {
    id: 1,
    payload: {
      user: { id: "9007199254740993" },
      traits: { plan: "pro", active: true },
      tags: ["new", "trial"],
      metrics: { score: 12.5, rank: 3 },
    },
  },
  {
    id: 2,
    payload: {
      user: { id: "42" },
      traits: { plan: "free", active: false },
      tags: [],
      metrics: { score: 0.25, rank: 8 },
    },
  },
];

export interface ClickHouseTestContext {
  client: ReturnType<typeof createClient>;
  container: StartedTestContainer;
}

export async function startClickHouse(): Promise<ClickHouseTestContext> {
  const container = await new GenericContainer(CLICKHOUSE_IMAGE)
    .withEnvironment({
      CLICKHOUSE_USER: CLICKHOUSE_USERNAME,
      CLICKHOUSE_PASSWORD: CLICKHOUSE_PASSWORD,
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1",
    })
    .withExposedPorts(CLICKHOUSE_PORT)
    .withWaitStrategy(Wait.forHttp("/ping", CLICKHOUSE_PORT))
    .withStartupTimeout(120_000)
    .start();

  const client = createClient({
    url: `http://${container.getHost()}:${container.getMappedPort(CLICKHOUSE_PORT)}`,
    request_timeout: 30_000,
    username: CLICKHOUSE_USERNAME,
    password: CLICKHOUSE_PASSWORD,
    clickhouse_settings: {
      allow_experimental_json_type: 1,
      date_time_input_format: "best_effort",
    },
  });

  await client.ping({ select: true });
  await resetFixtureSchema(client);

  return { client, container };
}

export async function stopClickHouse(context: ClickHouseTestContext | undefined): Promise<void> {
  if (!context) {
    return;
  }

  await context.client.close();
  await context.container.stop();
}

async function resetFixtureSchema(client: ReturnType<typeof createClient>): Promise<void> {
  await client.command({ query: "DROP TABLE IF EXISTS event_logs" });
  await client.command({ query: "DROP TABLE IF EXISTS inquiry_downloads" });
  await client.command({ query: "DROP TABLE IF EXISTS json_samples" });
  await client.command({ query: "DROP TABLE IF EXISTS typed_samples" });
  await client.command({ query: "DROP TABLE IF EXISTS users" });

  await client.command({
    query: `
			CREATE TABLE event_logs (
				user_id UInt32,
				event_type String,
				created_at DateTime,
				event_date Date,
				properties String,
				version UInt32
			)
			ENGINE = ReplacingMergeTree(version)
			ORDER BY (user_id, created_at, event_type)
		`,
  });

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
			CREATE TABLE inquiry_downloads (
				user_id UInt32,
				created_at DateTime,
				version UInt32
			)
			ENGINE = ReplacingMergeTree(version)
			ORDER BY (user_id, created_at)
		`,
  });

  await client.command({
    query: `
			CREATE TABLE typed_samples (
				id UInt32,
				big_user_id UInt64,
				label LowCardinality(String),
				status Enum8('pending' = 1, 'active' = 2, 'archived' = 3),
				nickname Nullable(String),
				tags Array(String),
				amount Decimal(18, 2),
				created_at DateTime64(3),
				location Tuple(Float64, Float64),
				attributes Map(String, String),
				metrics Nested(name String, score UInt32)
			)
			ENGINE = Memory
		`,
  });

  await client.command({
    query: `
			CREATE TABLE json_samples (
				id UInt32,
				payload JSON(
					user.id UInt64,
					traits.plan String,
					traits.active Bool,
					tags Array(String),
					metrics.score Float64,
					metrics.rank UInt32
				)
			)
			ENGINE = Memory
		`,
  });

  await client.insert({
    table: "users",
    values: usersFixture,
    format: "JSONEachRow",
  });

  await client.insert({
    table: "event_logs",
    values: eventLogsFixture,
    format: "JSONEachRow",
  });

  await client.insert({
    table: "inquiry_downloads",
    values: inquiryDownloadsFixture,
    format: "JSONEachRow",
  });

  await client.insert({
    table: "typed_samples",
    values: typedSamplesFixture,
    format: "JSONEachRow",
  });

  await client.insert({
    table: "json_samples",
    values: jsonSamplesFixture,
    format: "JSONEachRow",
  });
}
