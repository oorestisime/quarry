import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  Date32,
  Decimal,
  FixedString,
  IPv4,
  IPv6,
  Int8,
  Int16,
  LowCardinality,
  String,
  UInt8,
  UInt16,
  UUID,
  createClickHouseDB,
  defineSchema,
  table,
} from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

const schema = defineSchema({
  schema_scalar_samples: table({
    tiny_u8: UInt8(),
    small_u16: UInt16(),
    tiny_i8: Int8(),
    small_i16: Int16(),
    amount_d12_6: Decimal(12, 6),
    event_date32: Date32(),
    code: FixedString(8),
    account_uuid: UUID(),
    client_ipv4: IPv4(),
    client_ipv6: IPv6(),
    category: LowCardinality(String()),
  }),
});

const db = createClickHouseDB({ schema });

let context: ClickHouseTestContext | undefined;

describe("schema tier one scalar types", () => {
  function getContext(): ClickHouseTestContext {
    if (!context) {
      throw new Error("ClickHouse test context was not initialized");
    }

    return context;
  }

  beforeAll(async () => {
    context = await startClickHouse();

    await getContext().client.command({ query: "DROP TABLE IF EXISTS schema_scalar_samples" });
    await getContext().client.command({
      query: `
        CREATE TABLE schema_scalar_samples (
          tiny_u8 UInt8,
          small_u16 UInt16,
          tiny_i8 Int8,
          small_i16 Int16,
          amount_d12_6 Decimal(12, 6),
          event_date32 Date32,
          code FixedString(8),
          account_uuid UUID,
          client_ipv4 IPv4,
          client_ipv6 IPv6,
          category LowCardinality(String)
        )
        ENGINE = Memory
      `,
    });
  });

  beforeEach(async () => {
    await getContext().client.command({ query: "TRUNCATE TABLE IF EXISTS schema_scalar_samples" });
  });

  afterAll(async () => {
    await stopClickHouse(context);
  });

  it("round-trips the low-risk schema scalar types with runtime-honest values", async () => {
    await db
      .insertInto("schema_scalar_samples")
      .values([
        {
          tiny_u8: 7,
          small_u16: 512,
          tiny_i8: -3,
          small_i16: -1024,
          amount_d12_6: "123.456789",
          event_date32: new Date("2025-01-01T00:00:00.000Z"),
          code: "ABCDEFGH",
          account_uuid: "550e8400-e29b-41d4-a716-446655440000",
          client_ipv4: "127.0.0.1",
          client_ipv6: "::1",
          category: "premium",
        },
      ])
      .execute(getContext().client);

    const rows = await db
      .selectFrom("schema_scalar_samples as s")
      .selectAll()
      .where("s.event_date32", ">=", new Date("2025-01-01T00:00:00.000Z"))
      .execute(getContext().client);

    expect(rows).toEqual([
      {
        tiny_u8: 7,
        small_u16: 512,
        tiny_i8: -3,
        small_i16: -1024,
        amount_d12_6: 123.456789,
        event_date32: "2025-01-01",
        code: "ABCDEFGH",
        account_uuid: "550e8400-e29b-41d4-a716-446655440000",
        client_ipv4: "127.0.0.1",
        client_ipv6: "::1",
        category: "premium",
      },
    ]);

    expect(typeof rows[0].tiny_u8).toBe("number");
    expect(typeof rows[0].small_u16).toBe("number");
    expect(typeof rows[0].tiny_i8).toBe("number");
    expect(typeof rows[0].small_i16).toBe("number");
    expect(typeof rows[0].amount_d12_6).toBe("number");
    expect(typeof rows[0].event_date32).toBe("string");
    expect(typeof rows[0].code).toBe("string");
    expect(typeof rows[0].account_uuid).toBe("string");
    expect(typeof rows[0].client_ipv4).toBe("string");
    expect(typeof rows[0].client_ipv6).toBe("string");
    expect(typeof rows[0].category).toBe("string");
  });
});
