import {
  type ClickHouseDate,
  type ClickHouseDate32,
  type ClickHouseDateTime,
  type ClickHouseDateTime64,
  type ClickHouseDecimal,
  type ClickHouseInsertResult,
  type ClickHouseInt64,
  type ClickHouseUInt64,
  type ColumnType,
  createClickHouseDB,
  type InferResult,
  type Insertable,
  type Selectable,
  type TypedTable,
  type TypedView,
} from "../src";

interface AdvancedTypecheckDB {
  users: TypedTable<{
    id: number;
    created_at: ClickHouseDateTime64;
    big_user_id: ClickHouseUInt64;
    custom_metric: ColumnType<string, number, number>;
  }>;
  daily_users: TypedView<{
    signup_date: ClickHouseDate;
    total_users: ClickHouseUInt64;
  }>;
}

const advancedDb = createClickHouseDB<AdvancedTypecheckDB>();
const advancedUsersQuery = advancedDb
  .selectFrom("users as u")
  .select("u.id", "u.created_at", "u.big_user_id", "u.custom_metric")
  .where("u.created_at", ">=", new Date("2025-01-01T00:00:00.000Z"))
  .where("u.big_user_id", "=", 42n)
  .where("u.custom_metric", "=", 1);
const advancedViewQuery = advancedDb
  .selectFrom("daily_users as d")
  .select("d.signup_date", "d.total_users");

type AdvancedUserRow = InferResult<typeof advancedUsersQuery>;
type AdvancedViewRow = InferResult<typeof advancedViewQuery>;
type SelectableAdvancedUser = Selectable<AdvancedTypecheckDB["users"]>;
type InsertableAdvancedUser = Insertable<AdvancedTypecheckDB["users"]>;
type SelectableAdvancedView = Selectable<AdvancedTypecheckDB["daily_users"]>;
type SelectablePlainAdvancedRow = Selectable<{
  created_at: ClickHouseDateTime64;
  big_user_id: ClickHouseUInt64;
  custom_metric: ColumnType<string, number, number>;
}>;
type InsertablePlainAdvancedRow = Insertable<{
  created_at: ClickHouseDateTime64;
  big_user_id: ClickHouseUInt64;
  custom_metric: ColumnType<string, number, number>;
}>;
type AdvancedViewInsertable = Insertable<AdvancedTypecheckDB["daily_users"]>;

const validAdvancedUserRow: AdvancedUserRow = {
  id: 1,
  created_at: "2025-01-01 00:00:00.000",
  big_user_id: "42",
  custom_metric: "1",
};

const validAdvancedViewRow: AdvancedViewRow = {
  signup_date: "2025-01-01",
  total_users: "42",
};

const validSelectableAdvancedUser: SelectableAdvancedUser = {
  id: 1,
  created_at: "2025-01-01 00:00:00.000",
  big_user_id: "42",
  custom_metric: "1",
};

const validInsertableAdvancedUser: InsertableAdvancedUser = {
  id: 1,
  created_at: new Date("2025-01-01T00:00:00.000Z"),
  big_user_id: 42n,
  custom_metric: 1,
};

const validSelectableAdvancedView: SelectableAdvancedView = {
  signup_date: "2025-01-01",
  total_users: "42",
};

const validSelectablePlainAdvancedRow: SelectablePlainAdvancedRow = {
  created_at: "2025-01-01 00:00:00.000",
  big_user_id: "42",
  custom_metric: "1",
};

const validInsertablePlainAdvancedRow: InsertablePlainAdvancedRow = {
  created_at: new Date("2025-01-01T00:00:00.000Z"),
  big_user_id: 42n,
  custom_metric: 1,
};

const advancedInsertResultPromise: Promise<ClickHouseInsertResult> = advancedDb
  .insertInto("users")
  .values([
    {
      id: 1,
      created_at: new Date("2025-01-01T00:00:00.000Z"),
      big_user_id: 42n,
      custom_metric: 1,
    },
  ])
  .execute();

void validAdvancedUserRow;
void validAdvancedViewRow;
void validSelectableAdvancedUser;
void validInsertableAdvancedUser;
void validSelectableAdvancedView;
void validSelectablePlainAdvancedRow;
void validInsertablePlainAdvancedRow;
void advancedInsertResultPromise;

const _invalidSelectableAdvancedUser: SelectableAdvancedUser = {
  id: 1,
  created_at: "2025-01-01 00:00:00.000",
  big_user_id: "42",
  // @ts-expect-error Selectable should expose select values for wrapped columns
  custom_metric: 1,
};

const _invalidInsertableAdvancedUser: InsertableAdvancedUser = {
  id: 1,
  created_at: "2025-01-01 00:00:00.000",
  big_user_id: "42",
  // @ts-expect-error Insertable should expose insert values for wrapped columns
  custom_metric: "1",
};

// @ts-expect-error Insertable<TypedView<...>> should resolve to never
const _invalidAdvancedViewInsertable: AdvancedViewInsertable = {
  signup_date: "2025-01-01",
  total_users: "42",
};

// @ts-expect-error typed views should not be insertable
advancedDb.insertInto("daily_users");

// @ts-expect-error typed views should not be table sources
advancedDb.table("daily_users");

// @ts-expect-error custom column where type should stay numeric
advancedDb.selectFrom("users as u").where("u.custom_metric", "=", "1");

// @ts-expect-error ClickHouseDateTime64 should not accept numeric predicate values
advancedDb.selectFrom("users as u").where("u.created_at", "=", 1);

interface AliasTypecheckDB {
  typed_aliases: TypedTable<{
    event_date: ClickHouseDate;
    event_date32: ClickHouseDate32;
    event_time: ClickHouseDateTime;
    created_at: ClickHouseDateTime64;
    amount: ClickHouseDecimal;
    signed_total: ClickHouseInt64;
    unsigned_total: ClickHouseUInt64;
  }>;
}

const aliasDb = createClickHouseDB<AliasTypecheckDB>();
const aliasQuery = aliasDb
  .selectFrom("typed_aliases as t")
  .select(
    "t.event_date",
    "t.event_date32",
    "t.event_time",
    "t.created_at",
    "t.amount",
    "t.signed_total",
    "t.unsigned_total",
  )
  .where("t.event_date", "=", "2025-01-01")
  .where("t.event_date32", "=", "2025-01-01")
  .where("t.event_time", ">=", new Date("2025-01-01T12:00:00.000Z"))
  .where("t.created_at", ">=", "2025-01-01 12:00:00.123")
  .where("t.amount", "=", "1.25")
  .where("t.signed_total", "=", 42n)
  .where("t.unsigned_total", "=", 42);

type AliasRow = InferResult<typeof aliasQuery>;

const validAliasRow: AliasRow = {
  event_date: "2025-01-01",
  event_date32: "2025-01-01",
  event_time: "2025-01-01 12:00:00",
  created_at: "2025-01-01 12:00:00.123",
  amount: 1.25,
  signed_total: "42",
  unsigned_total: "42",
};

const aliasInsertResultPromise: Promise<ClickHouseInsertResult> = aliasDb
  .insertInto("typed_aliases")
  .values([
    {
      event_date: "2025-01-01",
      event_date32: "2025-01-01",
      event_time: new Date("2025-01-01T12:00:00.000Z"),
      created_at: new Date("2025-01-01T12:00:00.123Z"),
      amount: "1.25",
      signed_total: 42,
      unsigned_total: 42n,
    },
  ])
  .execute();

void validAliasRow;
void aliasInsertResultPromise;

// @ts-expect-error ClickHouseDate should not accept numeric predicate values
aliasDb.selectFrom("typed_aliases as t").where("t.event_date", "=", 1);

aliasDb.insertInto("typed_aliases").values([
  {
    // @ts-expect-error ClickHouseDate should not accept Date insert values
    event_date: new Date("2025-01-01T00:00:00.000Z"),
    event_date32: "2025-01-01",
    event_time: "2025-01-01 12:00:00",
    created_at: "2025-01-01 12:00:00.123",
    amount: 1.25,
    signed_total: "42",
    unsigned_total: "42",
  },
]);

aliasDb.insertInto("typed_aliases").values([
  {
    event_date: "2025-01-01",
    // @ts-expect-error ClickHouseDate32 should not accept boolean insert values
    event_date32: true,
    event_time: "2025-01-01 12:00:00",
    created_at: "2025-01-01 12:00:00.123",
    amount: 1.25,
    signed_total: "42",
    unsigned_total: "42",
  },
]);

// @ts-expect-error ClickHouseDateTime should not accept numeric predicate values
aliasDb.selectFrom("typed_aliases as t").where("t.event_time", "=", 1);

// @ts-expect-error ClickHouseDecimal should not accept boolean predicates
aliasDb.selectFrom("typed_aliases as t").where("t.amount", "=", true);

aliasDb.insertInto("typed_aliases").values([
  {
    event_date: "2025-01-01",
    event_date32: "2025-01-01",
    event_time: "2025-01-01 12:00:00",
    created_at: "2025-01-01 12:00:00.123",
    amount: 1.25,
    // @ts-expect-error ClickHouseInt64 should not accept boolean insert values
    signed_total: false,
    unsigned_total: "42",
  },
]);

// @ts-expect-error ClickHouseUInt64 should not accept boolean predicates
aliasDb.selectFrom("typed_aliases as t").where("t.unsigned_total", "=", false);
