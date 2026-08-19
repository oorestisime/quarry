import assert from "node:assert/strict";
import test from "node:test";
import * as compatibility from "../index.js";
import * as canonical from "quarry";

test("re-exports the canonical implementation without duplicating class identities", () => {
  assert.equal(compatibility.createClickHouseDB, canonical.createClickHouseDB);
  assert.equal(compatibility.ExpressionBuilder, canonical.ExpressionBuilder);

  const expressionBuilder = new compatibility.ExpressionBuilder();
  assert.ok(expressionBuilder instanceof canonical.ExpressionBuilder);
});
