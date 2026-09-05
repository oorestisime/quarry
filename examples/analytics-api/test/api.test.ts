import { strict as assert } from "node:assert";
import { test } from "node:test";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { createAnalyticsServer } from "../src/server";
import { rawAnalytics } from "../src/raw-analytics";

test("seeded analytics API supports filters, tenant scope, SQL inspection, and validation", async () => {
  const { server, client } = createAnalyticsServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    const raw = await rawAnalytics(client, 1, {
      from: "2026-08-01 00:00:00",
      to: "2026-09-01 00:00:00",
    });
    assert.deepEqual(await (await fetch(`${url}/analytics`)).json(), raw);
    const filteredRaw = await rawAnalytics(client, 1, {
      from: "2026-08-04 00:00:00",
      to: "2026-09-01 00:00:00",
      eventType: "purchase",
    });
    assert.deepEqual(
      await (await fetch(`${url}/analytics?eventType=purchase&from=2026-08-04`)).json(),
      filteredRaw,
    );
    assert.deepEqual(await (await fetch(`${url}/analytics`)).json(), [
      { event_type: "purchase", events: "2", users: "1" },
      { event_type: "signup", events: "2", users: "2" },
    ]);
    assert.deepEqual(
      await (await fetch(`${url}/analytics?eventType=purchase&from=2026-08-04`)).json(),
      [{ event_type: "purchase", events: "1", users: "1" }],
    );
    const sql = (await (await fetch(`${url}/sql?eventType=signup`)).json()) as {
      query: string;
      params: Record<string, unknown>;
    };
    assert.match(sql.query, /PREWHERE tenant_id = /);
    assert.equal(sql.params.p0, 1);
    assert.equal((await fetch(`${url}/analytics?from=2026-02-30`)).status, 400);
    assert.equal((await fetch(`${url}/analytics?eventType=bad`)).status, 400);
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
    await client.close();
  }
});
