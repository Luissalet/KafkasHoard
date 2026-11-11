import test from "node:test";
import assert from "node:assert/strict";
import { installAppDock } from "./src/app-dock.js";

test("Kafka opens only resolved documents and preserves filter values in its live routes", async () => {
  let refreshed = 0;
  const api = { document: async () => ({}), call: async () => ({ documents: [{ id: "d /1", title: "Póliza" }, { id: "d2", title: "Poliza" }] }) };
  const target = { location: { hash: "#/" }, document: { querySelector: () => null } };
  const dispose = installAppDock({ api, changed: () => refreshed++, pages: ["plazos", "documentos"] }, target);
  const queued = target.HoardApp.q;
  const tools = new Map(queued.filter(([m]) => m === "registerTool").map(([, n, d, h]) => [n, h]));
  assert.equal(tools.size, 3);
  const ctx = { cursor: { to: async () => {} } };
  await assert.rejects(tools.get("kafka_open")({ title: "póliza" }, ctx), (e) => e.code === "ambiguous" && e.candidates.length === 2);
  await assert.rejects(tools.get("kafka_open")({ title: "missing" }, ctx), (e) => e.code === "not_found");
  await tools.get("kafka_open")({ id: "d /1" }, ctx);
  assert.equal(target.location.hash, "#/documentos/d%20%2F1");
  assert.equal(queued.find(([m]) => m === "setContext")[1]().focus.id, "d /1");
  await tools.get("kafka_filter")({ q: "a & b", year: "2026", state: "review" }, ctx);
  assert.equal(new URLSearchParams(target.location.hash.split("?")[1]).get("q"), "a & b");
  await tools.get("kafka_view")({ view: "plazos" }, ctx); assert.equal(target.location.hash, "#/");
  const changed = queued.find(([m]) => m === "onChanged")[1];
  changed(); assert.equal(refreshed, 1); dispose(); changed(); assert.equal(refreshed, 1);
});
