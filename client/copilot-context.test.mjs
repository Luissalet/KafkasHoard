import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildContext, changedFields, collectEditors, deadlineUpdateValues, docUpdateValues, installCopilot, parseReminders, parseRoute, rebaseForm, setRefreshHandler,
} from "./src/copilot-context.js";

const doc = { id: "d_1", title: "Electricity", kind: "invoice", kind_label: "Invoice", issuer: "Iberdrola", amount: 42.5, state: "ok" };
const deadlines = [{ id: "t_1", title: "Pay", date: "2026-11-01", state: "open", kind_label: "Payment", days_left: 20 }];
const getDocument = async (id) => { if (id !== "d_1") throw new Error("not found"); return { document: doc, deadlines }; };
const getDeadline = async (id) => { if (id !== "t_9") throw new Error("not found"); return { deadline: { id: "t_9", title: "ITV", date: "2027-01-01", state: "open", kind: "itv" } }; };

test("only the fields the user changed are sent, including manual clears", () => {
  assert.deepEqual(changedFields({ title: "A", notes: "x", tags: "a" }, { title: "A", notes: "", tags: "a, b" }), { notes: "", tags: "a, b" });
  assert.deepEqual(docUpdateValues({ amount: "12,5", tags: "a; b", notes: "" }), { amount: 12.5, tags: ["a", "b"], notes: "" });
  assert.deepEqual(docUpdateValues({ amount: "", warranty_years: "" }), {});
  assert.deepEqual(deadlineUpdateValues({ title: "T", remind: [30, 0] }), { title: "T", remind: [30, 0] });
  assert.equal(parseReminders("30, 7 x"), null);
  assert.deepEqual(parseReminders("30, 7;0"), [30, 7, 0]);
});

test("a reload keeps what the user typed and updates the rest", () => {
  const before = { title: "Old", issuer: "A", notes: "" };
  const form = { title: "Old", issuer: "A", notes: "typed" };
  const after = { title: "Agent title", issuer: "A", notes: "" };
  assert.deepEqual(rebaseForm(before, after, form), { title: "Agent title", issuer: "A", notes: "typed" });
  // the agent edit survives a save of the user's own field: only notes differs from the loaded baseline
  assert.deepEqual(changedFields(before, form), { notes: "typed" });
});

test("route parsing", () => {
  assert.deepEqual([parseRoute("#/documentos/d_1?p=2").page, parseRoute("#/documentos/d_1?p=2").param], ["documentos", "d_1"]);
  assert.equal(parseRoute("").page, "");
  assert.equal(parseRoute("#/taller?doc=d_1").query.get("doc"), "d_1");
});

test("document view returns the real document, its deadlines and the editor", async () => {
  const editors = [{ kind: "document-form", object_id: "d_1", dirty: true, fields: { notes: "x" } }];
  const ctx = await buildContext({ hash: "#/documentos/d_1", editors, getDocument, getDeadline });
  assert.deepEqual(ctx.active_objects.map((o) => `${o.kind}:${o.object_id}`), ["document:d_1", "deadline:t_1"]);
  assert.equal(ctx.active_objects[0].issuer, "Iberdrola");
  assert.equal(ctx.view.page, "documents");
  assert.equal(ctx.view.object_id, "d_1");
  assert.deepEqual(ctx.view.editor.fields, { notes: "x" });
});

test("a document with twelve deadlines sends all twelve", async () => {
  const many = Array.from({ length: 12 }, (_, i) => ({ id: `t_${i + 1}`, title: `Deadline ${i + 1}`, date: "2026-11-01", state: "open" }));
  const getMany = async () => ({ document: doc, deadlines: many });
  const ctx = await buildContext({ hash: "#/documentos/d_1", editors: [], getDocument: getMany, getDeadline });
  const ids = ctx.active_objects.filter((o) => o.kind === "deadline").map((o) => o.object_id);
  assert.equal(ids.length, 12);
  assert.deepEqual(ids, many.map((d) => d.id));
});

test("unknown, new and list views yield no object and never throw", async () => {
  for (const hash of ["#/documentos/d_missing", "#/documentos/new", "", "#/revision", "#/ajustes", "#/taller?doc=nope"]) {
    const ctx = await buildContext({ hash, editors: [], getDocument, getDeadline });
    assert.deepEqual(ctx.active_objects, [], hash);
  }
  assert.equal((await buildContext({ hash: "", editors: [], getDocument, getDeadline })).view.page, "deadlines");
  assert.equal((await buildContext({ hash: "#/taller?doc=d_1", editors: [], getDocument, getDeadline })).active_objects[0].object_id, "d_1");
});

test("an open deadline editor adds its deadline; a creation form adds none", async () => {
  const ctx = await buildContext({ hash: "", getDocument, getDeadline, editors: [
    { kind: "deadline-form", object_id: "t_9", dirty: true, fields: { title: "ITV 2" } },
    { kind: "deadline-new", object_id: null, dirty: true, fields: { title: "New" } },
    { kind: "deadline-form", object_id: "t_gone", dirty: false, fields: {} },
  ] });
  assert.deepEqual(ctx.active_objects.map((o) => o.object_id), ["t_9"]);
  assert.equal(ctx.view.editor.fields.title, "ITV 2");
  assert.equal(ctx.view.editors.length, 3);
  const creating = await buildContext({ hash: "", getDocument, getDeadline, editors: [{ kind: "deadline-new", object_id: null, dirty: true, fields: { title: "New" } }] });
  assert.deepEqual(creating.active_objects, []);
  assert.deepEqual(creating.view.editor.fields, { title: "New" });
});

test("forms answer the context request and modals come first", () => {
  const target = new EventTarget();
  target.addEventListener("kafka:context-request", (e) => e.detail.provide({ kind: "document-form", object_id: "d_1" }));
  target.addEventListener("kafka:context-request", (e) => e.detail.provide({ kind: "deadline-new", object_id: null }));
  assert.deepEqual(collectEditors(target).map((e) => e.kind), ["deadline-new", "document-form"]);
});

test("turn end (event and callback) refreshes once", async () => {
  const target = new EventTarget();
  installCopilot(target);
  assert.equal(typeof target.hoardCopilot.context, "function");
  let calls = 0;
  setRefreshHandler(() => { calls += 1; });
  target.dispatchEvent(new Event("hoard:copilot-turn-end"));
  target.hoardCopilot.onTurnEnd();
  await new Promise((r) => setTimeout(r, 400));
  assert.equal(calls, 1);
});
