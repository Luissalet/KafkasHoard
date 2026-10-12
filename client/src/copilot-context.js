// What the shared copilot launcher asks this app for: the real objects open in the current view and the unsaved form fields,
// plus a way to refresh the open view when the assistant finishes a turn. Nothing here renders anything.
import { api } from "./api.js";

const PAGE_NAMES = { "": "deadlines", documentos: "documents", revision: "review", taller: "workshop", ajustes: "settings" };
const EDITOR_ORDER = { "deadline-form": 0, "deadline-new": 0, "document-form": 1 };

// Fields of `draft` whose value differs from `initial` (compared as JSON, so arrays and "" clears count).
export function changedFields(initial, draft) {
  const base = initial || {};
  return Object.fromEntries(Object.entries(draft || {}).filter(([key, value]) => JSON.stringify(value) !== JSON.stringify(base[key])));
}

// A refreshed baseline arrives while the user is editing: fields the user did not touch follow the new baseline, touched ones stay.
export function rebaseForm(previousBase, nextBase, form) {
  const out = { ...nextBase };
  for (const key of Object.keys(form)) {
    if (JSON.stringify(form[key]) !== JSON.stringify(previousBase?.[key])) out[key] = form[key];
  }
  return out;
}

// The doc_update arguments for the fields the user changed; fields that cannot be sent (an emptied amount) are left out.
export function docUpdateValues(changed) {
  const values = {};
  const number = (v) => Number(String(v).replace(",", "."));
  for (const [key, value] of Object.entries(changed)) {
    if (["title", "kind", "issuer", "ref", "issue_date", "period_from", "period_to", "item", "notes"].includes(key)) values[key] = value;
    else if (key === "amount" && value !== "") values.amount = number(value);
    else if (key === "tags") values.tags = String(value).split(/[,;]/).map((s) => s.trim()).filter(Boolean);
    else if (key === "warranty_years" && value !== "") values.warranty_years = number(value);
  }
  return values;
}

// "30, 7 0" -> [30, 7, 0]; null when something is not a whole number of days between 0 and 365.
export function parseReminders(text) {
  const list = String(text || "").split(/[\s,;]+/).filter(Boolean).map(Number);
  return list.some((n) => !Number.isInteger(n) || n < 0 || n > 365) ? null : list;
}

// The deadline_update arguments for the fields the user changed (reminders already parsed by the caller).
export function deadlineUpdateValues(changed) {
  const values = {};
  for (const [key, value] of Object.entries(changed)) {
    if (["title", "date", "recurring", "notes"].includes(key)) values[key] = value;
    else if (key === "remind") values.remind = Array.isArray(value) ? value : parseReminders(value) || [];
  }
  return values;
}

export function parseRoute(hash) {
  const [path, query = ""] = String(hash || "").replace(/^#\/?/, "").split("?");
  const parts = path.split("/").filter(Boolean).map((p) => { try { return decodeURIComponent(p); } catch { return p; } });
  return { page: parts[0] || "", param: parts[1] || null, query: new URLSearchParams(query) };
}

// Every open form answers this event with a snapshot of what the user has typed: { kind, object_id, dirty, fields }.
export function collectEditors(target = globalThis.window) {
  const found = [];
  if (!target?.dispatchEvent) return found;
  target.dispatchEvent(new CustomEvent("kafka:context-request", { detail: { provide: (snapshot) => { if (snapshot) found.push(snapshot); } } }));
  return found.sort((a, b) => (EDITOR_ORDER[a.kind] ?? 9) - (EDITOR_ORDER[b.kind] ?? 9));
}

const pick = (source, keys) => Object.fromEntries(keys.filter((k) => source?.[k] !== undefined && source[k] !== null && source[k] !== "").map((k) => [k, source[k]]));
const documentObject = (d) => ({ kind: "document", object_id: d.id, title: d.title || d.id, ...pick(d, ["kind_label", "issuer", "ref", "amount", "currency", "issue_date", "state"]), ...(d.kind ? { doc_kind: d.kind } : {}) });
const deadlineObject = (d) => ({ kind: "deadline", object_id: d.id, title: d.title || d.id, ...pick(d, ["date", "state", "kind_label", "recurring", "days_left", "doc_id"]), ...(d.kind && !d.kind_label ? { deadline_kind: d.kind } : {}) });

async function safe(fn) {
  try { return await fn(); } catch { return null; }
}

// Pure core (dependencies injected so it can be tested): never throws; unknown or "new" ids yield no object.
export async function buildContext({ hash, editors = [], getDocument, getDeadline }) {
  const route = parseRoute(hash);
  const page = PAGE_NAMES[route.page] || route.page || "deadlines";
  const active = [];
  const seen = new Set();
  const add = (object) => { const key = `${object.kind}:${object.object_id}`; if (!seen.has(key)) { seen.add(key); active.push(object); } };
  const realId = (id) => (id && id !== "new" ? id : null);

  let viewObject = null;
  const docId = realId(route.page === "documentos" ? route.param : route.page === "taller" ? route.query.get("doc") : null);
  if (docId) {
    const data = await safe(() => getDocument(docId));
    if (data?.document?.id) {
      viewObject = data.document.id;
      add(documentObject(data.document));
      for (const d of (data.deadlines || [])) if (d?.id) add(deadlineObject(d));
    }
  }
  for (const e of editors) {
    if (e.kind !== "deadline-form" || !realId(e.object_id) || seen.has(`deadline:${e.object_id}`)) continue;
    const data = await safe(() => getDeadline(e.object_id));
    const d = data?.deadline;
    if (d?.id) add(deadlineObject(d));
  }

  const view = { page };
  if (viewObject) view.object_id = viewObject;
  const editor = editors.find((e) => e.dirty) || editors[0];
  if (editor) view.editor = editor;
  if (editors.length > 1) view.editors = editors;
  return { active_objects: active, view };
}

export function context() {
  return buildContext({
    hash: globalThis.window?.location?.hash,
    editors: collectEditors(),
    getDocument: (id) => api.document(id),
    getDeadline: (id) => api.call("deadline_get", { deadline: id }),
  });
}

// ---- refresh when the assistant finishes a turn -------------------------------------------------------------------------
let refreshHandler = null;
let timer = null;

function scheduleRefresh() {
  clearTimeout(timer);
  timer = setTimeout(() => { try { refreshHandler?.(); } catch { /* a refresh must never break the page */ } }, 250);
}

// The app registers what "reload what is on screen" means (it keeps the forms the user is editing untouched).
export function setRefreshHandler(fn) {
  refreshHandler = fn;
  return () => { if (refreshHandler === fn) refreshHandler = null; };
}

export const onTurnEnd = () => scheduleRefresh();

// Once at start: the object the launcher looks for, and the window event it dispatches on turn end (the debounce merges the two).
export function installCopilot(target = globalThis.window) {
  if (!target || target.hoardCopilot) return;
  target.hoardCopilot = { context, onTurnEnd };
  target.addEventListener("hoard:copilot-turn-end", scheduleRefresh);
}
