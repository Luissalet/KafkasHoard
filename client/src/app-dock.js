const fold = (value) => String(value || "").normalize("NFD").replace(/\p{M}/gu, "").toLowerCase().trim();
export function installAppDock({ api, changed, pages }, target = window) {
  const app = target.HoardApp ||= { q: [] };
  const send = (method, ...args) => app[method] ? app[method](...args) : app.q.push([method, ...args]);
  let active = true;
  const point = async (cursor) => { const el = target.document.querySelector("main h1, main"); if (el) await cursor.to(el); };
  const tool = (name, description, properties, required, handler) => send("registerTool", name, {
    description, inputSchema: { type: "object", properties, required, additionalProperties: false },
  }, handler);
  const text = { type: "string", minLength: 1 };
  tool("kafka_open", "Open a filed document by id or unique title", { id: text, title: text }, [], async ({ id, title }, { cursor }) => {
    if (!id && !title?.trim()) throw Object.assign(new Error("Provide id or title"), { code: "bad_args" });
    if (!id) {
      const rows = (await api.call("docs_list", { text: title, limit: 100 })).documents;
      const exact = rows.filter((d) => fold(d.title) === fold(title));
      const matches = exact.length ? exact : rows.filter((d) => fold(d.title).includes(fold(title)));
      if (matches.length !== 1) throw Object.assign(new Error(matches.length ? "Several documents match; use an id" : "Document not found"), {
        code: matches.length ? "ambiguous" : "not_found", candidates: matches.slice(0, 8).map((d) => ({ id: d.id, label: d.title })),
      });
      id = matches[0].id;
    }
    await api.document(id);
    await point(cursor); target.location.hash = `#/documentos/${encodeURIComponent(id)}`;
    return { opened: id };
  });
  tool("kafka_filter", "Filter filed documents by text, kind, year and state", {
    q: { type: "string", maxLength: 200 }, kind: { type: "string" }, year: { type: "string" },
    state: { type: "string", enum: ["", "review", "ok", "archived"] },
  }, [], async (filter, { cursor }) => {
    await point(cursor);
    target.location.hash = `#/documentos?${new URLSearchParams(filter)}`;
    return { view: "documentos", filters: filter };
  });
  tool("kafka_view", "Switch the Kafka section", { view: { type: "string", enum: pages } }, ["view"], async ({ view }, { cursor }) => {
    await point(cursor); target.location.hash = view === "plazos" ? "#/" : `#/${view}`; return { view };
  });
  send("setContext", () => {
    const route = target.location.hash.replace(/^#\/?/, "");
    const [path, query = ""] = route.split("?");
    const [page = "", id] = path.split("/");
    return { view: page || "plazos", focus: page === "documentos" && id ? {
      kind: "document", id: decodeURIComponent(id), label: target.document.querySelector("main h1")?.textContent || "",
    } : undefined, summary: JSON.stringify(Object.fromEntries([...(target.document.querySelectorAll?.("[data-dock-filter]") || [])].map((el) => [el.dataset.dockFilter, el.value]))) };
  });
  const unsubscribe = send("onChanged", () => { if (active) changed(); });
  return () => { active = false; if (typeof unsubscribe === "function") unsubscribe(); };
}
