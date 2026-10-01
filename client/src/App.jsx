import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, errorText } from "./api.js";
import { initialLang, makeT, saveLang } from "./i18n.js";
import { AppContext } from "./context.js";
import { ConfirmDialog, Icon, ICONS } from "./components/ui.jsx";
import Plazos from "./pages/Plazos.jsx";
import Documentos from "./pages/Documentos.jsx";
import Revision from "./pages/Revision.jsx";
import Ajustes from "./pages/Ajustes.jsx";

export { useApp } from "./context.js";

const PAGES = [
  { path: "", key: "nav_deadlines", icon: ICONS.clock, component: Plazos, badge: "overdue" },
  { path: "documentos", key: "nav_documents", icon: ICONS.doc, component: Documentos },
  { path: "revision", key: "nav_review", icon: "M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6zM9 12l2 2 4-4", component: Revision, badge: "review" },
  { path: "ajustes", key: "nav_settings", icon: "M12 15a3 3 0 100-6 3 3 0 000 6zM19 12l2-1-1-3-2 .3-1.4-1.4.3-2-3-1-1 2h-2l-1-2-3 1 .3 2L6.8 7.3 5 7 4 10l2 1v2l-2 1 1 3 2-.3 1.4 1.4-.3 2 3 1 1-2h2l1 2 3-1-.3-2 1.4-1.4 2 .3 1-3-2-1z", component: Ajustes },
];

function useHashRoute() {
  const read = () => {
    const [path, query = ""] = window.location.hash.replace(/^#\/?/, "").split("?");
    const parts = path.split("/").filter(Boolean).map(decodeURIComponent);
    return { page: parts[0] || "", param: parts[1] || null, query: new URLSearchParams(query) };
  };
  const [route, setRoute] = useState(read);
  useEffect(() => {
    const onChange = () => { setRoute(read()); window.scrollTo(0, 0); };
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return route;
}

export function Toast({ toast, onClose }) {
  useEffect(() => {
    if (!toast) return undefined;
    const timer = setTimeout(onClose, toast.kind === "error" ? 9000 : 4500);
    return () => clearTimeout(timer);
  }, [toast, onClose]);
  if (!toast) return null;
  return (
    <div className={`toast ${toast.kind === "error" ? "toast-error" : "toast-ok"}`} role={toast.kind === "error" ? "alert" : "status"} onClick={onClose}>
      {toast.message}
    </div>
  );
}

function SchedulerLight({ scheduler, t }) {
  if (!scheduler) return null;
  const lanes = Object.values(scheduler.lanes || {});
  const busy = lanes.some((l) => l.current);
  const queue = lanes.reduce((n, l) => n + (l.queue || 0), 0);
  const state = !scheduler.enabled || !scheduler.running ? "off" : scheduler.paused ? "paused" : busy ? "busy" : "idle";
  const color = { off: "var(--muted)", paused: "var(--warn)", busy: "var(--accent)", idle: "var(--ok)" }[state];
  return (
    <div className="space-y-0.5" aria-live="polite">
      <div className="flex items-center gap-2 font-semibold text-[12px]" style={{ color: "var(--ink)" }}>
        <span className="dot" style={{ background: color }} />
        {t(`sched_${state}`)}
      </div>
      <div className="help">{t("sched_queue", { n: queue })} · {t("sched_done", { n: scheduler.jobs_done ?? 0 })}</div>
    </div>
  );
}

export default function App() {
  const route = useHashRoute();
  const [lang, setLang] = useState(initialLang);
  const t = useMemo(() => makeT(lang), [lang]);
  const [health, setHealth] = useState(null);
  const [dash, setDash] = useState(null);
  const [dashError, setDashError] = useState(null);
  const [toast, setToast] = useState(null);
  const [confirmReq, setConfirmReq] = useState(null);
  const [version, setVersion] = useState(0);
  const [uploading, setUploading] = useState(false);
  const [dragging, setDragging] = useState(false);
  const confirmResolve = useRef(null);
  const dragDepth = useRef(0);

  useEffect(() => { document.documentElement.lang = lang; }, [lang]);
  useEffect(() => {
    const n = dash?.counts?.unseen_notifications || 0;
    document.title = n ? `(${n}) Kafka's Hoard` : "Kafka's Hoard";
  }, [dash]);

  const refreshDash = useCallback(async () => {
    try {
      setDash(await api.dashboard());
      setDashError(null);
    } catch (e) {
      setDashError(e);
    }
  }, []);
  // Something changed (a document, a deadline, a setting): reload the dashboard and every page that listens to `version`.
  const changed = useCallback(() => { setVersion((v) => v + 1); refreshDash(); }, [refreshDash]);

  // Refresh every 60 s while the tab is visible, and when it becomes visible again.
  useEffect(() => {
    refreshDash();
    api.health().then(setHealth).catch(() => {});
    const timer = setInterval(() => { if (!document.hidden) refreshDash(); }, 60000);
    const onVisible = () => { if (!document.hidden) refreshDash(); };
    document.addEventListener("visibilitychange", onVisible);
    return () => { clearInterval(timer); document.removeEventListener("visibilitychange", onVisible); };
  }, [refreshDash]);

  const notify = useCallback((message, kind = "ok") => setToast({ message, kind, id: Math.random() }), []);
  const toastError = useCallback((error) => setToast({ message: errorText(error), kind: "error", id: Math.random() }), []);
  const confirm = useCallback((request) => new Promise((resolve) => {
    confirmResolve.current = resolve;
    setConfirmReq(request);
  }), []);
  const closeConfirm = useCallback((answer) => {
    setConfirmReq(null);
    if (confirmResolve.current) confirmResolve.current(answer);
    confirmResolve.current = null;
  }, []);

  const changeLang = useCallback((next) => {
    saveLang(next);
    setLang(next);
    api.call("settings_set", { values: { "ui.language": next } }).then(() => changed()).catch(() => {});
  }, [changed]);

  const upload = useCallback(async (files) => {
    if (!files?.length) return null;
    setUploading(true);
    try {
      const r = await api.upload(files);
      const failed = (r.results || []).filter((x) => !x.ok);
      const parts = [];
      if (r.created) parts.push(t("upload_created", { n: r.created }));
      if (r.duplicates) parts.push(t("upload_duplicates", { n: r.duplicates }));
      if (failed.length) parts.push(t("upload_failed", { n: failed.length, why: failed[0].error || "" }));
      setToast({ message: parts.join(" · ") || t("upload_nothing"), kind: failed.length && !r.created ? "error" : "ok", id: Math.random() });
      changed();
      const first = (r.results || []).find((x) => x.ok && x.created && x.documents?.length);
      if (first && files.length === 1 && first.documents.length === 1) window.location.hash = `#/documentos/${first.documents[0].id}`;
      return r;
    } catch (e) {
      toastError(e);
      return null;
    } finally {
      setUploading(false);
    }
  }, [t, changed, toastError]);

  // Dropping files anywhere on the window files them.
  useEffect(() => {
    const hasFiles = (e) => Array.from(e.dataTransfer?.types || []).includes("Files");
    const enter = (e) => { if (!hasFiles(e)) return; e.preventDefault(); dragDepth.current += 1; setDragging(true); };
    const over = (e) => { if (hasFiles(e)) e.preventDefault(); };
    const leave = (e) => { if (!hasFiles(e)) return; dragDepth.current = Math.max(0, dragDepth.current - 1); if (!dragDepth.current) setDragging(false); };
    const drop = (e) => {
      if (!hasFiles(e)) return;
      e.preventDefault();
      dragDepth.current = 0;
      setDragging(false);
      upload(Array.from(e.dataTransfer.files));
    };
    window.addEventListener("dragenter", enter);
    window.addEventListener("dragover", over);
    window.addEventListener("dragleave", leave);
    window.addEventListener("drop", drop);
    return () => {
      window.removeEventListener("dragenter", enter);
      window.removeEventListener("dragover", over);
      window.removeEventListener("dragleave", leave);
      window.removeEventListener("drop", drop);
    };
  }, [upload]);

  const value = useMemo(() => ({
    t, lang, setLang: changeLang, health, dash, dashError, refreshDash, changed, version, notify, toastError, confirm, upload, uploading, route,
  }), [t, lang, changeLang, health, dash, dashError, refreshDash, changed, version, notify, toastError, confirm, upload, uploading, route]);

  const page = PAGES.find((p) => p.path === route.page) || PAGES[0];
  const Component = page.component;
  const badges = { overdue: dash?.counts?.overdue || 0, review: (dash?.counts?.review || 0) + (dash?.counts?.mails_review || 0) };

  return (
    <AppContext.Provider value={value}>
      <a href="#main" className="sr-only focus:not-sr-only focus:absolute focus:z-50 skip-link focus:p-2">{t("skip")}</a>
      <div className="min-h-dvh md:grid md:grid-cols-[210px_minmax(0,1fr)]">
        <aside className="sticky top-0 z-10 border-b md:flex md:h-dvh md:flex-col md:self-start md:border-b-0 md:border-r" style={{ background: "var(--sidebar)", borderColor: "var(--line)" }}>
          <div className="flex items-center gap-3 px-4 py-3 md:py-4">
            <img src="/icon-192.png" alt="" width="30" height="30" className="rounded-lg" />
            <div className="text-[14px] font-semibold leading-tight">Kafka's Hoard</div>
          </div>
          <nav aria-label={t("sections")} className="flex gap-1 overflow-x-auto px-3 pb-2 md:flex-col">
            {PAGES.map((p) => {
              const n = p.badge ? badges[p.badge] : 0;
              return (
                <a key={p.path} href={`#/${p.path}`} className="nav-link shrink-0 text-[13px]" aria-current={p.path === page.path ? "page" : undefined}>
                  <Icon d={p.icon} />
                  {t(p.key)}
                  {n > 0 && <span className="nav-badge" aria-label={t("badge_n", { n })}>{n > 99 ? "99+" : n}</span>}
                </a>
              );
            })}
          </nav>
          <div className="hidden flex-1 md:block" />
          <div className="hidden space-y-3 border-t px-4 py-3 md:block" style={{ borderColor: "var(--line)" }}>
            <SchedulerLight scheduler={dash?.scheduler} t={t} />
            {health?.offline && <span className="chip chip-amber">{t("offline_on")}</span>}
            <button type="button" className="btn btn-sm" onClick={() => changeLang(lang === "es" ? "en" : "es")}>{t("language")}</button>
          </div>
        </aside>
        <main id="main" className="min-w-0 px-4 py-4 md:px-7 md:py-6">
          {dashError && !dash && (
            <div className="banner banner-danger mb-4" role="alert">
              {t("unreachable")}: {dashError.message}. <button type="button" className="btn-link" onClick={refreshDash}>{t("retry")}</button>
            </div>
          )}
          {dashError && dash && (
            <div className="banner banner-warn mb-4" role="status">
              {t("stale")}: {dashError.message}. <button type="button" className="btn-link" onClick={refreshDash}>{t("retry")}</button>
            </div>
          )}
          <Component key={`${route.page}/${route.param || ""}`} param={route.param} query={route.query} />
          <div className="mt-8 flex flex-wrap items-center gap-3 border-t pt-3 md:hidden" style={{ borderColor: "var(--line)" }}>
            <SchedulerLight scheduler={dash?.scheduler} t={t} />
            <button type="button" className="btn btn-sm" onClick={() => changeLang(lang === "es" ? "en" : "es")}>{t("language")}</button>
          </div>
        </main>
      </div>
      {dragging && (
        <div className="modal-backdrop" style={{ pointerEvents: "none", background: "#000c" }} aria-hidden="true">
          <div className="panel flex items-center gap-3" style={{ borderStyle: "dashed", borderColor: "var(--accent)", padding: "28px 36px" }}>
            <Icon d={ICONS.upload} size={30} color="var(--accent)" />
            <span className="font-semibold" style={{ fontSize: 16 }}>{t("drop_overlay")}</span>
          </div>
        </div>
      )}
      <Toast toast={toast} onClose={() => setToast(null)} />
      <ConfirmDialog request={confirmReq} onClose={closeConfirm} />
    </AppContext.Provider>
  );
}
