import React, { useEffect, useMemo, useRef, useState } from "react";
import { useApp } from "../context.js";
import { KIND_ICON, SEVERITY_CLASS } from "../meta.js";
import { clock, daysText, dayLabel, rel } from "../format.js";

export function Icon({ d, size = 17, color }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color || "currentColor"} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d={d} />
    </svg>
  );
}

export const ICONS = {
  mail: "M3 6h18v12H3zM3 7l9 7 9-7",
  plus: "M12 5v14M5 12h14",
  refresh: "M3 12a9 9 0 0115-6.7L21 8M21 3v5h-5M21 12a9 9 0 01-15 6.7L3 16M3 21v-5h5",
  external: "M14 4h6v6M20 4l-9 9M18 14v6H4V6h6",
  copy: "M9 9h11v11H9zM5 15V4h11",
  pencil: "M4 20h4L19 9l-4-4L4 16zM13 7l4 4",
  box: "M4 7l8-4 8 4v10l-8 4-8-4zM4 7l8 4 8-4M12 11v10",
  bell: "M6 9a6 6 0 1112 0c0 7 3 8 3 8H3s3-1 3-8M10 21h4",
  bellOff: "M6 9a6 6 0 019-5M18 9c0 3 .6 5 1.5 6.3M3 17s3-1 3-8M10 21h4M3 3l18 18",
  archive: "M3 5h18v4H3zM5 9v11h14V9M10 13h4",
  trash: "M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13",
  check: "M5 12l5 5 9-10",
  chevron: "M9 6l6 6-6 6",
  back: "M15 6l-6 6 6 6",
  upload: "M12 16V4M7 9l5-5 5 5M4 16v4h16v-4",
  doc: "M7 3h8l4 4v14H7zM15 3v4h4",
  clock: "M12 21a9 9 0 100-18 9 9 0 000 18zM12 7v5l3 2",
  search: "M11 4a7 7 0 100 14 7 7 0 000-14zM21 21l-5-5",
  folder: "M3 6h6l2 2h10v11H3z",
  tools: "M14 6a4 4 0 005 5l-9 9a2.1 2.1 0 01-3-3l9-9a4 4 0 00-2-2zM14 6l3-3 4 4-3 3",
  down: "M12 4v12M7 11l5 5 5-5M4 20h16",
  up: "M12 20V8M7 13l5-5 5 5",
  x: "M6 6l12 12M18 6L6 18",
};

export function Spinner() {
  return <span className="spinner" role="status" aria-label="…" />;
}

export function Empty({ children }) {
  return <div className="panel help text-center">{children}</div>;
}

export function Field({ label, hint, children, className = "" }) {
  return (
    <label className={`block ${className}`}>
      <span className="label">{label}</span>
      {children}
      {hint && <span className="help mt-1 block">{hint}</span>}
    </label>
  );
}

export function Switch({ checked, onChange, disabled, label }) {
  return (
    <label className="switch" title={label}>
      <input type="checkbox" role="switch" checked={!!checked} disabled={disabled} aria-label={label} onChange={(e) => onChange(e.target.checked)} />
      <span />
    </label>
  );
}

export function Chip({ children, className = "", title }) {
  return <span className={`chip ${className}`} title={title}>{children}</span>;
}

export function Section({ title, count, actions, children, id }) {
  return (
    <section className="space-y-2" aria-labelledby={id}>
      <div className="flex flex-wrap items-center gap-2">
        <h2 id={id}>{title}</h2>
        {count !== undefined && count !== null && <span className="chip">{count}</span>}
        <div className="ml-auto flex flex-wrap items-center gap-2">{actions}</div>
      </div>
      {children}
    </section>
  );
}

export function Busy({ busy, children, ...props }) {
  return (
    <button type="button" {...props} disabled={busy || props.disabled}>
      {busy && <span className="spinner" aria-hidden="true" />}
      {children}
    </button>
  );
}

export function ErrorBox({ error }) {
  const { t } = useApp();
  if (!error) return null;
  return (
    <div className="banner banner-danger" role="alert">
      {error.message || String(error)}
      {error.hint ? <span className="help block">{t("hint")}: {error.hint}</span> : null}
    </div>
  );
}

export function Rel({ ts }) {
  const { lang } = useApp();
  if (!ts) return <span className="help">—</span>;
  return <time dateTime={new Date(ts * 1000).toISOString()} title={clock(ts, lang)}>{rel(ts, lang)}</time>;
}

export function KindChip({ kind, label }) {
  const { t } = useApp();
  return (
    <span className="chip">
      <Icon d={KIND_ICON[kind] || KIND_ICON.other} size={12} />
      {label || t(`kind_${kind}`)}
    </span>
  );
}

// "en 5 días" coloured by how urgent it is. Closed deadlines show their state instead.
export function DaysChip({ deadline }) {
  const { t, lang } = useApp();
  if (deadline.state === "done") return <span className="chip chip-ok">{t("st_done")}</span>;
  if (deadline.state === "dismissed") return <span className="chip">{t("st_dismissed")}</span>;
  const cls = deadline.days_left < 0 ? "chip-danger" : SEVERITY_CLASS[deadline.severity] || "";
  return <span className={`chip ${cls}`} title={dayLabel(deadline.date, lang, { year: true })}>{daysText(deadline.days_left, t)}</span>;
}

// ------------------------------------------------------------------ dialogs
export function Modal({ title, onClose, children, wide }) {
  const ref = useRef(null);
  useEffect(() => {
    const onKey = (e) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    const first = ref.current?.querySelector("input, select, textarea, button");
    first?.focus();
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);
  return (
    <div className="modal-backdrop" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal space-y-3" ref={ref} role="dialog" aria-modal="true" aria-label={title} style={wide ? { width: "min(560px, 100%)" } : undefined} onMouseDown={(e) => e.stopPropagation()}>
        <h2>{title}</h2>
        {children}
      </div>
    </div>
  );
}

export function ConfirmDialog({ request, onClose }) {
  const { t } = useApp();
  const cancelRef = useRef(null);
  useEffect(() => {
    if (!request) return undefined;
    cancelRef.current?.focus();
    const onKey = (e) => { if (e.key === "Escape") onClose(false); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [request, onClose]);
  if (!request) return null;
  return (
    <div className="modal-backdrop" onClick={() => onClose(false)}>
      <div className="modal space-y-3" role="alertdialog" aria-modal="true" aria-labelledby="confirm-title" onClick={(e) => e.stopPropagation()}>
        <h2 id="confirm-title">{request.title || t("confirm")}</h2>
        <p>{request.message}</p>
        <div className="flex justify-end gap-2">
          <button type="button" ref={cancelRef} className="btn" onClick={() => onClose(false)}>{t("cancel")}</button>
          <button type="button" className={`btn ${request.danger === false ? "btn-primary" : "btn-danger"}`} onClick={() => onClose(true)}>{request.confirmLabel || t("delete")}</button>
        </div>
      </div>
    </div>
  );
}

// Loads something on mount and when `deps` change. `reload()` runs it again without flashing the placeholder.
export function useLoad(fn, deps) {
  const [state, setState] = useState({ data: null, error: null, loading: true });
  const run = useRef(fn);
  run.current = fn;
  const seq = useRef(0);
  const load = useMemo(() => async () => {
    const mine = ++seq.current;
    try {
      const data = await run.current();
      if (mine === seq.current) setState({ data, error: null, loading: false });
    } catch (error) {
      if (mine === seq.current) setState((s) => ({ data: s.data, error, loading: false }));
    }
  }, []);
  useEffect(() => {
    setState((s) => ({ ...s, loading: true }));
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  return { ...state, reload: load };
}

// Busy flags keyed by name; a failing action shows the backend's error and hint in a toast.
export function useBusy() {
  const { toastError } = useApp();
  const [busy, setBusy] = useState({});
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  const run = async (key, fn) => {
    setBusy((b) => ({ ...b, [key]: true }));
    try {
      return await fn();
    } catch (error) {
      toastError(error);
      return undefined;
    } finally {
      if (mounted.current) setBusy((b) => ({ ...b, [key]: false }));
    }
  };
  return [busy, run];
}
