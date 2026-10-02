import React, { useState } from "react";
import { api } from "../api.js";
import { useApp } from "../context.js";
import { dayLabel, money } from "../format.js";
import { DEADLINE_KINDS, RECURRING } from "../meta.js";
import { Busy, Chip, DaysChip, Field, Icon, ICONS, KindChip, Modal, useBusy } from "./ui.jsx";

const SOURCE_NAMES = { homehoard: "HomeHoard" };
const sourceName = (id) => SOURCE_NAMES[id] || id;

// One deadline: what, when, how urgent, why that date, and the actions that close it.
export function DeadlineCard({ d, onChanged, showDoc = true, compact = false }) {
  const { t, lang, notify, confirm, changed } = useApp();
  const [busy, run] = useBusy();
  const [open, setOpen] = useState(false);
  const [editing, setEditing] = useState(false);
  const doc = d.document;
  const closed = d.state !== "open";
  const after = async () => { changed(); await onChanged?.(); };
  const update = (values, message) => run("u", async () => {
    await api.call("deadline_update", { deadline: d.id, ...values });
    if (message) notify(message);
    await after();
  });
  const remove = async () => {
    const ok = await confirm({ title: t("delete_deadline"), message: t("delete_deadline_msg", { title: d.title }) });
    if (!ok) return;
    run("rm", async () => { await api.call("deadline_delete", { deadline: d.id, confirm: true }); notify(t("deleted")); await after(); });
  };
  const late = d.state === "open" && d.days_left < 0;
  return (
    <article className={`panel space-y-2 ${late ? "card-late" : ""}`} aria-label={d.title}>
      <div className="flex flex-wrap items-start gap-x-3 gap-y-1">
        <div className="min-w-0 flex-1 basis-60">
          <div className="font-semibold" style={{ overflowWrap: "anywhere" }}>{d.title}</div>
          <div className="help flex flex-wrap items-center gap-x-2">
            <span className="num">{dayLabel(d.date, lang, { year: true })}</span>
            {d.amount !== null && d.amount !== undefined && <span className="num">{money(d.amount, doc?.currency, lang)}</span>}
            {showDoc && doc && <a href={`#/documentos/${doc.id}${d.page ? `?p=${d.page}` : ""}`}>{doc.title}</a>}
            {d.source && (d.url ? <a href={d.url} target="_blank" rel="noreferrer">{t("from_source", { source: sourceName(d.source) })}</a>
              : <span>{t("from_source", { source: sourceName(d.source) })}</span>)}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <Chip>{d.kind_label}</Chip>
          {d.recurring !== "none" && <Chip title={t("recurring")}>{t(`rec_${d.recurring}`)}</Chip>}
          {d.edited && <Chip title={t("edited_hint")}>{t("edited")}</Chip>}
          <DaysChip deadline={d} />
        </div>
      </div>
      {!compact && (
        <div className="flex flex-wrap items-center gap-2">
          {!closed && <Busy className="btn btn-sm btn-primary" busy={busy.u} onClick={() => update({ state: "done" }, d.recurring !== "none" ? t("rolled") : t("marked_done"))}><Icon d={ICONS.check} size={14} />{t("mark_done")}</Busy>}
          {!closed && (
            <select className="field" style={{ width: "auto", minHeight: 26, padding: "2px 8px" }} value="" aria-label={t("snooze")} onChange={(e) => e.target.value && update({ snooze_days: Number(e.target.value) }, t("snoozed"))}>
              <option value="">{t("snooze")}…</option>
              <option value="1">{t("snooze_1")}</option>
              <option value="7">{t("snooze_7")}</option>
              <option value="30">{t("snooze_30")}</option>
            </select>
          )}
          <button type="button" className="btn btn-sm" onClick={() => setEditing(true)}><Icon d={ICONS.pencil} size={13} />{t("edit")}</button>
          {closed && <Busy className="btn btn-sm" busy={busy.u} onClick={() => update({ state: "open" }, t("reopened"))}>{t("reopen")}</Busy>}
          {!closed && <Busy className="btn btn-sm" busy={busy.u} onClick={() => update({ state: "dismissed" }, t("dismissed"))}>{t("dismiss")}</Busy>}
          <Busy className="btn btn-sm btn-danger" busy={busy.rm} onClick={remove} aria-label={t("delete")}><Icon d={ICONS.trash} size={13} /></Busy>
          <button type="button" className="btn-link ml-auto text-[12px]" aria-expanded={open} onClick={() => setOpen(!open)}>{open ? t("hide_why") : t("why_date")}</button>
        </div>
      )}
      {open && (
        <div className="space-y-1.5 rounded-md border p-2.5" style={{ borderColor: "var(--line)", background: "var(--field)" }}>
          <p>{d.basis}</p>
          {d.evidence && <blockquote className="help m-0 border-l-2 pl-2" style={{ borderColor: "var(--accent)" }}>«{d.evidence}»</blockquote>}
          <p className="help">
            {d.confidence ? `${t("confidence")}: ${d.confidence} %` : ""}
            {doc && d.page ? <> · <a href={`#/documentos/${doc.id}?p=${d.page}`}>{t("see_page", { n: d.page })}</a></> : null}
            {d.remind?.length ? <> · {t("reminders_at", { days: d.remind.join(", ") })}</> : null}
          </p>
          {d.notes && <p className="help">{d.notes}</p>}
        </div>
      )}
      {editing && <DeadlineEditor d={d} onClose={() => setEditing(false)} onSaved={async () => { setEditing(false); await after(); }} />}
    </article>
  );
}

function DeadlineEditor({ d, onClose, onSaved }) {
  const { t, notify } = useApp();
  const [busy, run] = useBusy();
  const [form, setForm] = useState({ title: d.title, date: d.date, remind: (d.remind || []).join(", "), recurring: d.recurring, notes: d.notes || "" });
  const set = (k, v) => setForm((f) => ({ ...f, [k]: v }));
  const save = (e) => {
    e.preventDefault();
    const remind = form.remind.split(/[\s,;]+/).filter(Boolean).map(Number);
    if (remind.some((n) => !Number.isInteger(n) || n < 0 || n > 365)) { notify(t("bad_reminders"), "error"); return; }
    run("save", async () => {
      await api.call("deadline_update", { deadline: d.id, title: form.title, date: form.date, remind, recurring: form.recurring, notes: form.notes });
      notify(t("saved"));
      await onSaved();
    });
  };
  return (
    <Modal title={t("edit_deadline")} onClose={onClose} wide>
      <form className="space-y-3" onSubmit={save} noValidate>
        <Field label={t("title")}><input className="field" value={form.title} onChange={(e) => set("title", e.target.value)} /></Field>
        <div className="grid gap-3 sm:grid-cols-2">
          <Field label={t("date")}><input className="field" type="date" value={form.date} onChange={(e) => set("date", e.target.value)} /></Field>
          <Field label={t("recurring")}>
            <select className="field" value={form.recurring} onChange={(e) => set("recurring", e.target.value)}>
              {RECURRING.map((r) => <option key={r} value={r}>{t(`rec_${r}`)}</option>)}
            </select>
          </Field>
        </div>
        <Field label={t("reminders")} hint={t("reminders_hint")}><input className="field" value={form.remind} onChange={(e) => set("remind", e.target.value)} /></Field>
        <Field label={t("notes")}><textarea className="field" rows={2} value={form.notes} onChange={(e) => set("notes", e.target.value)} /></Field>
        <div className="flex justify-end gap-2">
          <button type="button" className="btn" onClick={onClose}>{t("cancel")}</button>
          <Busy type="submit" className="btn btn-primary" busy={busy.save}>{t("save")}</Busy>
        </div>
      </form>
    </Modal>
  );
}

export function AddDeadline({ docId, onClose, onSaved }) {
  const { t, notify, changed } = useApp();
  const [busy, run] = useBusy();
  const [form, setForm] = useState({ title: "", date: "", kind: "custom", remind: "", recurring: "none", notes: "" });
  const set = (k, v) => setForm((f) => ({ ...f, [k]: v }));
  const save = (e) => {
    e.preventDefault();
    const remind = form.remind.split(/[\s,;]+/).filter(Boolean).map(Number);
    if (remind.some((n) => !Number.isInteger(n) || n < 0 || n > 365)) { notify(t("bad_reminders"), "error"); return; }
    if (!form.title.trim() || !form.date) { notify(t("need_title_date"), "error"); return; }
    run("save", async () => {
      await api.call("deadline_add", { title: form.title.trim(), date: form.date, kind: form.kind, remind: remind.length ? remind : undefined, recurring: form.recurring, notes: form.notes, doc: docId || "" });
      notify(t("deadline_added"));
      changed();
      await onSaved?.();
    });
  };
  return (
    <Modal title={t("add_deadline")} onClose={onClose} wide>
      <form className="space-y-3" onSubmit={save} noValidate>
        <Field label={t("title")}><input className="field" value={form.title} onChange={(e) => set("title", e.target.value)} placeholder={t("add_title_ph")} /></Field>
        <div className="grid gap-3 sm:grid-cols-2">
          <Field label={t("date")}><input className="field" type="date" value={form.date} onChange={(e) => set("date", e.target.value)} /></Field>
          <Field label={t("kind")}>
            <select className="field" value={form.kind} onChange={(e) => set("kind", e.target.value)}>
              {DEADLINE_KINDS.map((k) => <option key={k} value={k}>{t(`dk_${k}`)}</option>)}
            </select>
          </Field>
          <Field label={t("recurring")}>
            <select className="field" value={form.recurring} onChange={(e) => set("recurring", e.target.value)}>
              {RECURRING.map((r) => <option key={r} value={r}>{t(`rec_${r}`)}</option>)}
            </select>
          </Field>
          <Field label={t("reminders")} hint={t("reminders_hint")}><input className="field" value={form.remind} onChange={(e) => set("remind", e.target.value)} placeholder="30, 7, 0" /></Field>
        </div>
        <Field label={t("notes")}><textarea className="field" rows={2} value={form.notes} onChange={(e) => set("notes", e.target.value)} /></Field>
        <div className="flex justify-end gap-2">
          <button type="button" className="btn" onClick={onClose}>{t("cancel")}</button>
          <Busy type="submit" className="btn btn-primary" busy={busy.save}>{t("add")}</Busy>
        </div>
      </form>
    </Modal>
  );
}

// A document in a list: icon, title, issuer, amount, its next deadline.
export function DocRow({ d, snippet }) {
  const { t, lang } = useApp();
  return (
    <a href={`#/documentos/${d.id}${snippet?.page ? `?p=${snippet.page}` : ""}`} className="panel panel-tight flex flex-wrap items-center gap-x-3 gap-y-1 no-underline" style={{ color: "inherit" }}>
      <div className="min-w-0 flex-1 basis-60">
        <div className="trunc font-semibold">{d.title}</div>
        <div className="help flex flex-wrap items-center gap-x-2">
          {d.issuer && <span>{d.issuer}</span>}
          {(d.issue_date || d.period_to) && <span className="num">{dayLabel(d.issue_date || d.period_to, lang, { weekday: false, year: true })}</span>}
          {d.amount !== null && d.amount !== undefined && <span className="num">{money(d.amount, d.currency, lang)}</span>}
          {d.pages > 1 && <span>{t("pages_n", { n: d.pages })}</span>}
        </div>
        {snippet && <SnippetText snippet={snippet.snippet} />}
      </div>
      <div className="flex flex-wrap items-center gap-1.5">
        <KindChip kind={d.kind} label={d.kind_label} />
        {d.state === "review" && <Chip className="chip-amber">{t("needs_review")}</Chip>}
        {d.state === "archived" && <Chip>{t("archived")}</Chip>}
        {d.source === "mail" && <Chip>{t("src_mail")}</Chip>}
        {d.next_deadline && <Chip className="chip-accent">{dayLabel(d.next_deadline.date, lang, { weekday: false })}</Chip>}
      </div>
    </a>
  );
}

export function SnippetText({ snippet }) {
  const parts = [];
  const re = /\*\*(.+?)\*\*/gs;   // the search tool marks hits as **word**
  let last = 0;
  let m;
  const text = snippet || "";
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) parts.push({ text: text.slice(last, m.index), hit: false });
    parts.push({ text: m[1], hit: true });
    last = m.index + m[0].length;
  }
  if (last < text.length) parts.push({ text: text.slice(last), hit: false });
  return (
    <p className="help clamp2" style={{ overflowWrap: "anywhere" }}>
      {parts.map((p, i) => p.hit ? <mark key={i} style={{ background: "var(--accent-soft)", color: "var(--ink)", borderRadius: 3, padding: "0 2px" }}>{p.text}</mark> : <span key={i}>{p.text}</span>)}
    </p>
  );
}
