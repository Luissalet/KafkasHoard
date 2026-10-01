import React, { useEffect, useRef, useState } from "react";
import { api } from "../api.js";
import { useApp } from "../context.js";
import { AddDeadline, DeadlineCard, DocRow } from "../components/cards.jsx";
import { Busy, Chip, ErrorBox, Field, Icon, ICONS, KindChip, Rel, Section, useBusy, useLoad } from "../components/ui.jsx";
import { dayLabel, fileSize, money } from "../format.js";
import { DOC_KINDS, WARRANTY_KINDS } from "../meta.js";

const FIELD_LABEL = { kind: "f_kind", issuer: "f_issuer", ref: "f_ref", amount: "f_amount", "date:issue": "f_issue", "date:due": "f_due", "date:effect_from": "f_effect_from",
  "date:effect_to": "f_effect_to", "date:renewal": "f_renewal", "date:purchase": "f_purchase", "date:delivery": "f_delivery", "date:expiry": "f_expiry", "date:notified": "f_notified",
  "date:itv_next": "f_itv", "date:permanence_end": "f_permanence", "date:period_from": "f_period_from", "date:period_to": "f_period_to", "date:offence": "f_offence" };

function Preview({ doc, pages, initialPage }) {
  const { t } = useApp();
  const total = Math.max(1, doc.pages || pages.length || 1);
  const [page, setPage] = useState(Math.min(Math.max(1, initialPage || 1), total));
  const [failed, setFailed] = useState(false);
  const ref = useRef(null);
  useEffect(() => { if (initialPage) { setPage(Math.min(Math.max(1, initialPage), total)); ref.current?.scrollIntoView({ block: "nearest" }); } }, [initialPage, total]);
  useEffect(() => setFailed(false), [page, doc.id]);
  const visual = doc.has_file && (doc.mime === "application/pdf" || (doc.mime || "").startsWith("image/"));
  const text = pages.find((p) => p.page === page)?.text || "";
  return (
    <section className="space-y-2" aria-label={t("preview")} ref={ref}>
      <div className="flex flex-wrap items-center gap-2">
        <h2>{t("preview")}</h2>
        {total > 1 && (
          <div className="ml-auto flex items-center gap-1.5">
            <button type="button" className="btn btn-sm" disabled={page <= 1} onClick={() => setPage(page - 1)} aria-label={t("prev_page")}><Icon d={ICONS.back} size={13} /></button>
            <span className="num help">{t("page_of", { n: page, total })}</span>
            <button type="button" className="btn btn-sm" disabled={page >= total} onClick={() => setPage(page + 1)} aria-label={t("next_page")}><Icon d={ICONS.chevron} size={13} /></button>
          </div>
        )}
        {doc.has_file && <a className="btn btn-sm" href={api.fileUrl(doc.id)} target="_blank" rel="noopener noreferrer"><Icon d={ICONS.external} size={13} />{t("open_original")}</a>}
      </div>
      {visual && !failed ? (
        <img src={api.pageUrl(doc.id, page)} alt={t("page_alt", { n: page })} className="rounded-md border" style={{ borderColor: "var(--line)", background: "#fff", width: "100%" }} onError={() => setFailed(true)} />
      ) : (
        <pre className="mono panel overflow-auto" style={{ maxHeight: 520, whiteSpace: "pre-wrap" }}>{text || t("no_text")}</pre>
      )}
      {visual && text && (
        <details>
          <summary className="font-semibold">{t("read_text")}</summary>
          <pre className="mono panel mt-2 overflow-auto" style={{ maxHeight: 360, whiteSpace: "pre-wrap" }}>{text}</pre>
        </details>
      )}
    </section>
  );
}

function Fields({ doc, onSaved }) {
  const { t, notify } = useApp();
  const [busy, run] = useBusy();
  const init = () => ({ title: doc.title || "", kind: doc.kind, issuer: doc.issuer || "", ref: doc.ref || "", amount: doc.amount ?? "", issue_date: doc.issue_date || "",
    period_from: doc.period_from || "", period_to: doc.period_to || "", item: doc.item || "", tags: (doc.tags || []).join(", "), notes: doc.notes || "",
    warranty_years: doc.warranty_months ? String(doc.warranty_months / 12) : "" });
  const [form, setForm] = useState(init);
  useEffect(() => setForm(init()), [doc]); // eslint-disable-line react-hooks/exhaustive-deps
  const set = (k, v) => setForm((f) => ({ ...f, [k]: v }));
  const edited = new Set(doc.edited || []);
  const mark = (k) => (edited.has(k) ? <Chip className="chip-accent" title={t("edited_hint")}>{t("edited")}</Chip> : null);
  const save = (e) => {
    e.preventDefault();
    const values = { doc: doc.id };
    const same = (k, a, b) => String(a ?? "") === String(b ?? "");
    for (const k of ["title", "kind", "issuer", "ref", "issue_date", "period_from", "period_to", "item", "notes"]) {
      if (!same(k, form[k], doc[k] ?? "")) values[k] = form[k];
    }
    if (!same("amount", form.amount, doc.amount ?? "") && form.amount !== "") values.amount = Number(String(form.amount).replace(",", "."));
    if (!same("tags", form.tags, (doc.tags || []).join(", "))) values.tags = form.tags.split(/[,;]/).map((s) => s.trim()).filter(Boolean);
    if (form.warranty_years !== "" && !same("w", form.warranty_years, doc.warranty_months ? String(doc.warranty_months / 12) : "")) values.warranty_years = Number(String(form.warranty_years).replace(",", "."));
    if (Object.keys(values).length === 1) return;
    run("save", async () => { await api.call("doc_update", values); notify(t("saved")); await onSaved(); });
  };
  return (
    <form className="panel space-y-3" onSubmit={save} noValidate aria-label={t("fields")}>
      <Field label={<>{t("title")} {mark("title")}</>}><input className="field" value={form.title} onChange={(e) => set("title", e.target.value)} /></Field>
      <div className="grid gap-3 sm:grid-cols-2">
        <Field label={<>{t("kind")} {mark("kind")}</>}>
          <select className="field" value={form.kind} onChange={(e) => set("kind", e.target.value)}>
            {DOC_KINDS.map((k) => <option key={k} value={k}>{t(`kind_${k}`)}</option>)}
          </select>
        </Field>
        <Field label={<>{t("f_issuer")} {mark("issuer")}</>}><input className="field" value={form.issuer} onChange={(e) => set("issuer", e.target.value)} /></Field>
        <Field label={<>{t("f_ref")} {mark("ref")}</>}><input className="field" value={form.ref} onChange={(e) => set("ref", e.target.value)} /></Field>
        <Field label={<>{t("f_amount")} {mark("amount")}</>}><input className="field num" inputMode="decimal" value={form.amount} onChange={(e) => set("amount", e.target.value)} /></Field>
        <Field label={<>{t("f_issue")} {mark("issue_date")}</>}><input className="field" type="date" value={form.issue_date} onChange={(e) => set("issue_date", e.target.value)} /></Field>
        <Field label={<>{t("f_item")} {mark("item")}</>}><input className="field" value={form.item} onChange={(e) => set("item", e.target.value)} /></Field>
        <Field label={<>{t("f_period_from")} {mark("period_from")}</>}><input className="field" type="date" value={form.period_from} onChange={(e) => set("period_from", e.target.value)} /></Field>
        <Field label={<>{t("f_period_to")} {mark("period_to")}</>}><input className="field" type="date" value={form.period_to} onChange={(e) => set("period_to", e.target.value)} /></Field>
        {WARRANTY_KINDS.has(form.kind) && <Field label={t("warranty_years")} hint={t("warranty_years_hint")}><input className="field num" inputMode="decimal" value={form.warranty_years} onChange={(e) => set("warranty_years", e.target.value)} placeholder="3" /></Field>}
        <Field label={t("tags")} hint={t("tags_hint")}><input className="field" value={form.tags} onChange={(e) => set("tags", e.target.value)} /></Field>
      </div>
      <Field label={t("notes")}><textarea className="field" rows={2} value={form.notes} onChange={(e) => set("notes", e.target.value)} /></Field>
      <Busy type="submit" className="btn btn-primary btn-sm" busy={busy.save}>{t("save")}</Busy>
    </form>
  );
}

function SeriesBlock({ series }) {
  const { t, lang } = useApp();
  if (!series || series.history.length < 2) return null;
  return (
    <Section id="sec-series" title={t("series_title")} count={series.history.length}>
      <div className="panel overflow-x-auto p-0">
        <table className="grid">
          <thead><tr><th>{t("period")}</th><th>{t("document")}</th><th className="r">{t("f_amount")}</th><th className="r">{t("change")}</th></tr></thead>
          <tbody>
            {series.history.map((h) => (
              <tr key={h.doc_id}>
                <td className="num whitespace-nowrap">{h.date ? dayLabel(h.date, lang, { weekday: false, year: true }) : "—"}</td>
                <td><a href={`#/documentos/${h.doc_id}`}>{h.title}</a></td>
                <td className="r num">{money(h.amount, h.currency, lang)}</td>
                <td className="r num" style={{ color: h.pct > 0 ? "var(--danger)" : h.pct < 0 ? "var(--ok)" : undefined }}>{h.pct === null || h.pct === undefined ? "—" : `${h.pct > 0 ? "+" : ""}${h.pct} %`}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Section>
  );
}

export default function Detalle({ id, query }) {
  const { t, lang, notify, confirm, changed, version } = useApp();
  const [busy, run] = useBusy();
  const [adding, setAdding] = useState(false);
  const [useModel, setUseModel] = useState(false);
  const { data, error, reload } = useLoad(() => api.document(id), [id, version]);
  if (error && !data) return <div className="space-y-3"><a href="#/documentos" className="btn btn-sm"><Icon d={ICONS.back} size={13} />{t("nav_documents")}</a><ErrorBox error={error} /></div>;
  if (!data) return <div className="space-y-3"><h1>…</h1></div>;
  const doc = data.document;
  const refresh = async () => { changed(); await reload(); };
  const reprocess = () => run("rp", async () => {
    const r = await api.call("doc_reprocess", { doc: id, llm: useModel });
    notify(r.llm === "unavailable" ? t("model_unavailable") : t("reprocessed"));
    await refresh();
  });
  const remove = async () => {
    const ok = await confirm({ title: t("delete_doc"), message: t("delete_doc_msg", { title: doc.title }) });
    if (!ok) return;
    run("rm", async () => { await api.call("doc_delete", { doc: id, confirm: true }); notify(t("deleted")); changed(); window.location.hash = "#/documentos"; });
  };
  const setState = (state) => run("st", async () => { await api.call("doc_update", { doc: id, state }); notify(t("saved")); await refresh(); });
  const initialPage = Number(query?.get("p")) || 0;
  const facts = (data.facts || []).filter((f) => !["kind", "issuer"].includes(f.field) || f.evidence);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center gap-2">
        <a href="#/documentos" className="btn btn-sm"><Icon d={ICONS.back} size={13} />{t("nav_documents")}</a>
        <KindChip kind={doc.kind} label={doc.kind_label} />
        {doc.state === "review" && <Chip className="chip-amber">{t("needs_review")}</Chip>}
        {doc.state === "archived" && <Chip>{t("archived")}</Chip>}
        {doc.ocr && <Chip title={t("ocr_hint")}>OCR</Chip>}
        <Chip>{t(`src_${doc.source}`)}</Chip>
        <span className="help">{doc.file_name} {doc.size ? `· ${fileSize(doc.size)}` : ""} · <Rel ts={doc.created_ts} /></span>
      </div>
      <header>
        <h1 style={{ overflowWrap: "anywhere" }}>{doc.title}</h1>
        <p className="help">{[doc.issuer, doc.ref, doc.amount !== null ? money(doc.amount, doc.currency, lang) : ""].filter(Boolean).join(" · ")}</p>
      </header>
      {doc.state === "review" && (
        <div className="banner banner-warn flex flex-wrap items-center gap-3" role="status">
          <span>{t("review_explain")} {(data.extraction_notes || []).map((n) => t(`note_${n}`)).filter((x, i, a) => !x.startsWith("note_") && a.indexOf(x) === i).join(" ")}</span>
          <Busy className="btn btn-sm btn-primary" busy={busy.st} onClick={() => setState("ok")}><Icon d={ICONS.check} size={13} />{t("mark_ok")}</Busy>
        </div>
      )}
      {doc.notes_extraction?.some((n) => n.startsWith("unreadable") || n.includes("OCR is unavailable")) && <div className="banner banner-info">{t("ocr_missing")}</div>}

      <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <div className="min-w-0 space-y-5">
          <Preview doc={doc} pages={data.pages || []} initialPage={initialPage} />
        </div>
        <div className="min-w-0 space-y-5">
          <Section id="sec-dl" title={t("g_deadlines")} count={data.deadlines.length} actions={<button type="button" className="btn btn-sm" onClick={() => setAdding(true)}><Icon d={ICONS.plus} size={13} />{t("add_deadline")}</button>}>
            {data.deadlines.length === 0 ? <p className="help">{t("no_deadlines")}</p> : (
              <div className="space-y-2">{data.deadlines.map((d) => <DeadlineCard key={d.id} d={d} showDoc={false} onChanged={reload} />)}</div>
            )}
          </Section>
          <Fields doc={doc} onSaved={refresh} />
          <SeriesBlock series={data.series} />
          {data.neighbours?.length > 0 && (
            <Section id="sec-neigh" title={t("same_series")}><div className="space-y-2">{data.neighbours.map((n) => <DocRow key={n.id} d={n} />)}</div></Section>
          )}
          <details>
            <summary className="font-semibold">{t("facts_title")} <Chip>{facts.length}</Chip></summary>
            <div className="panel mt-2 overflow-x-auto p-0">
              <table className="grid">
                <thead><tr><th>{t("field")}</th><th>{t("value")}</th><th>{t("evidence")}</th></tr></thead>
                <tbody>
                  {facts.map((f, i) => (
                    <tr key={i}>
                      <td className="whitespace-nowrap">{FIELD_LABEL[f.field] ? t(FIELD_LABEL[f.field]) : f.field}</td>
                      <td className="num">{String(f.value)}</td>
                      <td className="help" style={{ overflowWrap: "anywhere" }}>{f.page ? <a href={`#/documentos/${doc.id}?p=${f.page}`}>p. {f.page}</a> : null} {f.evidence}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </details>
          {data.mails?.length > 0 && (
            <Section id="sec-mail" title={t("from_mail")}>
              {data.mails.map((m) => <div key={m.message_id} className="panel panel-tight"><div className="font-semibold">{m.subject}</div><div className="help">{m.from_name || m.from_address} · <Rel ts={m.ts} /></div></div>)}
            </Section>
          )}
          {data.notifications?.length > 0 && (
            <details>
              <summary className="font-semibold">{t("sent_notifications")} <Chip>{data.notifications.length}</Chip></summary>
              <ul className="help mt-2 space-y-1 pl-4">{data.notifications.map((n) => <li key={n.id}>{n.title} · <Rel ts={n.ts} /></li>)}</ul>
            </details>
          )}
          <div className="panel flex flex-wrap items-center gap-3">
            <Busy className="btn btn-sm" busy={busy.rp} onClick={reprocess}><Icon d={ICONS.refresh} size={13} />{t("reprocess")}</Busy>
            <label className="inline-flex items-center gap-2 help"><input type="checkbox" checked={useModel} onChange={(e) => setUseModel(e.target.checked)} />{t("ask_model")}</label>
            {doc.state !== "archived" ? <Busy className="btn btn-sm" busy={busy.st} onClick={() => setState("archived")}><Icon d={ICONS.archive} size={13} />{t("archive")}</Busy> : <Busy className="btn btn-sm" busy={busy.st} onClick={() => setState("ok")}>{t("unarchive")}</Busy>}
            <Busy className="btn btn-sm btn-danger ml-auto" busy={busy.rm} onClick={remove}><Icon d={ICONS.trash} size={13} />{t("delete")}</Busy>
          </div>
          {data.llm && <p className="help">{data.llm.used ? t("model_used") : t("model_not_used", { why: data.llm.error || "" })}</p>}
        </div>
      </div>
      {adding && <AddDeadline docId={id} onClose={() => setAdding(false)} onSaved={async () => { setAdding(false); await reload(); }} />}
    </div>
  );
}
