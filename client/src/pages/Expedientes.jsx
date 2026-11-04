import React, { useState } from "react";
import { api } from "../api.js";
import { useApp } from "../context.js";
import { Busy, Chip, Empty, ErrorBox, Field, Icon, ICONS, Rel, Section, useBusy, useLoad } from "../components/ui.jsx";

const STATE_CLASS = { fresh: "chip-ok", changed: "chip-amber", missing: "chip-danger", stale: "chip-amber" };

function DocPicker({ docs, picked, setPicked }) {
  const { t } = useApp();
  if (!docs.length) return <Empty>{t("no_documents")}</Empty>;
  return (
    <div className="panel panel-tight space-y-1" style={{ maxHeight: 220, overflowY: "auto" }}>
      {docs.map((d) => (
        <label key={d.id} className="flex items-center gap-2 text-[13px]">
          <input type="checkbox" checked={picked.includes(d.id)} onChange={(e) => setPicked(e.target.checked ? [...picked, d.id] : picked.filter((x) => x !== d.id))} />
          <span className="min-w-0 flex-1" style={{ overflowWrap: "anywhere" }}>{d.title}</span>
          <span className="help">{d.id}</span>
        </label>
      ))}
    </div>
  );
}

function Cites({ items }) {
  return <span className="help" style={{ overflowWrap: "anywhere" }}>{items.join(" · ")}</span>;
}

function Positions({ positions }) {
  return (
    <div className="grid gap-2 sm:grid-cols-2">
      {positions.map((p, i) => (
        <div key={i} className="panel panel-tight space-y-1">
          <div className="font-semibold">{p.display}</div>
          {(p.sources || []).map((s, k) => <div key={k} className="help" style={{ overflowWrap: "anywhere" }}>{s.cite || s} {s.text ? `· ${s.text}` : ""}</div>)}
        </div>
      ))}
    </div>
  );
}

function Conclusion({ d }) {
  const { t } = useApp();
  const c = d.conclusion;
  const groups = d.synthesis.contradictions;
  return (
    <div className="space-y-4">
      <Section id="sec-conflicts" title={t("dos_conflicts")} count={groups.length}>
        {groups.length === 0 ? <Empty>{t("dos_no_conflicts")}</Empty> : groups.map((g) => (
          <article key={g.id} className="panel space-y-2" aria-label={g.subject}>
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-semibold">{g.subject}</span>
              <Chip className="chip-amber">{t(`dos_nature_${g.nature || "disjoint"}`)}</Chip>
              {!g.relevant && <Chip>{t("dos_other")}</Chip>}
            </div>
            <Positions positions={g.positions} />
            <p className="help">{t("dos_not_resolved")}</p>
          </article>
        ))}
      </Section>
      <Section id="sec-agreed" title={t("dos_agreed")} count={c.agreed.length}>
        {c.agreed.length === 0 ? <Empty>{t("dos_no_agreed")}</Empty> : c.agreed.map((a) => (
          <div key={a.group} className="panel panel-tight"><div>{a.statement}</div><Cites items={a.sources} /></div>
        ))}
      </Section>
      <Section id="sec-questions" title={t("dos_questions")} count={c.open_questions.length}>
        {c.open_questions.length === 0 ? <Empty>{t("dos_no_questions")}</Empty> : (
          <ul className="list-disc space-y-1 pl-5">{c.open_questions.map((q, i) => <li key={i} style={{ overflowWrap: "anywhere" }}>{q}</li>)}</ul>
        )}
      </Section>
    </div>
  );
}

function SourceBlock({ sec }) {
  const { t } = useApp();
  return (
    <article className="panel space-y-2" aria-label={sec.title}>
      <div className="flex flex-wrap items-center gap-2">
        <a className="font-semibold" href={`#/documentos/${sec.doc_id}`}>{sec.title || sec.doc_id}</a>
        {sec.ocr && <Chip className="chip-amber">OCR</Chip>}
        {sec.table_rows > 0 && <Chip>{t("dos_table_rows", { n: sec.table_rows })}</Chip>}
        <Chip>{t("dos_claims_n", { n: sec.claims_total })}</Chip>
      </div>
      {sec.passages.length === 0 ? <p className="help">{t("dos_no_passages")}</p> : (
        <ul className="space-y-1">{sec.passages.map((p) => <li key={p.cite} style={{ overflowWrap: "anywhere" }}>{p.text} <span className="help">{p.cite}</span></li>)}</ul>
      )}
      <details>
        <summary className="help">{t("dos_claims")}</summary>
        <div className="mt-2 overflow-x-auto">
          <table className="w-full text-left text-[12px]">
            <thead><tr><th>{t("dos_claim")}</th><th>{t("dos_value")}</th><th>{t("dos_cite")}</th></tr></thead>
            <tbody>
              {sec.claims.map((c) => (
                <tr key={c.id}><td style={{ overflowWrap: "anywhere" }}>{c.claim}</td><td>{c.kind === "requirement" ? t(`dos_req_${c.value}`) : `${c.raw}`}</td><td>{c.cite}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </article>
  );
}

function Detail({ id }) {
  const { t, version, changed, notify, confirm } = useApp();
  const [busy, run] = useBusy();
  const { data, error, reload } = useLoad(() => api.call("kafka_dossier_status", { dossier: id, detail: "full" }), [id, version]);
  const d = data?.dossier;
  const st = data?.status;
  const refresh = () => run("r", async () => {
    const r = await api.call("kafka_dossier_refresh", { dossier: id, detail: "full" });
    notify(r.unchanged ? t("dos_up_to_date") : t("dos_refreshed", { n: r.recomputed.length }));
    changed();
    await reload();
  });
  const remove = () => run("d", async () => {
    if (!(await confirm({ title: t("dos_delete"), message: t("dos_delete_msg") }))) return;
    await api.call("kafka_dossier_delete", { dossier: id, confirm: true });
    window.location.hash = "#/expedientes";
    changed();
  });
  return (
    <div className="space-y-5">
      <a href="#/expedientes" className="btn-link"><Icon d={ICONS.back} size={14} /> {t("nav_dossiers")}</a>
      <ErrorBox error={error} />
      {d && (
        <>
          <header className="space-y-1">
            <h1 style={{ overflowWrap: "anywhere" }}>{d.question}</h1>
            <div className="flex flex-wrap items-center gap-2">
              <Chip className={STATE_CLASS[st.state]}>{t(`dos_state_${st.state}`)}</Chip>
              <span className="help"><Rel ts={d.updated_ts} /></span>
              {st.state === "stale" && <Busy className="btn btn-sm btn-primary" busy={busy.r} onClick={refresh}><Icon d={ICONS.refresh} size={13} />{t("dos_refresh")}</Busy>}
              <Busy className="btn btn-sm btn-danger" busy={busy.d} onClick={remove}><Icon d={ICONS.trash} size={13} />{t("delete")}</Busy>
            </div>
            <p className="help">{t("dos_rules_only")}</p>
          </header>
          {st.state === "stale" && (
            <div className="banner banner-warn" role="status">
              {t("dos_stale_msg")} {st.sources.filter((s) => s.state !== "fresh").map((s) => `${s.title} (${t(`dos_state_${s.state}`)})`).join(", ")}
            </div>
          )}
          <Conclusion d={d} />
          <Section id="sec-sources" title={t("dos_sources")} count={Object.keys(d.sections).length}>
            <div className="space-y-2">{Object.values(d.sections).map((sec) => <SourceBlock key={sec.doc_id} sec={sec} />)}</div>
          </Section>
        </>
      )}
    </div>
  );
}

function Diff() {
  const { t, lang } = useApp();
  const [busy, run] = useBusy();
  const docs = useLoad(() => api.call("docs_list", { limit: 200 }), []);
  const [before, setBefore] = useState("");
  const [after, setAfter] = useState("");
  const [result, setResult] = useState(null);
  const list = docs.data?.documents || [];
  const go = () => run("g", async () => setResult(await api.call("kafka_requirements_diff", { before, after, lang })));
  return (
    <Section id="sec-diff" title={t("dos_diff")}>
      <p className="help">{t("dos_diff_intro")}</p>
      <div className="grid gap-3 sm:grid-cols-[repeat(2,minmax(0,1fr))_auto] sm:items-end">
        <Field label={t("dos_before")}><select className="field" value={before} onChange={(e) => setBefore(e.target.value)}><option value="" />{list.map((d) => <option key={d.id} value={d.id}>{d.title}</option>)}</select></Field>
        <Field label={t("dos_after")}><select className="field" value={after} onChange={(e) => setAfter(e.target.value)}><option value="" />{list.map((d) => <option key={d.id} value={d.id}>{d.title}</option>)}</select></Field>
        <Busy className="btn btn-primary" busy={busy.g} disabled={!before || !after} onClick={go}>{t("dos_compare")}</Busy>
      </div>
      {result && (
        <div className="space-y-2">
          <p className="help">{t("dos_diff_summary", result.summary)}</p>
          {result.changes.length === 0 ? <Empty>{t("dos_diff_none")}</Empty> : result.changes.map((c) => (
            <article key={c.index} className="panel space-y-1">
              <div className="flex flex-wrap items-center gap-2">
                <Chip className={c.change === "added" ? "chip-ok" : c.change === "removed" ? "chip-danger" : "chip-amber"}>{t(`dos_change_${c.change}`)}</Chip>
                {c.clause && <span className="font-semibold">{t("dos_clause", { n: c.clause })}</span>}
              </div>
              <div className="grid gap-2 sm:grid-cols-2">
                <div style={{ overflowWrap: "anywhere" }}>{c.before ? <><div>{c.before.text}</div><div className="help">{c.before.cite}</div></> : <span className="help">—</span>}</div>
                <div style={{ overflowWrap: "anywhere" }}>{c.after ? <><div>{c.after.text}</div><div className="help">{c.after.cite}</div></> : <span className="help">—</span>}</div>
              </div>
              {c.claims.map((k, i) => (
                <div key={i} className="help">
                  {k.label || k.subject}: {k.before.map((x) => x.display).join(", ") || "—"} → {k.after.map((x) => x.display).join(", ") || "—"}
                </div>
              ))}
            </article>
          ))}
        </div>
      )}
    </Section>
  );
}

function List() {
  const { t, lang, version, changed } = useApp();
  const [busy, run] = useBusy();
  const [question, setQuestion] = useState("");
  const [picked, setPicked] = useState([]);
  const docs = useLoad(() => api.call("docs_list", { limit: 200 }), [version]);
  const dossiers = useLoad(() => api.call("kafka_dossier_status", {}), [version]);
  const create = () => run("c", async () => {
    const r = await api.call("kafka_dossier", { question, doc_ids: picked.length ? picked : undefined, lang, detail: "summary" });
    setQuestion("");
    setPicked([]);
    changed();
    window.location.hash = `#/expedientes/${r.id}`;
  });
  const items = dossiers.data?.dossiers || [];
  return (
    <div className="space-y-6">
      <header>
        <h1>{t("nav_dossiers")}</h1>
        <p className="help">{t("dos_intro")}</p>
      </header>
      <Section id="sec-new" title={t("dos_new")}>
        <Field label={t("dos_question")} hint={t("dos_question_hint")}>
          <input className="field" value={question} maxLength={400} onChange={(e) => setQuestion(e.target.value)} placeholder={t("dos_question_ph")} />
        </Field>
        <Field label={t("dos_pick")} hint={t("dos_pick_hint")}>
          <DocPicker docs={docs.data?.documents || []} picked={picked} setPicked={setPicked} />
        </Field>
        <Busy className="btn btn-primary" busy={busy.c} disabled={question.trim().length < 3} onClick={create}><Icon d={ICONS.plus} size={14} />{t("dos_create")}</Busy>
      </Section>
      <Section id="sec-list" title={t("dos_list")} count={items.length}>
        <ErrorBox error={dossiers.error} />
        {items.length === 0 ? <Empty>{t("dos_empty")}</Empty> : (
          <div className="space-y-2">
            {items.map((s) => (
              <a key={s.id} href={`#/expedientes/${s.id}`} className="panel panel-tight flex flex-wrap items-center gap-2" style={{ textDecoration: "none", color: "inherit" }}>
                <span className="min-w-0 flex-1" style={{ overflowWrap: "anywhere" }}>{s.question}</span>
                <Chip className={STATE_CLASS[s.state]}>{t(`dos_state_${s.state}`)}</Chip>
                <span className="help">{t("dos_sources_n", { n: s.sources.length })}</span>
              </a>
            ))}
          </div>
        )}
      </Section>
      <Diff />
    </div>
  );
}

export default function Expedientes({ param }) {
  if (param) return <Detail id={param} />;
  return <List />;
}
