import React, { useEffect, useState } from "react";
import { api } from "../api.js";
import { useApp } from "../context.js";
import { DocRow } from "../components/cards.jsx";
import UploadZone from "../components/Upload.jsx";
import { Empty, ErrorBox, Field, useLoad } from "../components/ui.jsx";
import { DOC_KINDS } from "../meta.js";
import Detalle from "./Detalle.jsx";

function useDebounced(value, ms = 250) {
  const [v, setV] = useState(value);
  useEffect(() => { const id = setTimeout(() => setV(value), ms); return () => clearTimeout(id); }, [value, ms]);
  return v;
}

function List() {
  const { t, version } = useApp();
  const [q, setQ] = useState("");
  const [kind, setKind] = useState("");
  const [year, setYear] = useState("");
  const [state, setState] = useState("");
  const dq = useDebounced(q.trim());
  const searching = dq.length >= 2;
  const { data, error } = useLoad(async () => {
    if (searching) return { mode: "search", ...(await api.call("doc_search", { query: dq, kind, year, state, limit: 40 })) };
    return { mode: "list", ...(await api.call("docs_list", { kind, year, state, limit: 200 })) };
  }, [dq, kind, year, state, version]);
  const thisYear = new Date().getFullYear();
  const years = Array.from({ length: 8 }, (_, i) => String(thisYear - i));

  return (
    <div className="space-y-4">
      <header>
        <h1>{t("nav_documents")}</h1>
        <p className="help">{t("documents_intro")}</p>
      </header>
      <UploadZone />
      <div className="grid gap-3 sm:grid-cols-[minmax(0,2fr)_repeat(3,minmax(0,1fr))]">
        <Field label={t("search")}><input className="field" type="search" value={q} onChange={(e) => setQ(e.target.value)} placeholder={t("search_ph")} /></Field>
        <Field label={t("kind")}>
          <select className="field" value={kind} onChange={(e) => setKind(e.target.value)}>
            <option value="">{t("all")}</option>
            {DOC_KINDS.map((k) => <option key={k} value={k}>{t(`kind_${k}`)}</option>)}
          </select>
        </Field>
        <Field label={t("year")}>
          <select className="field" value={year} onChange={(e) => setYear(e.target.value)}>
            <option value="">{t("all")}</option>
            {years.map((y) => <option key={y} value={y}>{y}</option>)}
          </select>
        </Field>
        <Field label={t("state")}>
          <select className="field" value={state} onChange={(e) => setState(e.target.value)}>
            <option value="">{t("state_active")}</option>
            <option value="review">{t("needs_review")}</option>
            <option value="ok">{t("state_ok")}</option>
            <option value="archived">{t("archived")}</option>
          </select>
        </Field>
      </div>
      <ErrorBox error={error} />
      {data?.mode === "search" && (
        data.results.length === 0 ? <Empty>{t("no_matches", { q: dq })}</Empty> : (
          <div className="space-y-2">
            <p className="help">{t("matches_n", { n: data.count })}</p>
            {data.results.map((r, i) => (
              <DocRow key={`${r.doc_id}-${r.page}-${i}`} d={{ id: r.doc_id, title: r.title, issuer: r.issuer, kind: r.kind, kind_label: t(`kind_${r.kind}`), issue_date: r.issue_date, pages: 0 }}
                snippet={{ snippet: r.snippet, page: r.page }} />
            ))}
          </div>
        )
      )}
      {data?.mode === "list" && (
        data.documents.length === 0 ? <Empty>{t("no_documents")}</Empty> : (
          <div className="space-y-2">
            <p className="help">{t("documents_n", { n: data.count })}</p>
            {data.documents.map((d) => <DocRow key={d.id} d={d} />)}
          </div>
        )
      )}
    </div>
  );
}

export default function Documentos({ param, query }) {
  if (param) return <Detalle id={param} query={query} />;
  return <List />;
}
