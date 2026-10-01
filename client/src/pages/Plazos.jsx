import React, { useState } from "react";
import { api } from "../api.js";
import { useApp } from "../context.js";
import { AddDeadline, DeadlineCard, DocRow } from "../components/cards.jsx";
import { Busy, Chip, Empty, Icon, ICONS, Rel, Section, useBusy } from "../components/ui.jsx";

function News({ news, onSeen }) {
  const { t } = useApp();
  const [busy, run] = useBusy();
  if (!news?.length) return null;
  return (
    <div className="banner banner-info flex flex-wrap items-start gap-3" role="status">
      <div className="min-w-0 flex-1 space-y-1">
        <div className="font-semibold">{t("news_title", { n: news.length })}</div>
        <ul className="m-0 list-none space-y-0.5 p-0">
          {news.slice(0, 6).map((n) => (
            <li key={n.id} className="flex flex-wrap gap-x-2">
              <span style={{ overflowWrap: "anywhere" }}>{n.doc_id ? <a href={`#/documentos/${n.doc_id}`}>{n.title}</a> : n.title}</span>
              <Rel ts={n.ts} />
            </li>
          ))}
        </ul>
      </div>
      <Busy className="btn btn-sm" busy={busy.seen} onClick={() => run("seen", onSeen)}>{t("mark_seen")}</Busy>
    </div>
  );
}

function Welcome({ status }) {
  const { t } = useApp();
  const inbox = status?.folders?.find((f) => f.inbox)?.path;
  return (
    <div className="panel space-y-3">
      <h2>{t("welcome_title")}</h2>
      <ol className="help m-0 space-y-1 pl-5">
        <li>{t("welcome_1")}</li>
        <li>{t("welcome_2")}{inbox ? <> <span className="mono">{inbox}</span></> : null}</li>
        <li>{t("welcome_3")}</li>
      </ol>
      <div className="flex flex-wrap gap-2">
        <a className="btn btn-primary btn-sm" href="#/documentos"><Icon d={ICONS.upload} size={14} />{t("upload_docs")}</a>
        <a className="btn btn-sm" href="#/ajustes">{t("nav_settings")}</a>
      </div>
    </div>
  );
}

export default function Plazos() {
  const { t, dash, refreshDash, changed, notify } = useApp();
  const [adding, setAdding] = useState(false);
  const [busy, run] = useBusy();
  if (!dash) return <div className="space-y-3"><h1>{t("nav_deadlines")}</h1><p className="help">…</p></div>;
  const counts = dash.counts || {};
  const empty = !counts.documents && !counts.deadlines_open;
  const seen = async () => { await api.visit(); await refreshDash(); };
  const groups = [
    { id: "late", title: t("g_overdue"), rows: dash.overdue, tone: "danger" },
    { id: "week", title: t("g_week"), rows: dash.week },
    { id: "month", title: t("g_month"), rows: dash.month },
  ];
  const later = dash.later || [];
  const closed = dash.recently_closed || [];
  const scanNow = () => run("scan", async () => {
    const r = await api.call("folder_scan");
    notify(t("folder_scan_result", { n: r.new_documents ?? 0 }));
    changed();
  });
  const nDocs = counts.review ?? 0, nMails = counts.mails_review ?? 0;
  const reviewItems = [
    nDocs ? t(nDocs === 1 ? "review_docs_1" : "review_docs_n", { n: nDocs }) : "",
    nMails ? t(nMails === 1 ? "review_mails_1" : "review_mails_n", { n: nMails }) : "",
  ].filter(Boolean).join(" · ");
  const none = groups.every((g) => !g.rows?.length) && !later.length;

  return (
    <div className="space-y-6">
      <header className="flex flex-wrap items-end gap-3">
        <div className="min-w-0 flex-1">
          <h1>{t("nav_deadlines")}</h1>
          <p className="help">{t("deadlines_intro", { n: counts.deadlines_open ?? 0, d: counts.documents ?? 0 })}</p>
        </div>
        <Busy className="btn btn-sm" busy={busy.scan} onClick={scanNow}><Icon d={ICONS.refresh} size={14} />{t("scan_folders")}</Busy>
        <button type="button" className="btn btn-sm" onClick={() => setAdding(true)}><Icon d={ICONS.plus} size={14} />{t("add_deadline")}</button>
        <a className="btn btn-primary btn-sm" href="#/documentos"><Icon d={ICONS.upload} size={14} />{t("upload_docs")}</a>
      </header>

      <News news={dash.news} onSeen={seen} />
      {(counts.review > 0 || counts.mails_review > 0) && (
        <div className="banner banner-warn flex flex-wrap items-center gap-3" role="status">
          <span>{t("review_banner", { items: reviewItems })}</span>
          <a className="btn btn-sm" href="#/revision">{t("nav_review")}</a>
        </div>
      )}
      {empty && <Welcome status={{ folders: dash.folders }} />}

      {groups.map((g) => g.rows?.length > 0 && (
        <Section key={g.id} id={`sec-${g.id}`} title={g.title} count={g.rows.length}>
          <div className="space-y-2">{g.rows.map((d) => <DeadlineCard key={d.id} d={d} onChanged={refreshDash} />)}</div>
        </Section>
      ))}
      {!empty && none && <Empty>{t("nothing_pending")}</Empty>}

      {later.length > 0 && (
        <details>
          <summary className="font-semibold">{t("g_later")} <Chip>{later.length}</Chip></summary>
          <div className="mt-2 space-y-2">{later.map((d) => <DeadlineCard key={d.id} d={d} onChanged={refreshDash} />)}</div>
        </details>
      )}
      {closed.length > 0 && (
        <details>
          <summary className="font-semibold">{t("g_closed")} <Chip>{closed.length}</Chip></summary>
          <div className="mt-2 space-y-2">{closed.map((d) => <DeadlineCard key={d.id} d={d} onChanged={refreshDash} />)}</div>
        </details>
      )}
      {dash.recent_documents?.length > 0 && (
        <Section id="sec-recent" title={t("g_recent_docs")} actions={<a className="btn btn-sm" href="#/documentos">{t("see_all")}</a>}>
          <div className="space-y-2">{dash.recent_documents.slice(0, 5).map((d) => <DocRow key={d.id} d={d} />)}</div>
        </Section>
      )}
      {adding && <AddDeadline onClose={() => setAdding(false)} onSaved={async () => { setAdding(false); await refreshDash(); }} />}
    </div>
  );
}
