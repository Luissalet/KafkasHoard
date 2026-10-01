import React from "react";
import { api } from "../api.js";
import { useApp } from "../context.js";
import { DocRow } from "../components/cards.jsx";
import { Busy, Chip, Empty, ErrorBox, Icon, ICONS, Rel, Section, useBusy, useLoad } from "../components/ui.jsx";
import { useMailScan } from "../components/hooks.js";

function MailCard({ m, onDone }) {
  const { t, notify, changed } = useApp();
  const [busy, run] = useBusy();
  const accept = () => run("a", async () => {
    const r = await api.call("mail_accept", { message_id: m.message_id });
    notify(t("mail_filed", { n: r.documents.length }));
    changed();
    await onDone();
  });
  const ignore = () => run("i", async () => { await api.call("mail_ignore", { message_id: m.message_id }); notify(t("mail_ignored")); changed(); await onDone(); });
  return (
    <article className="panel space-y-2" aria-label={m.subject}>
      <div className="flex flex-wrap items-start gap-x-3 gap-y-1">
        <div className="min-w-0 flex-1">
          <div className="font-semibold" style={{ overflowWrap: "anywhere" }}>{m.subject || t("no_subject")}</div>
          <div className="help">{m.from_name || m.from_address} · <Rel ts={m.ts} /></div>
        </div>
        <div className="flex flex-wrap gap-1.5">
          {(m.reasons || []).slice(0, 4).map((r) => <Chip key={r}>{r}</Chip>)}
          {m.attachments?.length > 0 && <Chip className="chip-accent">{t("attachments_n", { n: m.attachments.length })}</Chip>}
        </div>
      </div>
      {m.snippet && <p className="help clamp2" style={{ overflowWrap: "anywhere" }}>{m.snippet}</p>}
      <div className="flex flex-wrap gap-2">
        <Busy className="btn btn-sm btn-primary" busy={busy.a} onClick={accept}><Icon d={ICONS.check} size={13} />{t("file_it")}</Busy>
        <Busy className="btn btn-sm" busy={busy.i} onClick={ignore}>{t("ignore")}</Busy>
      </div>
    </article>
  );
}

export default function Revision() {
  const { t, version, refreshDash } = useApp();
  const docs = useLoad(() => api.call("docs_list", { state: "review", limit: 100 }), [version]);
  const mails = useLoad(() => api.call("mail_list", { kind: "maybe", state: "new", limit: 50 }), [version]);
  const [scanning, scan] = useMailScan(async () => { await mails.reload(); });
  const reloadAll = async () => { await Promise.all([docs.reload(), mails.reload()]); refreshDash(); };
  const d = docs.data?.documents || [];
  const m = mails.data?.mails || [];
  return (
    <div className="space-y-6">
      <header className="flex flex-wrap items-end gap-3">
        <div className="min-w-0 flex-1">
          <h1>{t("nav_review")}</h1>
          <p className="help">{t("review_intro")}</p>
        </div>
        <Busy className="btn btn-sm" busy={scanning} onClick={() => scan({})}><Icon d={ICONS.mail} size={14} />{t("read_mail_now")}</Busy>
      </header>
      <Section id="sec-rdocs" title={t("review_docs")} count={d.length}>
        <ErrorBox error={docs.error} />
        {d.length === 0 ? <Empty>{t("review_docs_empty")}</Empty> : <div className="space-y-2">{d.map((x) => <DocRow key={x.id} d={x} />)}</div>}
      </Section>
      <Section id="sec-rmails" title={t("review_mails")} count={m.length}>
        <ErrorBox error={mails.error} />
        {m.length === 0 ? <Empty>{t("review_mails_empty")}</Empty> : <div className="space-y-2">{m.map((x) => <MailCard key={x.message_id} m={x} onDone={reloadAll} />)}</div>}
      </Section>
    </div>
  );
}
