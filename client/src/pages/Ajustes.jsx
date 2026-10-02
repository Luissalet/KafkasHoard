import React, { useEffect, useState } from "react";
import { api } from "../api.js";
import { useApp } from "../context.js";
import { Busy, Chip, ErrorBox, Field, Icon, ICONS, Rel, Section, Switch, useBusy, useLoad } from "../components/ui.jsx";
import { useMailScan } from "../components/hooks.js";
import { duration, rel, shortClock } from "../format.js";
import { CHANNELS, DEADLINE_KINDS, REGIONS, SEVERITIES } from "../meta.js";

const CHANNEL_SECRETS = {
  telegram: [
    { name: "TELEGRAM_TOKEN", label: "telegram_token", secret: true },
    { name: "TELEGRAM_CHAT_ID", label: "telegram_chat_id" },
  ],
  ntfy: [
    { name: "NTFY_TOPIC", label: "ntfy_topic", secret: true },
    { name: "NTFY_TOKEN", label: "ntfy_token", secret: true },
  ],
  email: [
    { name: "SMTP_HOST", label: "smtp_host", placeholder: "smtp.ejemplo.com" },
    { name: "SMTP_PORT", label: "smtp_port", placeholder: "587" },
    { name: "SMTP_USER", label: "smtp_user", secret: true },
    { name: "SMTP_PASSWORD", label: "smtp_password", secret: true },
    { name: "SMTP_FROM", label: "smtp_from" },
    { name: "SMTP_TO", label: "smtp_to" },
  ],
};

// A group of settings with one Save button. fields: [{ key, type: text|number|switch|select, label, hint, options:[[value,label]] }]
function SettingsForm({ fields, settings, onSaved, children }) {
  const { t, notify } = useApp();
  const [busy, run] = useBusy();
  const original = (f) => settings[f.key] ?? "";
  const [draft, setDraft] = useState(() => Object.fromEntries(fields.map((f) => [f.key, original(f)])));
  useEffect(() => setDraft(Object.fromEntries(fields.map((f) => [f.key, original(f)]))), [settings]); // eslint-disable-line react-hooks/exhaustive-deps
  const dirty = fields.filter((f) => String(draft[f.key]) !== String(original(f)));
  const set = (key, value) => setDraft((d) => ({ ...d, [key]: value }));
  const save = (e) => {
    e.preventDefault();
    if (!dirty.length) return;
    run("save", async () => {
      await api.call("settings_set", { values: Object.fromEntries(dirty.map((f) => [f.key, String(draft[f.key])])) });
      notify(t("saved"));
      await onSaved();
    });
  };
  return (
    <form className="space-y-3" onSubmit={save} noValidate>
      <div className="grid gap-3 sm:grid-cols-2">
        {fields.map((f) => (
          <div key={f.key} className={f.wide ? "sm:col-span-2" : ""}>
            {f.type === "switch" ? (
              <div className="flex items-start gap-3">
                <Switch checked={draft[f.key] === "1"} onChange={(v) => set(f.key, v ? "1" : "0")} label={f.label} />
                <div><div className="font-semibold">{f.label}</div>{f.hint && <div className="help">{f.hint}</div>}</div>
              </div>
            ) : (
              <Field label={f.label} hint={f.hint}>
                {f.type === "select" ? (
                  <select className="field" value={draft[f.key]} onChange={(e) => set(f.key, e.target.value)}>
                    {f.options.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
                  </select>
                ) : (
                  <input className="field" type={f.type === "number" ? "number" : "text"} min={f.type === "number" ? 0 : undefined} value={draft[f.key]} onChange={(e) => set(f.key, e.target.value)} placeholder={f.placeholder || ""} />
                )}
              </Field>
            )}
          </div>
        ))}
      </div>
      {children}
      <Busy type="submit" className="btn btn-primary btn-sm" busy={busy.save} disabled={!dirty.length}>{t("save")}</Busy>
    </form>
  );
}

// Write-only credentials: the backend only says whether each is set, where from and a masked value.
function SecretsForm({ fields, secrets, onSaved, extra }) {
  const { t, notify, confirm } = useApp();
  const [busy, run] = useBusy();
  const [draft, setDraft] = useState({});
  const dirty = fields.filter((f) => (draft[f.name] || "") !== "");
  const set = (name, value) => setDraft((d) => ({ ...d, [name]: value }));
  const save = (e) => {
    e.preventDefault();
    if (!dirty.length) return;
    run("save", async () => {
      for (const f of dirty) await api.call("secret_set", { name: f.name, value: draft[f.name].trim() });
      notify(t("secrets_saved"));
      setDraft({});
      await onSaved();
    });
  };
  const remove = async (f) => {
    const ok = await confirm({ title: t("remove_secret"), message: t("remove_secret_msg", { name: t(f.label) }), confirmLabel: t("remove") });
    if (!ok) return;
    run(`rm-${f.name}`, async () => {
      await api.call("secret_set", { name: f.name, value: "" });
      notify(t("secret_removed"));
      await onSaved();
    });
  };
  return (
    <form className="space-y-3" onSubmit={save} noValidate autoComplete="off">
      <div className="grid gap-3 sm:grid-cols-2">
        {fields.map((f) => {
          const s = secrets[f.name] || {};
          return (
            <div key={f.name} className="space-y-1">
              <Field label={t(f.label)}>
                <input className="field" type={f.secret ? "password" : "text"} autoComplete="new-password" value={draft[f.name] || ""} onChange={(e) => set(f.name, e.target.value)}
                  placeholder={s.configured ? `${t("configured")}${s.value ? ` ${s.value}` : ""}` : f.placeholder || ""} />
              </Field>
              <div className="flex flex-wrap items-center gap-1.5">
                <Chip className={s.configured ? "chip-ok" : "chip-amber"}>{s.configured ? t("configured") : t("not_configured")}</Chip>
                {s.configured && s.source && <Chip title={t("secret_source")}>{s.source}</Chip>}
                {s.configured && s.source === "settings" && <button type="button" className="btn-link text-[11.5px]" onClick={() => remove(f)}>{t("remove")}</button>}
              </div>
            </div>
          );
        })}
      </div>
      {extra}
      <Busy type="submit" className="btn btn-primary btn-sm" busy={busy.save} disabled={!dirty.length}>{t("save")}</Busy>
    </form>
  );
}

function ChannelCard({ channel, info, secrets, settings, onChanged }) {
  const { t, notify, toastError } = useApp();
  const [busy, run] = useBusy();
  const [result, setResult] = useState(null);
  const setSetting = (key, value) => run(key, async () => {
    await api.call("settings_set", { values: { [key]: value } });
    notify(t("saved"));
    await onChanged();
  });
  const enabled = settings[`notify.${channel}.enabled`] === "1";
  const severity = settings[`notify.${channel}.min_severity`] || "low";
  const test = () => run("test", async () => setResult(await api.call("notify_test", { channel })));
  const findChat = () => run("chat", async () => {
    const r = await api.call("telegram_find_chat_id");
    if (r.ok) { notify(t("chat_found", { id: r.chat_id })); await onChanged(); } else toastError(new Error(r.error || t("chat_not_found")));
  });
  const fields = CHANNEL_SECRETS[channel];
  return (
    <article className="panel space-y-3" aria-label={t(`ch_${channel}`)}>
      <div className="flex flex-wrap items-center gap-2">
        <h3 style={{ fontSize: 14 }}>{t(`ch_${channel}`)}</h3>
        <Chip className={info?.configured ? "chip-ok" : "chip-amber"}>{info?.configured ? t("configured") : t("not_configured")}</Chip>
        <label className="ml-auto inline-flex items-center gap-2">
          <span className="help">{enabled ? t("enabled") : t("disabled")}</span>
          <Switch checked={enabled} disabled={busy[`notify.${channel}.enabled`]} onChange={(v) => setSetting(`notify.${channel}.enabled`, v ? "1" : "0")} label={`${t("enabled")}: ${t(`ch_${channel}`)}`} />
        </label>
      </div>
      <p className="help">{t(`ch_${channel}_hint`)}</p>
      {info?.detail && !info.configured && <p className="help" style={{ overflowWrap: "anywhere" }}>{info.detail}</p>}
      <div className="flex flex-wrap items-end gap-3">
        <label className="block">
          <span className="label">{t("min_severity")}</span>
          <select className="field" style={{ width: "auto" }} value={severity} onChange={(e) => setSetting(`notify.${channel}.min_severity`, e.target.value)}>
            {SEVERITIES.map((s) => <option key={s} value={s}>{t(`sev_${s}`)}</option>)}
          </select>
        </label>
        <Busy className="btn btn-sm" busy={busy.test} onClick={test}>{t("test")}</Busy>
        {channel === "telegram" && <Busy className="btn btn-sm" busy={busy.chat} onClick={findChat}>{t("find_chat")}</Busy>}
        {result && <span className={`chip ${result.ok ? "chip-ok" : "chip-danger"}`} role="status" style={{ whiteSpace: "normal" }}>{result.ok ? t("test_ok") : `${t("test_failed")}: ${result.error || ""}`}</span>}
      </div>
      {channel === "telegram" && <p className="help">{t("telegram_help")}</p>}
      {channel === "ntfy" && (
        <SettingsForm fields={[{ key: "notify.ntfy.server", type: "text", label: t("ntfy_server"), placeholder: "https://ntfy.sh" }]} settings={settings} onSaved={onChanged} />
      )}
      {channel === "email" && (
        <Field label={t("email_backend")} hint={t("email_backend_hint")}>
          <select className="field" style={{ maxWidth: 320 }} value={settings["notify.email.backend"] || "auto"} onChange={(e) => setSetting("notify.email.backend", e.target.value)}>
            {["auto", "faustus", "smtp"].map((b) => <option key={b} value={b}>{t(`backend_${b}`)}</option>)}
          </select>
        </Field>
      )}
      {fields && (
        <details open={!info?.configured}>
          <summary className="font-semibold">{t("credentials")}</summary>
          <div className="mt-2"><SecretsForm fields={fields} secrets={secrets} onSaved={onChanged} /></div>
        </details>
      )}
    </article>
  );
}


function Folders({ status, onChanged }) {
  const { t, lang, notify, confirm, changed } = useApp();
  const [busy, run] = useBusy();
  const [path, setPath] = useState("");
  const folders = status?.folders || [];
  const add = (e) => {
    e.preventDefault();
    if (!path.trim()) return;
    run("add", async () => { await api.call("folder_add", { path: path.trim() }); setPath(""); notify(t("folder_added")); await onChanged(); });
  };
  const remove = async (f) => {
    const ok = await confirm({ title: t("folder_remove"), message: t("folder_remove_msg", { path: f.path }), confirmLabel: t("remove") });
    if (!ok) return;
    run(`rm-${f.path}`, async () => { await api.call("folder_remove", { path: f.path }); notify(t("folder_removed")); await onChanged(); });
  };
  const scan = () => run("scan", async () => {
    const r = await api.call("folder_scan");
    notify(t("folder_scan_result", { n: r.new_documents ?? 0 }) + (r.errors?.length ? ` · ${r.errors.length} ${t("errors")}` : ""));
    changed();
    await onChanged();
  });
  return (
    <div className="panel space-y-3">
      <p className="help">{t("folders_help")}</p>
      <ul className="m-0 list-none space-y-1.5 p-0">
        {folders.map((f) => (
          <li key={f.path} className="flex flex-wrap items-center gap-2">
            <Icon d={ICONS.folder} size={15} />
            <span className="mono min-w-0 flex-1">{f.path}</span>
            {f.inbox && <Chip className="chip-accent">{t("inbox")}</Chip>}
            {!f.exists && <Chip className="chip-danger">{t("folder_missing")}</Chip>}
            {!f.inbox && <Busy className="btn btn-sm" busy={busy[`rm-${f.path}`]} onClick={() => remove(f)}>{t("remove")}</Busy>}
          </li>
        ))}
      </ul>
      <form className="flex flex-wrap items-end gap-2" onSubmit={add} noValidate>
        <Field label={t("folder_path")} className="min-w-[260px] flex-1"><input className="field" value={path} onChange={(e) => setPath(e.target.value)} placeholder={t("folder_ph")} /></Field>
        <Busy type="submit" className="btn btn-primary btn-sm" busy={busy.add} disabled={!path.trim()}>{t("folder_add")}</Busy>
        <Busy type="button" className="btn btn-sm" busy={busy.scan} onClick={scan}><Icon d={ICONS.refresh} size={13} />{t("scan_folders")}</Busy>
      </form>
      <p className="help">{t("last_scan")}: {status?.last_folder_scan_ts ? rel(status.last_folder_scan_ts, lang) : "—"}</p>
    </div>
  );
}

function MailBox({ status, settings, reload }) {
  const { t, lang } = useApp();
  const [days, setDays] = useState("");
  const [scanning, scan] = useMailScan(reload);
  const m = status?.mail || {};
  return (
    <div className="panel space-y-3">
      <p className="help">{t("set_mail_help")}</p>
      <div className="flex flex-wrap items-center gap-2">
        <Chip className="chip-accent">{t("mail_reading_from")}: {t(`mail_src_${m.source || "faustus"}`)}</Chip>
        {m.source !== "hub" && <Chip className={m.faustus_dir ? "chip-ok" : "chip-amber"}>{m.faustus_dir ? t("faustus_found") : t("faustus_not_found")}</Chip>}
        {m.last_scan_ts ? <span className="help">{t("last_scan")}: {shortClock(m.last_scan_ts, lang)}</span> : <span className="help">{t("never_scanned")}</span>}
        {m.last_error && <Chip className="chip-danger" title={m.last_error}>{m.last_error.slice(0, 80)}</Chip>}
      </div>
      <SettingsForm settings={settings} onSaved={reload} fields={[
        { key: "mail.enabled", type: "switch", label: t("mail_enabled"), hint: t("mail_enabled_hint"), wide: true },
        { key: "mail.source", type: "select", label: t("mail_source"), hint: t("mail_source_hint"), options: ["auto", "hub", "faustus"].map((v) => [v, t(`mail_src_${v}`)]), wide: true },
        { key: "mail.faustus_dir", type: "text", label: t("faustus_folder"), hint: t("faustus_folder_hint"), wide: true },
        { key: "mail.faustus_owner", type: "text", label: t("faustus_owner"), hint: t("faustus_owner_hint") },
        { key: "mail.interval_min", type: "number", label: t("mail_interval"), hint: t("minutes") },
        { key: "mail.window_days", type: "number", label: t("mail_window"), hint: t("mail_window_hint") },
        { key: "mail.first_days", type: "number", label: t("mail_first"), hint: t("mail_first_hint") },
        { key: "folders.interval_min", type: "number", label: t("folders_interval"), hint: t("minutes") },
      ]} />
      <div className="flex flex-wrap items-end gap-2">
        <Busy className="btn btn-sm" busy={scanning} onClick={() => scan({})}><Icon d={ICONS.mail} size={13} />{t("read_mail_now")}</Busy>
        <Field label={t("search_back")} className="w-36"><input className="field" type="number" min="1" max="730" value={days} onChange={(e) => setDays(e.target.value)} placeholder="90" /></Field>
        <Busy className="btn btn-sm" busy={scanning} disabled={!days} onClick={() => scan({ since_days: Number(days) })}>{t("search_back_go")}</Busy>
      </div>
    </div>
  );
}

function Reminders({ settings, reload }) {
  const { t } = useApp();
  const kindFields = DEADLINE_KINDS.map((k) => ({ key: `remind.${k}`, type: "text", label: t(`dk_${k}`), placeholder: "30, 7, 0" }));
  return (
    <div className="panel space-y-3">
      <p className="help">{t("reminders_help")}</p>
      <SettingsForm settings={settings} onSaved={reload} fields={kindFields} />
      <hr style={{ borderColor: "var(--line)" }} />
      <SettingsForm settings={settings} onSaved={reload} fields={[
        { key: "notify.night_from", type: "number", label: t("night_from"), hint: t("night_hint") },
        { key: "notify.night_to", type: "number", label: t("night_to"), hint: t("night_hint") },
        { key: "notify.night_high", type: "switch", label: t("night_high"), hint: t("night_high_hint"), wide: true },
      ]} />
    </div>
  );
}

function Reading({ status, settings, reload }) {
  const { t, notify } = useApp();
  const [busy, run] = useBusy();
  const ocr = status?.ocr || {};
  const llm = status?.llm || {};
  const sync = () => run("sync", async () => {
    const r = await api.call("phileas_sync");
    if (r.ok) notify(t("phileas_synced", { n: r.created ?? 0, l: r.linked ?? 0 })); else notify(r.error || r.reason || t("phileas_failed"), "error");
    await reload();
  });
  return (
    <div className="panel space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-semibold">OCR</span>
        <Chip className={ocr.available ? "chip-ok" : "chip-amber"}>{ocr.available ? t("available") : t("unavailable")}</Chip>
        <span className="help" style={{ overflowWrap: "anywhere" }}>{ocr.detail}</span>
      </div>
      <p className="help">{t("ocr_help")}</p>
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-semibold">{t("model_pass")}</span>
        <Chip className={llm.available?.ok ? "chip-ok" : "chip"}>{llm.mode === "off" ? t("off") : llm.available?.ok ? t("available") : t("unavailable")}</Chip>
        {llm.available?.detail && <span className="help" style={{ overflowWrap: "anywhere" }}>{llm.available.detail}</span>}
      </div>
      <SettingsForm settings={settings} onSaved={reload} fields={[
        { key: "extract.llm", type: "select", label: t("model_mode"), hint: t("model_mode_hint"), options: [["auto", t("model_auto")], ["off", t("off")]] },
        { key: "warranty.years", type: "number", label: t("warranty_default"), hint: t("warranty_default_hint") },
        { key: "calendar.region", type: "select", label: t("region"), hint: t("region_hint"), options: REGIONS.map((r) => [r, t(`region_${r || "none"}`)]) },
        { key: "calendar.extra_holidays", type: "text", label: t("extra_holidays"), hint: t("extra_holidays_hint"), placeholder: "2026-12-24, 2026-12-31" },
        { key: "prices.alert_pct", type: "number", label: t("price_alert"), hint: t("price_alert_hint") },
        { key: "prices.bills", type: "switch", label: t("price_bills"), hint: t("price_bills_hint"), wide: true },
      ]} />
      <hr style={{ borderColor: "var(--line)" }} />
      <div className="space-y-2">
        <div className="flex flex-wrap items-center gap-2">
          <span className="font-semibold">{t("phileas_link")}</span>
          <Chip className={status?.phileas?.last_error ? "chip-danger" : status?.phileas?.last_sync_ts ? "chip-ok" : ""}>{status?.phileas?.last_error ? t("phileas_failed") : status?.phileas?.last_sync_ts ? t("phileas_ok") : t("never_scanned")}</Chip>
          {status?.phileas?.last_error && <span className="help">{status.phileas.last_error}</span>}
        </div>
        <p className="help">{t("phileas_help")}</p>
        <SettingsForm settings={settings} onSaved={reload} fields={[{ key: "links.phileas", type: "switch", label: t("phileas_enabled"), wide: true }]} />
        <Busy className="btn btn-sm" busy={busy.sync} onClick={sync}>{t("phileas_sync")}</Busy>
      </div>
      <hr style={{ borderColor: "var(--line)" }} />
      <div className="space-y-2">
        <span className="font-semibold">{t("ledger_tx")}</span>
        <p className="help">{t("ledger_help")}</p>
        <SettingsForm settings={settings} onSaved={reload} fields={[
          { key: "links.ledger", type: "switch", label: t("ledger_enabled"), wide: true },
          { key: "minutes.me", type: "text", label: t("minutes_me"), hint: t("minutes_me_hint"), wide: true },
        ]} />
      </div>
    </div>
  );
}

function Runs() {
  const { t, notify } = useApp();
  const [busy, run] = useBusy();
  const { data, error, reload } = useLoad(() => api.call("runs_list", { limit: 30 }), []);
  const runs = data?.runs || [];
  const housekeeping = () => run("hk", async () => { await api.call("housekeeping_run"); notify(t("housekeeping_done")); await reload(); });
  return (
    <Section id="sec-runs" title={t("runs_title")} actions={<><button type="button" className="btn btn-sm" onClick={reload}>{t("reload")}</button><Busy className="btn btn-sm" busy={busy.hk} onClick={housekeeping} title={t("housekeeping_hint")}>{t("housekeeping")}</Busy></>}>
      <ErrorBox error={error} />
      {runs.length === 0 ? <div className="panel help text-center">{t("runs_empty")}</div> : (
        <div className="panel overflow-x-auto p-0">
          <table className="grid">
            <thead><tr><th>{t("time")}</th><th>{t("kind")}</th><th>{t("result")}</th><th className="r">{t("duration")}</th><th>{t("detail")}</th></tr></thead>
            <tbody>
              {runs.map((r) => (
                <tr key={r.id}>
                  <td className="whitespace-nowrap"><Rel ts={r.ts} /></td>
                  <td>{t(`run_${r.kind}`) === `run_${r.kind}` ? r.kind : t(`run_${r.kind}`)}{r.ref ? <span className="help mono"> {r.ref}</span> : null}</td>
                  <td><Chip className={r.ok ? "chip-ok" : "chip-danger"}>{r.ok ? t("ok") : t("failed")}</Chip></td>
                  <td className="r num">{duration(r.duration_ms)}</td>
                  <td style={{ overflowWrap: "anywhere", minWidth: 180 }}>{r.detail}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Section>
  );
}

export default function Ajustes() {
  const { t, lang, setLang, refreshDash } = useApp();
  const status = useLoad(() => api.call("kafka_status"), []);
  const notifyStatus = useLoad(() => api.call("notify_status"), []);
  const data = status.data;
  const reload = async () => { await Promise.all([status.reload(), notifyStatus.reload()]); refreshDash(); };
  if (!data) return <div className="space-y-3"><h1>{t("nav_settings")}</h1>{status.error ? <ErrorBox error={status.error} /> : <p className="help">…</p>}</div>;
  const settings = data.settings || {};
  const secrets = data.secrets || {};
  const channels = notifyStatus.data?.channels || data.channels || {};

  return (
    <div className="space-y-6">
      <header>
        <h1>{t("nav_settings")}</h1>
        <p className="help">{t("settings_intro")}</p>
      </header>

      <Section id="sec-folders" title={t("set_folders")}><Folders status={data} onChanged={reload} /></Section>
      <Section id="sec-mail" title={t("set_mail")}><MailBox status={data} settings={settings} reload={reload} /></Section>
      <Section id="sec-reading" title={t("set_reading")}><Reading status={data} settings={settings} reload={reload} /></Section>
      <Section id="sec-reminders" title={t("set_reminders")}><Reminders settings={settings} reload={reload} /></Section>

      <Section id="sec-workshop" title={t("set_workshop")}>
        <div className="panel">
          <SettingsForm settings={settings} onSaved={reload} fields={[
            { key: "workshop.dir", type: "text", label: t("tl_workshop_dir"), hint: t("tl_workshop_dir_hint"), wide: true },
          ]} />
        </div>
      </Section>

      <Section id="sec-notify" title={t("set_notify")}>
        <p className="help">{t("set_notify_help")}</p>
        <div className="panel space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <Chip className={data.notify?.effective === "hub" ? "chip-ok" : ""}>{data.notify?.effective === "hub" ? t("via_now_hub") : t("via_now_own")}</Chip>
          </div>
          <SettingsForm settings={settings} onSaved={reload} fields={[
            { key: "notify.via", type: "select", label: t("notify_via"), hint: t("notify_via_hint"), options: ["auto", "hub", "own"].map((v) => [v, t(`via_${v}`)]), wide: true },
          ]} />
        </div>
        <ErrorBox error={notifyStatus.error} />
        <div className="grid gap-3 xl:grid-cols-2">
          {CHANNELS.map((c) => <ChannelCard key={c} channel={c} info={channels[c]} secrets={secrets} settings={settings} onChanged={reload} />)}
        </div>
      </Section>

      <Section id="sec-general" title={t("set_general")}>
        <div className="panel space-y-4">
          <div className="space-y-1">
            <span className="label">{t("language_label")}</span>
            <div className="flex flex-wrap gap-2" role="group" aria-label={t("language_label")}>
              <button type="button" className={`btn btn-sm ${lang === "es" ? "btn-primary" : ""}`} aria-pressed={lang === "es"} onClick={() => setLang("es")}>Español</button>
              <button type="button" className={`btn btn-sm ${lang === "en" ? "btn-primary" : ""}`} aria-pressed={lang === "en"} onClick={() => setLang("en")}>English</button>
            </div>
          </div>
          <SettingsForm settings={settings} onSaved={reload} fields={[
            { key: "scheduler.paused", type: "switch", label: t("scheduler_paused"), hint: t("scheduler_paused_hint"), wide: true },
          ]} />
        </div>
      </Section>

      <Runs />
    </div>
  );
}
