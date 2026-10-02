import React, { useEffect, useRef, useState } from "react";
import { api } from "../api.js";
import { useApp } from "../context.js";
import { Busy, Chip, ErrorBox, Field, Icon, ICONS, Section, Switch, useBusy, useLoad } from "../components/ui.jsx";

// What each operation takes: `accepts` is the kind of input file, `multi` lets the list hold several, `order` lets the user reorder it.
const OPS = [
  { id: "merge", accepts: "pdf", multi: true, order: true, icon: "M5 4h6v6H5zM13 14h6v6h-6zM8 10v4h5" },
  { id: "split", accepts: "pdf", icon: "M12 3v18M4 7h5v10H4zM15 7h5v10h-5z" },
  { id: "pages", accepts: "pdf", icon: "M7 3h8l4 4v14H7zM15 3v4h4M10 13h6M10 17h6" },
  { id: "compress", accepts: "pdf", icon: "M4 4l6 6M10 5v5H5M20 4l-6 6M14 5v5h5M4 20l6-6M10 19v-5H5M20 20l-6-6M14 19v-5h5" },
  { id: "password", accepts: "pdf", icon: "M6 11h12v9H6zM8 11V8a4 4 0 018 0v3" },
  { id: "watermark", accepts: "pdf", icon: "M12 3c3 4 6 7 6 11a6 6 0 01-12 0c0-4 3-7 6-11z" },
  { id: "images_pdf", accepts: "image", multi: true, order: true, folders: true, icon: "M4 5h16v14H4zM4 16l5-5 4 4 3-3 4 4M9 9h.01" },
  { id: "office_pdf", accepts: "office", icon: "M7 3h8l4 4v14H7zM15 3v4h4M9 12l1.5 6 1.5-4 1.5 4 1.5-6" },
  { id: "pdf_images", accepts: "pdf", icon: "M4 5h16v14H4zM8 15l3-3 3 3 2-2 3 3" },
  { id: "images_compress", accepts: "image", multi: true, folders: true, icon: "M4 4h16v16H4zM9 9l-3 3M15 15l3-3M9 15l-3-3M15 9l3 3" },
  { id: "info", accepts: "pdf", icon: "M12 21a9 9 0 100-18 9 9 0 000 18zM12 11v6M12 7.5v.01" },
];

const IMAGE_EXT = ["png", "jpg", "jpeg", "webp", "bmp", "gif", "tif", "tiff", "heic", "heif"];
const OFFICE_EXT = ["docx", "doc", "odt", "rtf"];
const COLORS = ["gris", "rojo", "azul", "negro", "verde", "naranja"];

const DEFAULTS = {
  split_mode: "pages", split_ranges: "", split_every: 2,
  pg_action: "extract", pg_pages: "", pg_degrees: 90, pg_order: "",
  preset: "ebook", target: "",
  pw_mode: "protect", pw_new: "", pw_owner: "", allow_print: true, allow_copy: true, allow_modify: true,
  wm_text: "", wm_opacity: 30, wm_angle: 45, wm_size: 60, wm_color: "gris", wm_pages: "",
  page_size: "A4", margin: 10, orientation: "auto",
  engine: "auto",
  fmt: "png", dpi: 150, quality: 90, pi_pages: "",
  limit: "5", unit: "MB", recursive: true, lossless: false, skip_small: false,
};

const extOf = (name) => { const m = /\.([A-Za-z0-9]+)$/.exec(name || ""); return m ? m[1].toLowerCase() : ""; };
const dirOf = (path) => (path || "").replace(/[\\/][^\\/]*$/, "");
const isDocId = (ref) => /^d_[A-Za-z0-9]+$/.test(ref);

function acceptedExt(op, status) {
  if (op.accepts === "pdf") return ["pdf"];
  if (op.accepts === "image") return status?.image_extensions?.length ? status.image_extensions : IMAGE_EXT;
  return status?.office_extensions?.length ? status.office_extensions : OFFICE_EXT;
}

// ------------------------------------------------------------------ tax return folder
function TaxPack() {
  const { t, notify } = useApp();
  const [busy, run] = useBusy();
  const [year, setYear] = useState(String(new Date().getFullYear() - 1));
  const [zip, setZip] = useState(false);
  const [out, setOut] = useState(null);
  const [error, setError] = useState(null);
  const go = (e) => {
    e.preventDefault();
    setError(null);
    run("pack", async () => {
      try {
        const r = await api.call("tax_pack", { year: Number(year), zip });
        setOut(r);
        notify(t("tx_pack_done", { n: (r.files || []).length }));
      } catch (err) { setError(err); }
    });
  };
  return (
    <form className="panel space-y-3" onSubmit={go} noValidate aria-label={t("tx_pack_title")}>
      <p className="help">{t("tx_pack_help")}</p>
      <div className="flex flex-wrap items-end gap-3">
        <Field label={t("tx_pack_year")} className="w-32"><input className="field num" type="number" min="2000" max="2100" value={year} onChange={(e) => setYear(e.target.value)} /></Field>
        <label className="inline-flex items-center gap-2 help"><input type="checkbox" checked={zip} onChange={(e) => setZip(e.target.checked)} />{t("tx_pack_zip")}</label>
        <Busy type="submit" className="btn btn-primary btn-sm" busy={busy.pack} disabled={!year}>{t("tx_pack_go")}</Busy>
      </div>
      <ErrorBox error={error} />
      {out && (
        <div className="space-y-1">
          <div className="mono" style={{ overflowWrap: "anywhere" }}>{out.path}</div>
          <div className="help">{t("tx_pack_docs", { n: out.documents ?? 0 })}{out.zip ? ` · ${out.zip}` : ""}</div>
          {out.missing?.length > 0 && <div className="help">{t("tx_pack_missing")}: {out.missing.map((m) => m.label).join(", ")}</div>}
        </div>
      )}
    </form>
  );
}

// ------------------------------------------------------------------ the file list
function FilePicker({ op, files, setFiles, job, setJob, status, doc }) {
  const { t, notify, toastError } = useApp();
  const [over, setOver] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [path, setPath] = useState("");
  const input = useRef(null);
  const accepted = acceptedExt(op, status);
  const accept = accepted.map((e) => `.${e}`).join(",");

  const put = (added) => setFiles((current) => (op.multi ? [...current, ...added.filter((a) => !current.some((c) => c.ref === a.ref))] : added.slice(0, 1)));

  const addFiles = async (list) => {
    const chosen = Array.from(list || []);
    if (!chosen.length) return;
    const good = [];
    for (const file of chosen) {
      if (accepted.includes(extOf(file.name))) good.push(file);
      else notify(t("tl_wrong_type", { name: file.name, ext: extOf(file.name) ? `.${extOf(file.name)}` : "?" }), "error");
    }
    if (!good.length) return;
    setUploading(true);
    try {
      const r = await api.workshopUpload(op.multi ? good : good.slice(0, 1), job);
      setJob(r.job);
      for (const x of (r.results || []).filter((y) => !y.ok)) notify(t("tl_upload_failed", { name: x.name, why: x.error || "" }), "error");
      put((r.files || []).map((f) => ({ ref: f.path, name: f.name, kind: "upload", size_text: f.size_text, pages: f.pages, encrypted: f.encrypted, damaged: f.damaged, ranges: "" })));
    } catch (e) {
      toastError(e);
    } finally {
      setUploading(false);
    }
  };

  const addPath = () => {
    const ref = path.trim().replace(/^"|"$/g, "");
    if (!ref) return;
    put([{ ref, name: ref, kind: isDocId(ref) ? "doc" : "path", ranges: "" }]);
    setPath("");
  };

  const move = (i, d) => setFiles((list) => {
    const next = list.slice();
    const j = i + d;
    if (j < 0 || j >= next.length) return list;
    [next[i], next[j]] = [next[j], next[i]];
    return next;
  });
  const remove = (i) => setFiles((list) => list.filter((_, k) => k !== i));
  const patch = (i, values) => setFiles((list) => list.map((f, k) => (k === i ? { ...f, ...values } : f)));

  return (
    <div className="space-y-3">
      <div
        className={`tl-drop${over ? " tl-drop-over" : ""}`}
        data-own-drop="1"
        onDragEnter={(e) => { e.preventDefault(); setOver(true); }}
        onDragOver={(e) => { e.preventDefault(); setOver(true); }}
        onDragLeave={() => setOver(false)}
        onDrop={(e) => { e.preventDefault(); setOver(false); addFiles(e.dataTransfer.files); }}
      >
        <Icon d={ICONS.upload} size={22} />
        <p className="help" style={{ margin: 0, maxWidth: 480 }}>{t("tl_drop_hint")}</p>
        <input ref={input} type="file" hidden multiple={!!op.multi} accept={accept} onChange={(e) => { addFiles(e.target.files); e.target.value = ""; }} />
        <Busy className="btn btn-sm btn-primary" busy={uploading} onClick={() => input.current?.click()}>{uploading ? t("tl_uploading") : t("tl_choose")}</Busy>
      </div>

      <div className="flex flex-wrap items-end gap-2">
        <Field label={t("tl_path")} className="min-w-0 flex-1" hint={t("tl_path_hint")}>
          <input className="field mono" value={path} placeholder={op.accepts === "image" ? t("tl_path_ph_images") : t("tl_path_ph")}
            onChange={(e) => setPath(e.target.value)} onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); addPath(); } }} />
        </Field>
        <button type="button" className="btn btn-sm" onClick={addPath} disabled={!path.trim()}><Icon d={ICONS.plus} size={13} />{t("tl_add")}</button>
      </div>

      {doc && !files.some((f) => f.ref === doc.id) && op.accepts === "pdf" && (
        <button type="button" className="btn btn-sm" onClick={() => put([{ ref: doc.id, name: doc.title || doc.id, kind: "doc", ranges: "" }])}><Icon d={ICONS.doc} size={13} />{t("tl_pre_doc", { name: doc.title || doc.id })}</button>
      )}

      {files.length === 0 ? <p className="help">{t("tl_no_files")}</p> : (
        <ul className="space-y-1.5" aria-label={t("tl_files")}>
          {files.map((f, i) => (
            <li key={f.ref} className="panel panel-tight tl-file">
              <div className="flex flex-wrap items-center gap-2">
                <Icon d={f.kind === "doc" ? ICONS.doc : ICONS.folder} size={14} />
                <span className="trunc font-semibold" style={{ flex: "1 1 160px" }} title={f.ref}>{f.name}</span>
                {f.kind === "doc" && <Chip>{t("tl_doc")}</Chip>}
                {f.size_text && <span className="help num">{f.size_text}</span>}
                {f.pages ? <span className="help num">{t("tl_pages_n", { n: f.pages })}</span> : null}
                {f.encrypted && <Chip className="chip-amber">{t("tl_encrypted")}</Chip>}
                {f.damaged && <Chip className="chip-danger">{t("tl_damaged")}</Chip>}
                {op.order && files.length > 1 && (
                  <>
                    <button type="button" className="btn btn-sm" disabled={i === 0} onClick={() => move(i, -1)} aria-label={t("tl_move_up")} title={t("tl_move_up")}><Icon d={ICONS.up} size={13} /></button>
                    <button type="button" className="btn btn-sm" disabled={i === files.length - 1} onClick={() => move(i, 1)} aria-label={t("tl_move_down")} title={t("tl_move_down")}><Icon d={ICONS.down} size={13} /></button>
                  </>
                )}
                <button type="button" className="btn btn-sm" onClick={() => remove(i)} aria-label={t("delete")} title={t("delete")}><Icon d={ICONS.x} size={13} /></button>
              </div>
              {op.id === "merge" && (
                <input className="field mono mt-1.5" value={f.ranges} placeholder={t("tl_file_pages")} aria-label={`${t("tl_file_pages")}: ${f.name}`} onChange={(e) => patch(i, { ranges: e.target.value })} />
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ------------------------------------------------------------------ options of each operation
function Select({ value, onChange, options, label, hint }) {
  return (
    <Field label={label} hint={hint}>
      <select className="field" value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
      </select>
    </Field>
  );
}

function Text({ value, onChange, label, hint, type = "text", ...rest }) {
  return (
    <Field label={label} hint={hint}>
      <input className="field" type={type} value={value} onChange={(e) => onChange(e.target.value)} {...rest} />
    </Field>
  );
}

function Check({ checked, onChange, label, hint }) {
  return (
    <div className="flex items-start gap-3">
      <Switch checked={checked} onChange={onChange} label={label} />
      <div><div className="font-semibold">{label}</div>{hint && <div className="help">{hint}</div>}</div>
    </div>
  );
}

function Options({ op, o, set, status }) {
  const { t } = useApp();
  switch (op.id) {
    case "split":
      return (
        <div className="grid gap-3 sm:grid-cols-2">
          <Select label={t("tl_split_mode")} value={o.split_mode} onChange={(v) => set("split_mode", v)}
            options={[["pages", t("tl_split_pages")], ["ranges", t("tl_split_ranges")], ["every", t("tl_split_every")]]} />
          {o.split_mode === "ranges" && <Text label={t("tl_ranges")} hint={t("tl_ranges_hint")} className="mono" value={o.split_ranges} placeholder="1-3,4-6,7-" onChange={(v) => set("split_ranges", v)} />}
          {o.split_mode === "every" && <Text label={t("tl_every_n")} type="number" min="1" value={o.split_every} onChange={(v) => set("split_every", v)} />}
        </div>
      );
    case "pages":
      return (
        <div className="grid gap-3 sm:grid-cols-2">
          <Select label={t("tl_action")} value={o.pg_action} onChange={(v) => set("pg_action", v)}
            options={[["extract", t("tl_act_extract")], ["delete", t("tl_act_delete")], ["rotate", t("tl_act_rotate")], ["reorder", t("tl_act_reorder")]]} />
          {o.pg_action === "rotate" && (
            <Select label={t("tl_degrees")} value={String(o.pg_degrees)} onChange={(v) => set("pg_degrees", Number(v))}
              options={[["90", t("tl_rot_90")], ["180", t("tl_rot_180")], ["270", t("tl_rot_270")]]} />
          )}
          {o.pg_action !== "reorder" && (
            <Text label={t("tl_pages")} hint={o.pg_action === "rotate" ? `${t("tl_rotate_hint")} ${t("tl_pages_hint")}` : t("tl_pages_hint")} value={o.pg_pages} placeholder="1-3,5,8-" onChange={(v) => set("pg_pages", v)} />
          )}
          {o.pg_action === "reorder" && <Text label={t("tl_order")} hint={t("tl_order_hint")} value={o.pg_order} placeholder="3,1,2,4-6" onChange={(v) => set("pg_order", v)} />}
        </div>
      );
    case "compress":
      return (
        <div className="space-y-2">
          <div className="grid gap-3 sm:grid-cols-2">
            <Select label={t("tl_preset")} value={o.preset} onChange={(v) => set("preset", v)}
              options={[["screen", t("tl_preset_screen")], ["ebook", t("tl_preset_ebook")], ["printer", t("tl_preset_printer")], ["prepress", t("tl_preset_prepress")]]} />
            <Text label={t("tl_target")} hint={t("tl_target_hint")} type="number" min="0" step="0.1" value={o.target} onChange={(v) => set("target", v)} />
          </div>
          {status && <p className="help">{status.ghostscript ? t("tl_engine_gs") : t("tl_engine_builtin")}</p>}
        </div>
      );
    case "password":
      return (
        <div className="space-y-3">
          <Select label={t("tl_action")} value={o.pw_mode} onChange={(v) => set("pw_mode", v)} options={[["protect", t("tl_mode_protect")], ["unprotect", t("tl_mode_unprotect")]]} />
          {o.pw_mode === "protect" ? (
            <>
              <div className="grid gap-3 sm:grid-cols-2">
                <Text label={t("tl_password")} hint={t("tl_password_set_hint")} type="password" autoComplete="new-password" value={o.pw_new} onChange={(v) => set("pw_new", v)} />
                <Text label={t("tl_owner_password")} type="password" autoComplete="new-password" value={o.pw_owner} onChange={(v) => set("pw_owner", v)} />
              </div>
              <div className="space-y-1.5">
                <span className="label">{t("tl_allow")}</span>
                <div className="flex flex-wrap gap-x-6 gap-y-2">
                  <Check checked={o.allow_print} onChange={(v) => set("allow_print", v)} label={t("tl_allow_print")} />
                  <Check checked={o.allow_copy} onChange={(v) => set("allow_copy", v)} label={t("tl_allow_copy")} />
                  <Check checked={o.allow_modify} onChange={(v) => set("allow_modify", v)} label={t("tl_allow_modify")} />
                </div>
              </div>
            </>
          ) : (
            <Text label={t("tl_password_current")} type="password" autoComplete="off" value={o.pw_new} onChange={(v) => set("pw_new", v)} />
          )}
        </div>
      );
    case "watermark":
      return (
        <div className="grid gap-3 sm:grid-cols-2">
          <Text label={t("tl_wm_text")} className="sm:col-span-2" value={o.wm_text} placeholder="CONFIDENCIAL" maxLength={200} onChange={(v) => set("wm_text", v)} />
          <Field label={t("tl_wm_opacity", { n: o.wm_opacity })}>
            <input className="field" type="range" min="2" max="100" value={o.wm_opacity} onChange={(e) => set("wm_opacity", Number(e.target.value))} />
          </Field>
          <Text label={t("tl_wm_angle")} type="number" min="-360" max="360" value={o.wm_angle} onChange={(v) => set("wm_angle", v)} />
          <Text label={t("tl_wm_size")} type="number" min="6" max="400" value={o.wm_size} onChange={(v) => set("wm_size", v)} />
          <Select label={t("tl_wm_color")} value={o.wm_color} onChange={(v) => set("wm_color", v)} options={COLORS.map((c) => [c, t(`tl_c_${c}`)])} />
          <Text label={t("tl_file_pages")} hint={t("tl_pages_hint")} value={o.wm_pages} onChange={(v) => set("wm_pages", v)} />
        </div>
      );
    case "images_pdf":
      return (
        <div className="grid gap-3 sm:grid-cols-3">
          <Select label={t("tl_page_size")} value={o.page_size} onChange={(v) => set("page_size", v)} options={[["A4", t("tl_size_a4")], ["Letter", t("tl_size_letter")], ["fit", t("tl_size_fit")]]} />
          <Text label={t("tl_margin")} type="number" min="0" max="100" value={o.margin} onChange={(v) => set("margin", v)} />
          <Select label={t("tl_orientation")} value={o.orientation} onChange={(v) => set("orientation", v)} options={[["auto", t("tl_or_auto")], ["portrait", t("tl_or_portrait")], ["landscape", t("tl_or_landscape")]]} />
        </div>
      );
    case "office_pdf":
      return (
        <div className="space-y-2">
          <div className="grid gap-3 sm:grid-cols-2">
            <Select label={t("tl_engine")} value={o.engine} onChange={(v) => set("engine", v)} options={[["auto", t("tl_eng_auto")], ["word", t("tl_eng_word")], ["libreoffice", t("tl_eng_lo")]]} />
          </div>
          {status && (status.word || status.libreoffice
            ? <p className="help">{t("tl_office_found", { name: status.word ? t("tl_eng_word") : t("tl_eng_lo") })}</p>
            : <div className="banner banner-warn">{t("tl_no_office")}</div>)}
        </div>
      );
    case "pdf_images":
      return (
        <div className="grid gap-3 sm:grid-cols-2">
          <Select label={t("tl_format")} value={o.fmt} onChange={(v) => set("fmt", v)} options={[["png", "PNG"], ["jpg", "JPG"]]} />
          <Text label={t("tl_dpi")} type="number" min="36" max="600" value={o.dpi} onChange={(v) => set("dpi", v)} />
          {o.fmt === "jpg" && <Text label="JPEG %" type="number" min="10" max="100" value={o.quality} onChange={(v) => set("quality", v)} />}
          <Text label={t("tl_file_pages")} hint={t("tl_pages_hint")} value={o.pi_pages} onChange={(v) => set("pi_pages", v)} />
        </div>
      );
    case "images_compress":
      return (
        <div className="space-y-3">
          <div className="grid gap-3 sm:grid-cols-2">
            <Text label={t("tl_limit")} type="number" min="0" step="any" value={o.limit} onChange={(v) => set("limit", v)} />
            <Select label={t("tl_unit")} value={o.unit} onChange={(v) => set("unit", v)} options={[["MB", "MB"], ["KB", "KB"]]} />
          </div>
          <div className="space-y-2">
            <Check checked={o.recursive} onChange={(v) => set("recursive", v)} label={t("tl_recursive")} />
            <Check checked={o.lossless} onChange={(v) => set("lossless", v)} label={t("tl_lossless")} />
            <Check checked={o.skip_small} onChange={(v) => set("skip_small", v)} label={t("tl_skip_small")} />
          </div>
        </div>
      );
    default:
      return null;
  }
}

// ------------------------------------------------------------------ building the tool call from the form
function build(op, files, o, password, t) {
  const need = (what) => ({ error: t("tl_need_value", { what }) });
  const refs = files.map((f) => f.ref);
  if (!files.length) return { error: op.id === "merge" ? t("tl_need_two") : t("tl_need_file") };
  if (op.id === "merge" && files.length < 2) return { error: t("tl_need_two") };
  const file = refs[0];
  const base = password ? { password } : {};
  switch (op.id) {
    case "merge": {
      const ranges = files.map((f) => f.ranges.trim());
      return { tool: "pdf_merge", args: { files: refs, ...(ranges.some(Boolean) ? { ranges } : {}), ...base } };
    }
    case "split": {
      if (o.split_mode === "ranges" && !o.split_ranges.trim()) return need(t("tl_ranges"));
      return { tool: "pdf_split", args: { file, mode: o.split_mode, ...(o.split_mode === "ranges" ? { ranges: o.split_ranges.trim() } : {}), ...(o.split_mode === "every" ? { every: Math.max(1, Number(o.split_every) || 1) } : {}), ...base } };
    }
    case "pages": {
      if ((o.pg_action === "extract" || o.pg_action === "delete") && !o.pg_pages.trim()) return need(t("tl_pages"));
      if (o.pg_action === "reorder" && !o.pg_order.trim()) return need(t("tl_order"));
      const extra = o.pg_action === "reorder" ? { order: o.pg_order.trim() } : o.pg_action === "rotate" ? { degrees: o.pg_degrees, pages: o.pg_pages.trim() } : { pages: o.pg_pages.trim() };
      return { tool: "pdf_pages", args: { action: o.pg_action, file, ...extra, ...base } };
    }
    case "compress": {
      const target = Number(String(o.target).replace(",", "."));
      return { tool: "pdf_compress", args: { file, preset: o.preset, ...(target > 0 ? { target_mb: target } : {}), ...base } };
    }
    case "password": {
      if (!o.pw_new) return need(o.pw_mode === "protect" ? t("tl_password") : t("tl_password_current"));
      if (o.pw_mode === "unprotect") return { tool: "pdf_protect", args: { action: "unprotect", file, password: o.pw_new } };
      return { tool: "pdf_protect", args: { action: "protect", file, password: o.pw_new, ...(o.pw_owner ? { owner_password: o.pw_owner } : {}), ...(password ? { current_password: password } : {}),
        allow_print: o.allow_print, allow_copy: o.allow_copy, allow_modify: o.allow_modify } };
    }
    case "watermark": {
      if (!o.wm_text.trim()) return need(t("tl_wm_text"));
      return { tool: "pdf_watermark", args: { file, text: o.wm_text.trim(), opacity: Number(o.wm_opacity) / 100, angle: Number(o.wm_angle) || 0, font_size: Number(o.wm_size) || 60, color: o.wm_color,
        ...(o.wm_pages.trim() ? { pages: o.wm_pages.trim() } : {}), ...base } };
    }
    case "images_pdf":
      return { tool: "pdf_from_images", args: { images: refs, page_size: o.page_size, margin_mm: Number(o.margin) || 0, orientation: o.orientation } };
    case "office_pdf":
      return { tool: "pdf_from_office", args: { file, engine: o.engine } };
    case "pdf_images":
      return { tool: "pdf_to_images", args: { file, format: o.fmt, dpi: Number(o.dpi) || 150, quality: Number(o.quality) || 90, ...(o.pi_pages.trim() ? { pages: o.pi_pages.trim() } : {}), ...base } };
    case "images_compress": {
      const limit = Number(String(o.limit).replace(",", "."));
      if (!(limit > 0)) return need(t("tl_limit"));
      return { tool: "images_compress", args: { sources: refs, ...(o.unit === "KB" ? { limit_kb: limit } : { limit_mb: limit }), recursive: o.recursive, lossless_only: o.lossless, skip_small: o.skip_small, time_limit_s: 0 } };
    }
    default:
      return { tool: "pdf_info", args: { file, ...base } };
  }
}

// ------------------------------------------------------------------ result card
function Output({ out }) {
  const { t, changed, toastError } = useApp();
  const [busy, run] = useBusy();
  const [filed, setFiled] = useState(null);
  const isPdf = /\.pdf$/i.test(out.name || "");
  const file = () => run("file", async () => {
    const r = await api.call("doc_add_file", { path: out.path });
    const id = r?.documents?.[0]?.id || r?.duplicate_of;
    if (!id) throw new Error(r?.error || t("tl_upload_failed", { name: out.name, why: "" }));
    setFiled({ id, duplicate: !!r.duplicate_of });
    changed();
  });
  return (
    <li className="panel panel-tight">
      <div className="flex flex-wrap items-center gap-2">
        <Icon d={ICONS.doc} size={14} />
        <span className="trunc font-semibold" style={{ flex: "1 1 160px" }} title={out.path}>{out.name}</span>
        <span className="help num">{out.size_text}</span>
        {out.pages ? <span className="help num">{t("tl_pages_n", { n: out.pages })}</span> : null}
        <a className="btn btn-sm btn-primary" href={api.workshopFileUrl(out.path)} download={out.name}><Icon d={ICONS.down} size={13} />{t("tl_download")}</a>
        {isPdf && !filed && <Busy className="btn btn-sm" busy={busy.file} onClick={file}>{t("tl_file_it")}</Busy>}
        {filed && <a className="btn btn-sm" href={`#/documentos/${filed.id}`}><Chip className="chip-ok">{filed.duplicate ? t("tl_filed_dup") : t("tl_filed")}</Chip>{t("tl_open_doc")}</a>}
      </div>
    </li>
  );
}

function Result({ result }) {
  const { t } = useApp();
  const outs = result.outputs || [];
  const shown = outs.slice(0, 8);
  const folder = result.out_dir || (outs[0] ? dirOf(outs[0].path) : "");
  const before = result.size_before;
  const after = result.size_after;
  const pct = before > 0 && after >= 0 && after < before ? Math.round((1 - after / before) * 100) : null;
  const imgs = result.operation === "images_compress";
  const failedItems = (result.items || []).filter((i) => i.status === "failed");
  return (
    <section className="panel space-y-3" aria-label={t("tl_result")} aria-live="polite">
      <div className="flex flex-wrap items-center gap-2">
        <h2>{t("tl_result")}</h2>
        {result.pages ? <Chip>{t("tl_pages_result", { n: result.pages })}</Chip> : null}
        {before !== undefined && after !== undefined && (
          <span className="help num">{t("tl_before")}: {result.size_before_text} → {t("tl_after")}: {result.size_after_text}</span>
        )}
        {pct !== null && <Chip className="chip-ok">{t("tl_saved_pct", { pct })}</Chip>}
      </div>
      {imgs && <p>{t("tl_img_summary", { ok: result.compressed, skipped: result.skipped, failed: result.failed, saved: result.saved_text })}</p>}
      {imgs && result.partial && <div className="banner banner-warn">{t("tl_partial", { n: result.pending })}</div>}
      {shown.length > 0 && <ul className="space-y-1.5">{shown.map((o) => <Output key={o.path} out={o} />)}</ul>}
      {outs.length > shown.length && <p className="help">{t("tl_outputs_more", { n: outs.length - shown.length })}</p>}
      {folder && <p className="help">{t("tl_saved_in")}: <span className="mono">{folder}</span></p>}
      {failedItems.length > 0 && (
        <details>
          <summary className="font-semibold">{t("tl_item_failed")} <Chip>{failedItems.length}</Chip></summary>
          <ul className="help mt-2 space-y-1 pl-4">{failedItems.slice(0, 40).map((i, k) => <li key={k} style={{ overflowWrap: "anywhere" }}>{i.source}: {i.reason}</li>)}</ul>
        </details>
      )}
      {(result.notes || []).map((n, i) => <p key={i} className="help">{n}</p>)}
    </section>
  );
}

// ------------------------------------------------------------------ info and metadata
function Info({ info, onSave, busy }) {
  const { t } = useApp();
  const meta = info.metadata || {};
  const [draft, setDraft] = useState({ title: meta.title || "", author: meta.author || "", subject: meta.subject || "", keywords: meta.keywords || "" });
  const changedKeys = ["title", "author", "subject", "keywords"].filter((k) => draft[k] !== (meta[k] || ""));
  return (
    <section className="panel space-y-3" aria-label={t("op_info")}>
      {info.locked ? <div className="banner banner-warn">{t("tl_locked")}</div> : (
        <>
          <dl className="tl-facts">
            <div><dt>{t("tl_info_pages")}</dt><dd className="num">{info.pages}</dd></div>
            <div><dt>{t("tl_info_size")}</dt><dd className="num">{info.size_text}</dd></div>
            <div><dt>{t("tl_info_text")}</dt><dd>{info.scanned ? t("tl_scanned") : t("tl_has_text")}</dd></div>
            {info.encrypted && <div><dt>{t("tl_encrypted")}</dt><dd>AES</dd></div>}
            <div className="tl-facts-wide"><dt>{t("tl_info_sizes")}</dt><dd>{(info.page_sizes || []).map((s) => `${s.pages}: ${s.name || ""} ${s.width_mm}×${s.height_mm} mm`.replace("  ", " ")).join(" · ")}</dd></div>
          </dl>
          {(info.notes || []).map((n, i) => <p key={i} className="help">{n}</p>)}
          <div className="grid gap-3 sm:grid-cols-2">
            <Text label={t("tl_meta_title")} value={draft.title} onChange={(v) => setDraft({ ...draft, title: v })} />
            <Text label={t("tl_meta_author")} value={draft.author} onChange={(v) => setDraft({ ...draft, author: v })} />
            <Text label={t("tl_meta_subject")} value={draft.subject} onChange={(v) => setDraft({ ...draft, subject: v })} />
            <Text label={t("tl_meta_keywords")} value={draft.keywords} onChange={(v) => setDraft({ ...draft, keywords: v })} />
          </div>
          <p className="help">{t("tl_meta_hint")}</p>
          <Busy className="btn btn-sm btn-primary" busy={busy} disabled={!changedKeys.length} onClick={() => onSave(Object.fromEntries(changedKeys.map((k) => [k, draft[k]])))}>{t("tl_meta_save")}</Busy>
        </>
      )}
    </section>
  );
}

// ------------------------------------------------------------------ page
export default function Taller({ query }) {
  const { t, notify } = useApp();
  const [busy, run] = useBusy();
  const status = useLoad(() => api.workshopStatus(), []);
  const [opId, setOpId] = useState(query?.get("op") || null);
  const [files, setFiles] = useState([]);
  const [job, setJob] = useState("");
  const [o, setO] = useState(DEFAULTS);
  const [password, setPassword] = useState("");
  const [needPw, setNeedPw] = useState(false);
  const [result, setResult] = useState(null);
  const [info, setInfo] = useState(null);
  const [error, setError] = useState(null);
  const [doc, setDoc] = useState(null);
  const docId = query?.get("doc") || "";

  const op = OPS.find((x) => x.id === opId) || null;
  const set = (key, value) => setO((v) => ({ ...v, [key]: value }));

  // A document opened from its detail page: offered as the first file of any PDF operation.
  useEffect(() => {
    if (!docId) { setDoc(null); return; }
    setDoc({ id: docId, title: docId });
    api.document(docId).then((r) => setDoc({ id: docId, title: r?.document?.title || docId })).catch(() => {});
  }, [docId]);

  const choose = (next) => {
    setOpId(next.id);
    setResult(null);
    setInfo(null);
    setError(null);
    setNeedPw(false);
    setPassword("");
    // Keep what is still valid for the new operation (a PDF stays for the next PDF operation).
    setFiles((list) => {
      const kind = (f) => (f.kind === "doc" ? "pdf" : f.ext || extOf(f.name));
      const keep = list.filter((f) => f.kind === "doc" ? next.accepts === "pdf" : (f.kind === "path" && !extOf(f.name) && next.folders) || (next.accepts === "pdf" ? kind(f) === "pdf" : next.accepts === "office" ? OFFICE_EXT.includes(kind(f)) : IMAGE_EXT.includes(kind(f))));
      const withDoc = doc && next.accepts === "pdf" && !keep.some((f) => f.ref === doc.id) ? [{ ref: doc.id, name: doc.title, kind: "doc", ranges: "" }, ...keep] : keep;
      return next.multi ? withDoc : withDoc.slice(0, 1);
    });
  };

  const call = async (tool, args) => {
    try {
      const r = await api.call(tool, args);
      if (r && r.ok === false) throw Object.assign(new Error(r.error || "Error"), { hint: r.hint });
      return r;
    } catch (e) {
      if (/contraseña|password|protegid/i.test(e.message || "")) setNeedPw(true);
      setError(e);
      return null;
    }
  };

  const submit = (e) => {
    e?.preventDefault();
    const built = build(op, files, o, password, t);
    if (built.error) { notify(built.error, "error"); return; }
    setError(null);
    setResult(null);
    setInfo(null);
    run("go", async () => {
      const r = await call(built.tool, built.args);
      if (!r) return;
      if (built.tool === "pdf_info") setInfo(r);
      else setResult(r);
    });
  };

  const saveMeta = (values) => {
    setError(null);
    run("meta", async () => {
      const r = await call("pdf_metadata_set", { file: files[0].ref, ...values, ...(password ? { password } : {}) });
      if (r) setResult(r);
    });
  };

  const showPassword = needPw || files.some((f) => f.encrypted);

  if (!op) {
    return (
      <div className="space-y-5">
        <header>
          <h1>{t("nav_workshop")}</h1>
          <p className="help">{t("workshop_intro")}</p>
        </header>
        {doc && <div className="banner banner-info">{t("tl_pre_doc", { name: doc.title })}</div>}
        <Section id="sec-tools" title={t("tl_all_tools")}>
          <div className="tl-grid">
            {OPS.map((x) => (
              <button type="button" key={x.id} className="tl-tile" onClick={() => choose(x)}>
                <span className="tl-tile-icon"><Icon d={x.icon} size={22} /></span>
                <span className="font-semibold">{t(`op_${x.id}`)}</span>
                <span className="help">{t(`op_${x.id}_d`)}</span>
              </button>
            ))}
          </div>
        </Section>
        <Section id="sec-taxpack" title={t("tx_pack_title")}><TaxPack /></Section>
      </div>
    );
  }

  const isInfo = op.id === "info";
  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center gap-2">
        <button type="button" className="btn btn-sm" onClick={() => { setOpId(null); setResult(null); setInfo(null); setError(null); }}><Icon d={ICONS.back} size={13} />{t("nav_workshop")}</button>
      </div>
      <header>
        <h1>{t(`op_${op.id}`)}</h1>
        <p className="help">{t(`op_${op.id}_d`)}</p>
      </header>
      <form className="space-y-5" onSubmit={submit} noValidate>
        <Section id="sec-files" title={t("tl_files")} count={files.length}>
          <FilePicker op={op} files={files} setFiles={setFiles} job={job} setJob={setJob} status={status.data} doc={doc} />
        </Section>
        {(showPassword && op.accepts === "pdf") && (
          <div className="panel">
            <Text label={t("tl_src_password")} hint={t("tl_src_password_hint")} type="password" autoComplete="off" value={password} onChange={setPassword} />
          </div>
        )}
        {!isInfo && op.id !== "merge" && (
          <Section id="sec-options" title={t("tl_options")}>
            <div className="panel"><Options op={op} o={o} set={set} status={status.data} /></div>
          </Section>
        )}
        <ErrorBox error={error} />
        <div className="flex flex-wrap items-center gap-2">
          <Busy type="submit" className="btn btn-primary" busy={busy.go}>{isInfo ? t("tl_show_info") : t("tl_run")}</Busy>
        </div>
      </form>
      {info && <Info key={`${info.file}-${info.size}`} info={info} onSave={saveMeta} busy={busy.meta} />}
      {result && <Result result={result} />}
    </div>
  );
}
