import React, { useRef, useState } from "react";
import { useApp } from "../context.js";
import { Icon, ICONS, Spinner } from "./ui.jsx";

// Drop zone + file picker. Uploads go through the app-level `upload`, which toasts the result and refreshes the data.
export default function UploadZone({ compact = false }) {
  const { t, upload, uploading } = useApp();
  const input = useRef(null);
  const [over, setOver] = useState(false);
  const pick = (files) => { if (files && files.length) upload(Array.from(files)); };
  return (
    <div
      className="panel flex flex-wrap items-center gap-3"
      style={{ borderStyle: "dashed", borderColor: over ? "var(--accent)" : undefined, background: over ? "var(--accent-soft)" : undefined, padding: compact ? "10px 12px" : "20px 14px" }}
      onDragOver={(e) => { e.preventDefault(); setOver(true); }}
      onDragLeave={() => setOver(false)}
      onDrop={(e) => { e.preventDefault(); e.stopPropagation(); setOver(false); pick(e.dataTransfer.files); }}
    >
      <Icon d={ICONS.upload} size={compact ? 18 : 26} color="var(--accent)" />
      <div className="min-w-0 flex-1">
        <div className="font-semibold">{t("drop_title")}</div>
        <div className="help">{t("drop_hint")}</div>
      </div>
      <input ref={input} type="file" multiple accept=".pdf,.png,.jpg,.jpeg,.webp,.heic,.tif,.tiff,.eml,.txt,.html,.htm,.docx,.md" hidden aria-label={t("upload_docs")} onChange={(e) => { pick(e.target.files); e.target.value = ""; }} />
      <button type="button" className="btn btn-primary" disabled={uploading} onClick={() => input.current?.click()}>
        {uploading ? <Spinner /> : <Icon d={ICONS.upload} size={14} />}
        {t("choose_files")}
      </button>
    </div>
  );
}
