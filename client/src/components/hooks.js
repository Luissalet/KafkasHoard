import { api } from "../api.js";
import { useApp } from "../context.js";
import { useBusy } from "./ui.jsx";

// Reads the mail now (or searches back) and reports what was filed.
export function useMailScan(onDone) {
  const { t, notify, toastError, changed } = useApp();
  const [busy, run] = useBusy();
  const scan = (args = {}) => run("scan", async () => {
    const r = await api.call("mail_scan", args);
    if (!r || r.ok === false) {
      toastError(new Error((r && r.error) || t("scan_failed")));
    } else {
      const parts = [t("scan_result", { n: r.messages ?? 0, d: r.documents_created ?? 0 })];
      if (r.maybe) parts.push(t("scan_maybe", { n: r.maybe }));
      notify(parts.join(" · "));
    }
    changed();
    onDone?.(r);
    return r;
  });
  return [busy.scan, scan];
}
