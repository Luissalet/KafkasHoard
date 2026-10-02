// Vocabulary shared with the backend. Labels live in i18n.js.
export const DOC_KINDS = ["invoice", "receipt", "bill", "contract", "insurance", "warranty", "tax", "official_notice", "fine", "vehicle", "identity", "subscription", "payslip", "bank", "manual", "other"];
export const DEADLINE_KINDS = ["payment", "renewal", "cancel_by", "warranty_end", "permanence_end", "appeal", "fine_discount", "expiry", "itv", "tax", "custom"];

// 24x24 stroke icon paths per document kind
export const KIND_ICON = {
  invoice: "M6 3h12v18l-3-2-3 2-3-2-3 2zM9 8h6M9 12h6",
  receipt: "M6 3h12v18l-3-2-3 2-3-2-3 2zM9 8h6M9 12h3",
  bill: "M4 6h16v12H4zM4 10h16M8 15h3",
  contract: "M7 3h8l4 4v14H7zM15 3v4h4M10 12h6M10 16h6",
  insurance: "M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z",
  warranty: "M12 3l2.6 5.3 5.9.9-4.3 4.1 1 5.8L12 16.3 6.8 19.1l1-5.8L3.5 9.2l5.9-.9z",
  tax: "M5 19L19 5M7.5 8.5a1.5 1.5 0 100-3 1.5 1.5 0 000 3zM16.5 18.5a1.5 1.5 0 100-3 1.5 1.5 0 000 3z",
  official_notice: "M4 7l8 5 8-5M4 7v10h16V7zM4 7l2-3h12l2 3",
  fine: "M12 3l10 18H2zM12 10v5M12 18h.01",
  vehicle: "M3 15l1.5-5h15L21 15v4h-3v-2H6v2H3zM6.5 12.5h.01M17.5 12.5h.01",
  identity: "M3 6h18v12H3zM8 12a2 2 0 100-4 2 2 0 000 4zM5.5 16c.5-1.5 1.7-2 2.5-2s2 .5 2.5 2M14 10h4M14 13h3",
  subscription: "M3 12a9 9 0 0115-6.7L21 8M21 3v5h-5M21 12a9 9 0 01-15 6.7L3 16M3 21v-5h5",
  payslip: "M5 4h14v16H5zM8 9h8M8 13h8M8 17h4",
  bank: "M3 10l9-6 9 6M5 10v8M9 10v8M15 10v8M19 10v8M3 20h18",
  manual: "M4 5a2 2 0 012-2h12v16H6a2 2 0 00-2 2zM4 5v16M8 7h7M8 11h5",
  other: "M7 3h8l4 4v14H7zM15 3v4h4",
};

export const SEVERITY_CLASS = { high: "chip-danger", medium: "chip-amber", low: "" };
export const CHANNELS = ["toast", "hub", "ntfy", "telegram", "email"];
export const SEVERITIES = ["low", "medium", "high"];
export const REGIONS = ["", "ES-MD", "ES-CT", "ES-AN", "ES-VC", "ES-GA", "ES-PV"];
export const RECURRING = ["none", "monthly", "yearly"];
export const WARRANTY_KINDS = new Set(["receipt", "invoice", "warranty"]);
