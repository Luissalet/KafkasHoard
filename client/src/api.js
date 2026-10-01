// Thin fetch wrapper: JSON in/out. `{ error, code, hint }` bodies become exceptions that keep code and hint.
async function request(method, path, { params, body } = {}) {
  const url = new URL(path, window.location.origin);
  for (const [key, value] of Object.entries(params || {})) {
    if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, value);
  }
  let response;
  try {
    response = await fetch(url, {
      method,
      headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
  } catch (cause) {
    const error = new Error(cause && cause.message ? cause.message : "Network error");
    error.code = "network";
    throw error;
  }
  const text = await response.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { error: text.slice(0, 300) };
  }
  if (!response.ok) {
    const detail = data && (data.error || data.detail);
    const error = new Error((typeof detail === "string" && detail) || `Error ${response.status}`);
    error.code = data && data.code;
    error.hint = data && data.hint;
    error.status = response.status;
    throw error;
  }
  return data;
}

// Every write and most reads go through the same tool handlers the assistant uses.
const call = (name, args) => request("POST", "/api/ui/call", { body: { name, arguments: args || {} } });

export const api = {
  health: () => request("GET", "/api/health"),
  dashboard: () => request("GET", "/api/dashboard"),
  visit: () => request("POST", "/api/dashboard/visit", { body: {} }),
  document: (id) => request("GET", `/api/documents/${encodeURIComponent(id)}`),
  fileUrl: (id) => `/api/documents/${encodeURIComponent(id)}/file`,
  pageUrl: (id, n) => `/api/documents/${encodeURIComponent(id)}/page/${n}.png`,
  upload: async (files) => {
    const form = new FormData();
    for (const f of files) form.append("files", f, f.name);
    let response;
    try {
      response = await fetch("/api/documents/upload", { method: "POST", body: form });
    } catch (cause) {
      const error = new Error(cause && cause.message ? cause.message : "Network error");
      error.code = "network";
      throw error;
    }
    const data = await response.json().catch(() => null);
    if (!response.ok) {
      const error = new Error((data && (data.error || data.detail)) || `Error ${response.status}`);
      error.hint = data && data.hint;
      throw error;
    }
    return data;
  },
  call,
};

// Message for a toast: the backend's error plus its hint.
export function errorText(error) {
  if (!error) return "";
  const base = error.message || String(error);
  return error.hint ? `${base} — ${error.hint}` : base;
}
