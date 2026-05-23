import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Phase 4.3: dev-server proxy so the UI can call Engine-4A's
// `POST /import/url` (and the existing `/build`, `/cards/*` family) at
// the same origin during development. Without this, browser CORS blocks
// the cross-port call from Vite (5173) to FastAPI (8000).
//
// Per autonomous_repair_log soft-safety #2 (CORS/proxy config): Vite
// proxy entry was the documented auto-repair path; the underlying
// FastAPI app at api/main.py BYTE-IDENTICAL pre/post Phase 4.3 (no
// Engine-4A changes per HARD safety).
//
// Override at dev-time via VITE_API_TARGET (Vite's import.meta.env), e.g.
//   VITE_API_TARGET=http://127.0.0.1:9000 npm run dev
// Default targets local FastAPI default port. Vitest config lives in
// vitest.config.ts so this file stays in vite's UserConfigExport shape.

const API_TARGET = "http://127.0.0.1:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/import": { target: API_TARGET, changeOrigin: true },
      "/build": { target: API_TARGET, changeOrigin: true },
      "/cards": { target: API_TARGET, changeOrigin: true },
      "/health": { target: API_TARGET, changeOrigin: true },
      "/snapshots": { target: API_TARGET, changeOrigin: true },
      "/snapshot": { target: API_TARGET, changeOrigin: true },
      // Mega-task v6 Phase 1: route /agent through the proxy so SSE streaming
      // is same-origin in dev. Server-side CORS+SSE both verified clean via
      // tools/mega_task_v6_phase1_browser_simulation.py; this entry removes
      // the cross-origin variable entirely from the dev workflow so a vite
      // port outside the CORSMiddleware allowlist (e.g. 5175 when 5173 is
      // taken) cannot silently break the build UI.
      "/agent": { target: API_TARGET, changeOrigin: true },
      // Agent surface also includes a few other paths the UI may call later.
      "/deck": { target: API_TARGET, changeOrigin: true },
      "/commander": { target: API_TARGET, changeOrigin: true },
      "/theme": { target: API_TARGET, changeOrigin: true },
      "/card": { target: API_TARGET, changeOrigin: true },
      "/playtest": { target: API_TARGET, changeOrigin: true },
      "/corpus": { target: API_TARGET, changeOrigin: true },
    },
  },
});
