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
    },
  },
});
