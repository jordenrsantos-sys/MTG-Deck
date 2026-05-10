import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// Vitest config separated from vite.config.ts so each file uses its own
// `defineConfig` flavor (vite's UserConfigExport vs vitest's UserConfigExport).
export default defineConfig({
  plugins: [react()],
  test: {
    environment: "node",
    include: ["src/**/__tests__/**/*.test.ts", "src/**/*.test.ts"],
  },
});
