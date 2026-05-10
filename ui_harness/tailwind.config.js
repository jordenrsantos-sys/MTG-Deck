/**
 * Tailwind config — Phase 4.2 design system foundation.
 *
 * Folds src/ui/tokens.css design tokens into theme.extend so dark-glass
 * tokens flow through unchanged. Existing CSS variable names from
 * tokens.css remain the source of truth — Tailwind reads them via
 * `var(--token-name)` so changes to tokens.css ripple automatically.
 *
 * Per Phase 4.1 Decision 1: Tailwind utility-only; no shadcn vendor lock.
 * Per safety: NO ui_contract_v1 widening; UI primitives are presentation only.
 */
/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{ts,tsx,js,jsx}",
  ],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        "bg-base": "var(--bg-base)",
        "bg-elev-1": "var(--bg-elev-1)",
        "bg-elev-2": "var(--bg-elev-2)",
        "glass-bg": "var(--glass-bg)",
        "glass-border": "var(--glass-border)",
        "text-primary": "var(--text-primary)",
        "text-secondary": "var(--text-secondary)",
        "text-muted": "var(--text-muted)",
        accent: {
          DEFAULT: "var(--accent)",
          hover: "var(--accent-hover)",
          active: "var(--accent-active)",
        },
      },
      borderRadius: {
        "token-sm": "var(--radius-sm)",
        "token-md": "var(--radius-md)",
        "token-lg": "var(--radius-lg)",
      },
      spacing: {
        "token-1": "var(--space-1)",
        "token-2": "var(--space-2)",
        "token-3": "var(--space-3)",
        "token-4": "var(--space-4)",
        "token-5": "var(--space-5)",
        "panel-pad": "var(--panel-pad)",
        "panel-gap": "var(--panel-gap)",
        "panel-title-gap": "var(--panel-title-gap)",
      },
      boxShadow: {
        "token-soft": "var(--shadow-soft)",
        "token-elevated": "var(--shadow-elevated)",
        "token-rail": "var(--shadow-rail)",
        "token-panel": "var(--shadow-panel)",
        "token-sticky": "var(--shadow-sticky)",
        "token-modal": "var(--shadow-modal)",
        "focus-ring": "var(--focus-ring)",
      },
      backdropBlur: {
        glass: "var(--glass-blur)",
      },
      zIndex: {
        rail: "var(--z-depth-rail)",
        panel: "var(--z-depth-panel)",
        sticky: "var(--z-depth-sticky)",
        modal: "var(--z-depth-modal)",
      },
      transitionDuration: {
        fast: "150ms",
        med: "280ms",
      },
      transitionTimingFunction: {
        med: "cubic-bezier(0.22, 1, 0.36, 1)",
      },
    },
  },
  plugins: [],
};
