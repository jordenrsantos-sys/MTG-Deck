/**
 * LeftRail — v1.6 Stage 1 redesign (DeckCheck-style drawer nav).
 *
 * Was a permanent always-visible rail on ≥md viewports (Phase 4.x) with a
 * partial hamburger-Dialog flavor for mobile. v1.6 converts to a single
 * drawer overlay pattern for all viewports — DeckCheck-style:
 *
 *   - Closed by default. Hamburger button at top-left toggles open.
 *   - Slide-in from left via the new Drawer primitive (sibling of Dialog).
 *   - Backdrop dim + click-outside-to-close + Escape-to-close (Drawer
 *     primitive handles these).
 *   - Entries grouped under uppercase section dividers: "My Stuff" / "Play"
 *     / "Settings". Each entry: icon (inline SVG) + full label, NOT
 *     2-letter abbreviations.
 *   - Runs + Diagnostics gated behind `import.meta.env.DEV` — production
 *     users never see dev-only routes. Surfaced in a "Developer Tools"
 *     section at the bottom of the drawer when DEV.
 *
 * Hamburger trigger position: top-left of the workspace header. Persistent
 * fixed-position so it's reachable across the full viewport.
 *
 * Per HARD #13: inline SVG icons only (no lucide-react / heroicons dep).
 */
import { useEffect, useState } from "react";
import type { ReactNode } from "react";

import { Drawer, DrawerContent, DrawerTitle } from "../../ui/primitives/Drawer";
import HamburgerButton from "./HamburgerButton";

type RailLink = {
  label: string;
  href: string;
  view: "workspace" | "diagnostics" | "playtest" | "settings" | "landing" | "decks";
  icon: ReactNode;
};

// Inline SVG icon helpers — small, ~18px, currentColor stroke.
function _icon(path: ReactNode) {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {path}
    </svg>
  );
}

// v1.6.3 Stage 1: HOME redrawn to fill 20×20 effective bounding box
// (was 18×17 in v1.6 — y short by 1 from the 18-22 tolerance). Uses
// polyline primitives for testability (the v1.6.3 leftRailIconBounds
// test parses geometry directly; polylines are trivial to bound-check).
// BEFORE bounds: x=3..21 (18), y=3..20 (17). AFTER bounds: x=2..22 (20),
// y=2..22 (20).
const ICON_HOME = _icon(
  <>
    <polyline points="2,12 12,2 22,12" />
    <polyline points="4,10 4,22 20,22 20,10" />
  </>,
);
const ICON_DECKS = _icon(
  <>
    <rect x="3" y="3" width="18" height="18" rx="2" />
    <line x1="3" y1="9" x2="21" y2="9" />
    <line x1="9" y1="3" x2="9" y2="21" />
  </>,
);
// v1.6.3 Stage 1: PLAY redrawn to fill 18×18 effective. BEFORE bounds:
// x=6..20 (14), y=4..20 (16) — both below the 18-22 tolerance. AFTER
// bounds: x=4..22 (18), y=3..21 (18). Right-pointing triangle, same
// metaphor, wider footprint.
const ICON_PLAY = _icon(<polygon points="4,3 22,12 4,21" />);
const ICON_SETTINGS = _icon(
  <>
    <circle cx="12" cy="12" r="3" />
    <path d="M12 1v6m0 10v6m11-11h-6m-10 0H1m17.5-7.5l-4.2 4.2m-8.6 8.6l-4.2 4.2m0-17l4.2 4.2m8.6 8.6l4.2 4.2" />
  </>,
);
const ICON_RUNS = _icon(
  <>
    <circle cx="12" cy="12" r="10" />
    <polyline points="12 6 12 12 16 14" />
  </>,
);
// v1.6.3 Stage 1: DIAG redrawn to fill 20×18 effective. BEFORE bounds:
// x=5..19 (14), y=3..21 (18) — x below the 18-22 tolerance. AFTER
// bounds: x=2..22 (20), y=4..22 (18). New metaphor: diagnostic monitor
// (rect screen with internal waveform + small stand) — fills the
// 20-wide footprint better than the narrow document-with-corner-fold
// path used in v1.6, and uses simple primitives that the bounds parser
// can verify without arc-command handling.
const ICON_DIAG = _icon(
  <>
    <rect x="2" y="4" width="20" height="14" rx="2" />
    <polyline points="5,11 8,11 10,8 12,14 14,11 19,11" />
    <line x1="12" y1="18" x2="12" y2="22" />
    <line x1="8" y1="22" x2="16" y2="22" />
  </>,
);

const SECTION_MY_STUFF: RailLink[] = [
  { label: "Home", href: "#/", view: "landing", icon: ICON_HOME },
  { label: "Decks", href: "#decks", view: "decks", icon: ICON_DECKS },
];
const SECTION_PLAY: RailLink[] = [
  { label: "Playtest", href: "#playtest", view: "playtest", icon: ICON_PLAY },
];
const SECTION_SETTINGS: RailLink[] = [
  { label: "Settings", href: "#settings", view: "settings", icon: ICON_SETTINGS },
];
const SECTION_DEV: RailLink[] = [
  { label: "Runs", href: "#workspace-runs", view: "workspace", icon: ICON_RUNS },
  { label: "Diagnostics", href: "#diagnostics", view: "diagnostics", icon: ICON_DIAG },
];

const WORKSPACE_DEFAULT_HASH = "#workspace-decks";

function normalizeHash(value: string): string {
  return value.trim().toLowerCase();
}

function resolveActiveWorkspaceHash(hashValue: string): string {
  if (hashValue === "#workspace-runs") {
    return "#workspace-runs";
  }
  return WORKSPACE_DEFAULT_HASH;
}

// v1.6: read DEV flag once at module load — Vite/esbuild inlines the
// boolean at build time, so production bundles literally never include
// the Dev Tools section under the false branch (dead-code-eliminated).
// Tests can override by setting `globalThis.__MTG_FORCE_DEV_NAV__` true
// (the override is OR'd in).
function _isDevMode(): boolean {
  let metaDev = false;
  try {
    metaDev = Boolean((import.meta as ImportMeta).env?.DEV);
  } catch {
    metaDev = false;
  }
  const override = Boolean(
    (globalThis as { __MTG_FORCE_DEV_NAV__?: boolean }).__MTG_FORCE_DEV_NAV__,
  );
  return metaDev || override;
}

type SectionProps = {
  title: string;
  items: RailLink[];
  activeHash: string;
  activeWorkspaceHash: string;
};

function NavSection(props: SectionProps) {
  const { title, items, activeHash, activeWorkspaceHash } = props;
  if (items.length === 0) return null;
  return (
    <div className="mt-token-3">
      <p
        className="text-xs uppercase tracking-wider text-text-muted px-token-2 py-token-1 border-t border-glass-border pt-token-2"
        aria-hidden="true"
      >
        {title}
      </p>
      <nav aria-label={title}>
        {items.map((link) => {
          let isActive = false;
          if (link.view === "diagnostics") isActive = activeHash === "#diagnostics";
          else if (link.view === "playtest") isActive = activeHash === "#playtest";
          else if (link.view === "settings") isActive = activeHash === "#settings";
          else if (link.view === "decks") isActive = activeHash === "#decks";
          else if (link.view === "landing") isActive = activeHash === "#/" || activeHash === "";
          else
            isActive =
              activeHash !== "#diagnostics" &&
              activeHash !== "#playtest" &&
              activeHash !== "#settings" &&
              activeHash !== "#decks" &&
              activeHash !== "#/" &&
              activeHash !== "" &&
              link.href === activeWorkspaceHash;
          const classes = [
            "workspace-left-rail-link flex items-center gap-token-2",
            "px-token-2 py-token-2 rounded-token-sm",
            isActive
              ? "is-active bg-accent/15 text-accent"
              : "text-text-secondary hover:bg-bg-elev-2 hover:text-text-primary",
          ].join(" ");
          return (
            <a
              key={link.label}
              href={link.href}
              className={classes}
              aria-current={isActive ? "page" : undefined}
            >
              <span className="workspace-left-rail-icon" aria-hidden="true">
                {link.icon}
              </span>
              <span className="workspace-left-rail-text font-medium">{link.label}</span>
            </a>
          );
        })}
      </nav>
    </div>
  );
}

function _readHashSafe(): string {
  if (typeof window === "undefined" || !window.location) return "";
  return normalizeHash(window.location.hash);
}

// v1.6.2 Stage 1: LeftRail now supports optional controlled-mode props
// (`open` / `onOpenChange`) + a `renderHamburger` toggle so parent layouts
// (e.g. the AppRouter app-shell header bar) can own the drawer-open state
// and render the hamburger inline at their own position. Defaults preserve
// back-compat with v1.6 + v1.6.1 call sites that render `<LeftRail />`
// without props: hamburger stays fixed-position at top-2 left-2 z-modal,
// state stays internal.
export type LeftRailProps = {
  /** Controlled-mode: when provided, drawer-open is managed by the parent.
   *  When omitted, LeftRail uses its own useState (uncontrolled mode). */
  open?: boolean;
  /** Controlled-mode change handler. Called whenever drawer-open should
   *  flip (hamburger click + Escape + backdrop + hashchange navigation). */
  onOpenChange?: (open: boolean) => void;
  /** Hamburger render mode. `"fixed"` (default, back-compat with v1.6/v1.6.1):
   *  HamburgerButton renders inside a `fixed top-2 left-2 z-modal` div.
   *  `"inline"`: HamburgerButton renders inline (no positioning wrapper) so
   *  the parent can place it inside its own layout container.
   *  `"none"`: HamburgerButton not rendered — parent renders its own and
   *  drives state via the controlled-mode props above. */
  renderHamburger?: "fixed" | "inline" | "none";
};

export default function LeftRail(props: LeftRailProps = {}) {
  const { open: controlledOpen, onOpenChange: controlledOnChange, renderHamburger = "fixed" } = props;
  const [hashValue, setHashValue] = useState(() => _readHashSafe());
  const [internalOpen, setInternalOpen] = useState(false);

  const isControlled = controlledOpen !== undefined;
  const drawerOpen = isControlled ? Boolean(controlledOpen) : internalOpen;
  const setDrawerOpen = (next: boolean): void => {
    if (isControlled) {
      controlledOnChange?.(next);
    } else {
      setInternalOpen(next);
    }
  };

  useEffect(() => {
    if (typeof window === "undefined") return;
    function onHashChange(): void {
      setHashValue(_readHashSafe());
      // Close the drawer after navigating so the user lands on the new view.
      setDrawerOpen(false);
    }

    window.addEventListener("hashchange", onHashChange);
    return () => {
      window.removeEventListener("hashchange", onHashChange);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isControlled, controlledOnChange]);

  const activeWorkspaceHash = resolveActiveWorkspaceHash(hashValue);
  const showDevSection = _isDevMode();

  return (
    <>
      {/* v1.6.2 Stage 1: hamburger render is now mode-gated. "fixed"
          (default) preserves v1.6/v1.6.1 behavior; "none" lets a parent
          shell render its own hamburger in a header bar; "inline" renders
          without positioning wrapper. */}
      {renderHamburger === "fixed" ? (
        <div className="fixed top-2 left-2 z-modal">
          <HamburgerButton
            open={drawerOpen}
            onToggle={() => setDrawerOpen(!drawerOpen)}
          />
        </div>
      ) : renderHamburger === "inline" ? (
        <HamburgerButton
          open={drawerOpen}
          onToggle={() => setDrawerOpen(!drawerOpen)}
        />
      ) : null}

      <Drawer open={drawerOpen} onOpenChange={(next) => setDrawerOpen(next)}>
        <DrawerContent>
          <div className="flex items-center gap-token-2 mb-token-3">
            <span
              className="inline-flex items-center justify-center w-8 h-8 rounded-token-sm bg-accent/20 text-accent font-bold text-sm"
              aria-hidden="true"
            >
              ME
            </span>
            <DrawerTitle className="mb-0">MTG Engine</DrawerTitle>
          </div>

          <NavSection
            title="My Stuff"
            items={SECTION_MY_STUFF}
            activeHash={hashValue}
            activeWorkspaceHash={activeWorkspaceHash}
          />
          <NavSection
            title="Play"
            items={SECTION_PLAY}
            activeHash={hashValue}
            activeWorkspaceHash={activeWorkspaceHash}
          />
          <NavSection
            title="Settings"
            items={SECTION_SETTINGS}
            activeHash={hashValue}
            activeWorkspaceHash={activeWorkspaceHash}
          />
          {showDevSection ? (
            <NavSection
              title="Developer Tools"
              items={SECTION_DEV}
              activeHash={hashValue}
              activeWorkspaceHash={activeWorkspaceHash}
            />
          ) : null}
        </DrawerContent>
      </Drawer>
    </>
  );
}

// Test-only exports for vitest coverage — keep internal helpers + section
// vocabularies inspectable without exporting them on the runtime API.
export const __testing = {
  SECTION_MY_STUFF,
  SECTION_PLAY,
  SECTION_SETTINGS,
  SECTION_DEV,
  isDevMode: _isDevMode,
};
