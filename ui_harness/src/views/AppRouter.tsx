import { useEffect, useState } from "react";

import DiagnosticsView from "./DiagnosticsView";
import ImportRoute from "./ImportRoute";
import WorkspaceView from "./WorkspaceView";
import GoldfishView from "./GoldfishView";
import LandingView from "./LandingView";
import SettingsView from "./SettingsView";
import SavedDecksView from "./SavedDecksView";
import ErrorBoundary from "../ui/primitives/ErrorBoundary";
import { useHealthPrewarm } from "../lib/useHealthPrewarm";
// v1.6.1 hotfix Stage 2: LeftRail hoisted from WorkspaceView (where it was
// at line 4223 in v1.6) up to AppRouter as a persistent sibling of every
// view-switch branch. Symptom that motivated the hoist: hamburger button
// disappeared on every non-workspace route because AppRouter unmounts
// WorkspaceView when navigating to #decks / #diagnostics / #playtest /
// #settings / #/, taking LeftRail with it. Hoisting to AppRouter ensures
// the hamburger persists across all 7 ViewId values.
import LeftRail from "../components/layout/LeftRail";
// v1.6.2 Stage 1: HamburgerButton hoisted from LeftRail's fixed-position
// overlay into the AppRouter app-shell header bar so it stops clipping
// page content. LeftRail still owns the drawer panel; AppRouter owns
// drawer-open state + the hamburger render site via LeftRail's new
// controlled-mode props (added Stage 1 to keep the lift additive).
import HamburgerButton from "../components/layout/HamburgerButton";

type ViewId = "workspace" | "diagnostics" | "import" | "playtest" | "landing" | "settings" | "decks";

const API_BASE_URL =
  ((import.meta as ImportMeta).env?.VITE_API_BASE_URL as string | undefined) ??
  "http://localhost:8000";

// Exported for vitest coverage of the Phase 4.12.1 hotfix (the URL→view
// mapping IS the regression surface — keep it pure + testable).
//
// v1.6.1 hotfix HARD safety: parseHash is BYTE-IDENTICAL from v1.6 (and
// from Phase 4.12.1 before that). The hash-to-view mapping is a contract
// — Stage 2's hoist must NOT alter it.
export function parseHash(hashValue: string): ViewId {
  const token = hashValue.replace(/^#/, "").trim().toLowerCase();
  if (token === "diagnostics") return "diagnostics";
  if (token === "import") return "import";
  if (token === "playtest") return "playtest";
  if (token === "settings") return "settings";
  if (token === "decks") return "decks";
  // Phase 4.12b: explicit landing route at `#/`. Existing workspace hashes
  // (`#workspace-decks` / `#workspace-runs`) continue to render WorkspaceView.
  if (token === "/" || token === "") return "landing";
  if (token.startsWith("workspace")) return "workspace";
  return "landing";
}

// v1.6.1 hotfix Stage 2: extracted per-view render into a pure helper so
// the AppRouter return JSX can wrap LeftRail + the per-view ErrorBoundary
// in a single Fragment without duplicating the LeftRail render across 7
// early-return branches. Behavior BYTE-IDENTICAL — same ErrorBoundary per
// view + same onBack wiring; only the JSX shape changed (autonomous_repair).
function _renderViewBranch(view: ViewId, openWorkspace: () => void) {
  if (view === "diagnostics") {
    return (
      <ErrorBoundary panelLabel="Diagnostics">
        <DiagnosticsView onBack={openWorkspace} />
      </ErrorBoundary>
    );
  }

  if (view === "import") {
    return (
      <ErrorBoundary panelLabel="Import">
        <ImportRoute onBack={openWorkspace} />
      </ErrorBoundary>
    );
  }

  if (view === "playtest") {
    return (
      <ErrorBoundary panelLabel="Playtest">
        <GoldfishView onBack={openWorkspace} />
      </ErrorBoundary>
    );
  }

  if (view === "landing") {
    return (
      <ErrorBoundary panelLabel="Landing">
        <LandingView />
      </ErrorBoundary>
    );
  }

  if (view === "settings") {
    return (
      <ErrorBoundary panelLabel="Settings">
        <SettingsView onBack={openWorkspace} />
      </ErrorBoundary>
    );
  }

  if (view === "decks") {
    return (
      <ErrorBoundary panelLabel="SavedDecks">
        <SavedDecksView onBack={openWorkspace} />
      </ErrorBoundary>
    );
  }

  return (
    <ErrorBoundary panelLabel="Workspace">
      <WorkspaceView />
    </ErrorBoundary>
  );
}

// v1.6.1 hotfix Stage 2 SOFT-safety addition: defensive window read so
// AppRouter renders cleanly under SSR / test env (renderToString-based
// vitest cases for the LeftRail persistence guard). Browser behavior
// BYTE-IDENTICAL — `window` is defined and the lookup returns the same
// `window.location.hash` value the previous version read directly.
function _readHashSafe(): string {
  if (typeof window === "undefined" || !window.location) return "";
  return window.location.hash;
}

export default function AppRouter() {
  const [view, setView] = useState<ViewId>(() => parseHash(_readHashSafe()));
  // v1.6.2 Stage 1: drawer-open state hoisted from LeftRail into AppRouter
  // so the header-bar HamburgerButton + LeftRail's drawer panel share one
  // source of truth. LeftRail uses controlled-mode (open + onOpenChange
  // props) when these are passed.
  const [drawerOpen, setDrawerOpen] = useState(false);

  // Phase 4 BUNDLE Stage 5 polish: one-shot /health prewarm to avoid the
  // 1.4s cold-start cost on the first /build.
  useHealthPrewarm(API_BASE_URL);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const onHashChange = () => {
      setView(parseHash(_readHashSafe()));
    };

    window.addEventListener("hashchange", onHashChange);
    return () => {
      window.removeEventListener("hashchange", onHashChange);
    };
  }, []);

  function openWorkspace() {
    // Phase 4.12.1 hotfix: explicit `#workspace` hash aligns the URL with the
    // rendered view. Setting hash to "" triggered a hashchange ~4ms later
    // that ran `setView(parseHash(""))` → "landing", clobbering the explicit
    // setView("workspace") below and orphaning the staged-import slot
    // (WorkspaceView would mount, consume + clear the slot, then unmount).
    window.location.hash = "#workspace";
    setView("workspace");
  }

  // v1.6.1 hotfix Stage 2: LeftRail rendered as a persistent sibling of the
  // per-view branch. Required no props (self-contained per v1.6 Stage 1
  // implementation: drawer state, hash listener, DEV gate all internal).
  //
  // v1.6.2 Stage 1: wrapped in an outer app-shell with a thin 48px sticky
  // header bar containing the HamburgerButton inline (no more fixed-position
  // overlay clipping). LeftRail renders with renderHamburger="none" + the
  // controlled-mode props so AppRouter is the single source of truth for
  // drawer-open state. View content sits below the header bar in natural
  // document flow.
  return (
    <div className="min-h-screen flex flex-col" data-v162-stage="app-shell">
      <header
        className="sticky top-0 left-0 right-0 h-12 z-modal bg-bg-base border-b border-glass-border flex items-center gap-token-2 px-token-2"
        role="banner"
        aria-label="App shell header"
        data-v162-stage="app-shell-header"
      >
        <HamburgerButton
          open={drawerOpen}
          onToggle={() => setDrawerOpen((prev) => !prev)}
        />
      </header>
      <LeftRail
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        renderHamburger="none"
      />
      <div className="flex-1" data-v162-stage="app-shell-content">
        {_renderViewBranch(view, openWorkspace)}
      </div>
    </div>
  );
}
