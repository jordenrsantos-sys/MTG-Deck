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

type ViewId = "workspace" | "diagnostics" | "import" | "playtest" | "landing" | "settings" | "decks";

const API_BASE_URL =
  ((import.meta as ImportMeta).env?.VITE_API_BASE_URL as string | undefined) ??
  "http://localhost:8000";

// Exported for vitest coverage of the Phase 4.12.1 hotfix (the URL→view
// mapping IS the regression surface — keep it pure + testable).
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

export default function AppRouter() {
  const [view, setView] = useState<ViewId>(() => parseHash(window.location.hash));

  // Phase 4 BUNDLE Stage 5 polish: one-shot /health prewarm to avoid the
  // 1.4s cold-start cost on the first /build.
  useHealthPrewarm(API_BASE_URL);

  useEffect(() => {
    const onHashChange = () => {
      setView(parseHash(window.location.hash));
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
