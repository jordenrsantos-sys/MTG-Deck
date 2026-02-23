import { useEffect, useState } from "react";

import DiagnosticsView from "./DiagnosticsView";
import WorkspaceView from "./WorkspaceView";

type ViewId = "workspace" | "diagnostics";

function parseHash(hashValue: string): ViewId {
  const token = hashValue.replace(/^#/, "").trim().toLowerCase();
  return token === "diagnostics" ? "diagnostics" : "workspace";
}

export default function AppRouter() {
  const [view, setView] = useState<ViewId>(() => parseHash(window.location.hash));

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
    window.location.hash = "";
    setView("workspace");
  }

  if (view === "diagnostics") {
    return <DiagnosticsView onBack={openWorkspace} />;
  }

  return <WorkspaceView />;
}
