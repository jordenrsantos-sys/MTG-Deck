import { useEffect, useMemo, useState } from "react";

import GlassPanel from "../../ui/primitives/GlassPanel";

type RailLink = {
  label: string;
  href: string;
  icon: string;
  view: "workspace" | "diagnostics";
};

const links: RailLink[] = [
  { label: "Decks", href: "#workspace-decks", icon: "DK", view: "workspace" },
  { label: "Runs", href: "#workspace-runs", icon: "RN", view: "workspace" },
  { label: "Diagnostics", href: "#diagnostics", icon: "DG", view: "diagnostics" },
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

export default function LeftRail() {
  const [hashValue, setHashValue] = useState(() => normalizeHash(window.location.hash));

  useEffect(() => {
    function onHashChange(): void {
      setHashValue(normalizeHash(window.location.hash));
    }

    window.addEventListener("hashchange", onHashChange);
    return () => {
      window.removeEventListener("hashchange", onHashChange);
    };
  }, []);

  const diagnosticsActive = hashValue === "#diagnostics";
  const activeWorkspaceHash = resolveActiveWorkspaceHash(hashValue);

  const navItems = useMemo(
    () =>
      links.map((link: RailLink) => {
        const isActive =
          link.view === "diagnostics"
            ? diagnosticsActive
            : !diagnosticsActive && link.href === activeWorkspaceHash;
        const classes = ["workspace-left-rail-link", isActive ? "is-active" : ""].filter(Boolean).join(" ");
        return {
          ...link,
          isActive,
          classes,
        };
      }),
    [activeWorkspaceHash, diagnosticsActive],
  );

  return (
    <aside className="workspace-left-rail" aria-label="Workspace navigation">
      <GlassPanel className="workspace-left-rail-panel">
        <div className="workspace-left-rail-brand" title="MTG Engine">
          <span className="workspace-left-rail-brand-mark" aria-hidden="true">
            ME
          </span>
          <div className="workspace-left-rail-brand-copy">
            <p className="workspace-left-rail-brand-name">MTG Engine</p>
            <span className="workspace-left-rail-brand-chip">vNext</span>
          </div>
        </div>

        <p className="workspace-left-rail-title">Workspace</p>
        <nav className="workspace-left-rail-nav">
          {navItems.map((link) => (
            <a
              key={link.label}
              href={link.href}
              title={link.label}
              className={link.classes}
              aria-current={link.isActive ? "page" : undefined}
            >
              <span className="workspace-left-rail-icon" aria-hidden="true">
                {link.icon}
              </span>
              <span className="workspace-left-rail-text">{link.label}</span>
            </a>
          ))}
        </nav>
      </GlassPanel>
    </aside>
  );
}
