import { useEffect, useMemo, useState } from "react";

import GlassPanel from "../../ui/primitives/GlassPanel";
import Button from "../../ui/primitives/Button";
import { Dialog, DialogContent, DialogTitle } from "../../ui/primitives/Dialog";

type RailLink = {
  label: string;
  href: string;
  icon: string;
  view: "workspace" | "diagnostics" | "playtest" | "settings" | "landing";
};

const links: RailLink[] = [
  { label: "Home", href: "#/", icon: "HM", view: "landing" },
  { label: "Decks", href: "#workspace-decks", icon: "DK", view: "workspace" },
  { label: "Runs", href: "#workspace-runs", icon: "RN", view: "workspace" },
  { label: "Playtest", href: "#playtest", icon: "PT", view: "playtest" },
  { label: "Diagnostics", href: "#diagnostics", icon: "DG", view: "diagnostics" },
  { label: "Settings", href: "#settings", icon: "ST", view: "settings" },
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
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  useEffect(() => {
    function onHashChange(): void {
      setHashValue(normalizeHash(window.location.hash));
      // Close the mobile menu after navigating so the user lands on the new view.
      setMobileMenuOpen(false);
    }

    window.addEventListener("hashchange", onHashChange);
    return () => {
      window.removeEventListener("hashchange", onHashChange);
    };
  }, []);

  const diagnosticsActive = hashValue === "#diagnostics";
  const playtestActive = hashValue === "#playtest";
  const settingsActive = hashValue === "#settings";
  const landingActive = hashValue === "#/" || hashValue === "";
  const activeWorkspaceHash = resolveActiveWorkspaceHash(hashValue);

  const navItems = useMemo(
    () =>
      links.map((link: RailLink) => {
        let isActive = false;
        if (link.view === "diagnostics") isActive = diagnosticsActive;
        else if (link.view === "playtest") isActive = playtestActive;
        else if (link.view === "settings") isActive = settingsActive;
        else if (link.view === "landing") isActive = landingActive;
        else
          isActive =
            !diagnosticsActive &&
            !playtestActive &&
            !settingsActive &&
            !landingActive &&
            link.href === activeWorkspaceHash;
        const classes = ["workspace-left-rail-link", isActive ? "is-active" : ""].filter(Boolean).join(" ");
        return {
          ...link,
          isActive,
          classes,
        };
      }),
    [activeWorkspaceHash, diagnosticsActive, playtestActive, settingsActive, landingActive],
  );

  const navList = (
    <nav className="workspace-left-rail-nav" aria-label="Primary navigation">
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
  );

  return (
    <>
      {/* Phase 4.11 mobile pass: hide the rail at <md (768px) and surface a
          hamburger Button + Dialog instead. autonomous_repair_log #2: hamburger
          Dialog flavor (preserves all nav entries; click-anywhere-outside
          closes via existing Dialog backdrop). */}
      <aside className="workspace-left-rail hidden md:flex" aria-label="Workspace navigation">
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
          {navList}
        </GlassPanel>
      </aside>

      <div className="md:hidden fixed top-2 left-2 z-modal">
        <Button
          variant="secondary"
          size="md"
          onClick={() => setMobileMenuOpen(true)}
          aria-label="Open navigation menu"
        >
          ☰ Menu
        </Button>
      </div>

      <Dialog open={mobileMenuOpen} onOpenChange={setMobileMenuOpen}>
        <DialogContent className="max-w-xs">
          <DialogTitle>Navigation</DialogTitle>
          {navList}
        </DialogContent>
      </Dialog>
    </>
  );
}
