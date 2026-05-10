/**
 * SettingsView — Phase 4.12b site shell.
 *
 * Local-only settings (no server sync; OQ-1=(c) local-first):
 *   - darkMode: boolean — toggles `dark` class on document.documentElement
 *     per Tailwind 4.2 setup (`darkMode: 'class'`); persists under
 *     `mtgdb:settings:darkMode` via storage adapter.
 *   - bracketDefault: B1 / B2 / B3 / B4 / B5 — preferred bracket; consumed
 *     by future workspace defaulters.
 *   - profileDefault: balanced (autonomous_repair_log soft-safety #12 —
 *     hardcoded enum until BuildResponse exposes profiles).
 *
 * All values persist via `getStorageAdapter()` under `mtgdb:settings:*`.
 * No auth; no server endpoints.
 */
import { useEffect, useState } from "react";
import Button from "../ui/primitives/Button";
import Select from "../ui/primitives/Select";
import { Card, CardHeader, CardBody } from "../ui/primitives/Card";
import { getStorageAdapter } from "../lib/storage";

const SETTINGS_PREFIX = "mtgdb:settings:";

const BRACKETS = ["B1", "B2", "B3", "B4", "B5"] as const;
// Per autonomous_repair_log soft-safety #12: BuildResponse doesn't yet expose
// the canonical profile list; ship "balanced" as the only v1.2 option and
// revisit when the engine adds a profile-enumeration field.
const PROFILES = ["balanced"] as const;

type Settings = {
  darkMode: boolean;
  bracketDefault: typeof BRACKETS[number];
  profileDefault: typeof PROFILES[number];
};

const DEFAULTS: Settings = {
  darkMode: true,
  bracketDefault: "B2",
  profileDefault: "balanced",
};

function _key(name: keyof Settings): string {
  return `${SETTINGS_PREFIX}${name}`;
}

function _readSetting<K extends keyof Settings>(name: K): Settings[K] {
  const v = getStorageAdapter().load<Settings[K]>(_key(name));
  return v === null ? DEFAULTS[name] : v;
}

function _writeSetting<K extends keyof Settings>(name: K, value: Settings[K]): void {
  getStorageAdapter().save(_key(name), value);
}

function _applyDarkMode(enabled: boolean): void {
  if (typeof document === "undefined") return;
  const root = document.documentElement;
  if (enabled) root.classList.add("dark");
  else root.classList.remove("dark");
}

export type SettingsViewProps = {
  onBack?: () => void;
};

export default function SettingsView({ onBack }: SettingsViewProps) {
  const [darkMode, setDarkMode] = useState<boolean>(() => _readSetting("darkMode"));
  const [bracket, setBracket] = useState<Settings["bracketDefault"]>(() => _readSetting("bracketDefault"));
  const [profile, setProfile] = useState<Settings["profileDefault"]>(() => _readSetting("profileDefault"));

  // Apply darkMode on mount and whenever it changes.
  useEffect(() => {
    _applyDarkMode(darkMode);
  }, [darkMode]);

  function handleDarkModeToggle() {
    const next = !darkMode;
    setDarkMode(next);
    _writeSetting("darkMode", next);
  }
  function handleBracketChange(v: string) {
    const safe = (BRACKETS as ReadonlyArray<string>).includes(v) ? (v as Settings["bracketDefault"]) : "B2";
    setBracket(safe);
    _writeSetting("bracketDefault", safe);
  }
  function handleProfileChange(v: string) {
    const safe = (PROFILES as ReadonlyArray<string>).includes(v) ? (v as Settings["profileDefault"]) : "balanced";
    setProfile(safe);
    _writeSetting("profileDefault", safe);
  }

  return (
    <main className="min-h-screen bg-bg-base text-text-primary p-token-3 md:p-panel-pad" aria-label="Settings">
      <header className="flex items-center justify-between mb-token-3">
        <h1 className="text-2xl font-semibold">Settings</h1>
        {onBack ? (
          <Button variant="ghost" onClick={onBack} aria-label="Back">
            ← Back
          </Button>
        ) : null}
      </header>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-token-3 max-w-3xl">
        <Card>
          <CardHeader>Appearance</CardHeader>
          <CardBody>
            <p className="text-sm text-text-secondary mb-token-2">
              Toggle the app's dark theme.
            </p>
            <Button
              variant={darkMode ? "primary" : "secondary"}
              onClick={handleDarkModeToggle}
              aria-pressed={darkMode}
              aria-label="Toggle dark mode"
            >
              Dark mode: {darkMode ? "ON" : "OFF"}
            </Button>
          </CardBody>
        </Card>

        <Card>
          <CardHeader>Defaults</CardHeader>
          <CardBody>
            <label className="flex flex-col gap-token-1 mb-token-2">
              <span className="text-xs text-text-muted">Default bracket</span>
              <Select
                value={bracket}
                onChange={(e) => handleBracketChange(e.target.value)}
                aria-label="Default bracket"
              >
                {BRACKETS.map((b) => (
                  <option key={b} value={b}>
                    {b}
                  </option>
                ))}
              </Select>
            </label>
            <label className="flex flex-col gap-token-1">
              <span className="text-xs text-text-muted">Default profile</span>
              <Select
                value={profile}
                onChange={(e) => handleProfileChange(e.target.value)}
                aria-label="Default profile"
              >
                {PROFILES.map((p) => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </Select>
              <span className="text-xs text-text-muted mt-token-1">
                Default deck-build profile.
              </span>
            </label>
          </CardBody>
        </Card>
      </div>
    </main>
  );
}

/**
 * Test-friendly accessor — exported for vitest helper coverage of the
 * read/write/applyDarkMode logic without rendering the React component.
 */
export const _settingsHelpers = {
  readSetting: _readSetting,
  writeSetting: _writeSetting,
  applyDarkMode: _applyDarkMode,
  DEFAULTS,
  BRACKETS,
  PROFILES,
};
