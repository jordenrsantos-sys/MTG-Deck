/**
 * ImportRoute — Phase 4.6.
 *
 * Top-level hash route hosting `<ImportView>` and bridging its
 * `onImportComplete` payload to DeckInputPanel via a localStorage staging
 * slot under `mtgdb:workspace:import:staged_v1`. WorkspaceView reads that
 * slot on mount + when the hash route returns to "" so the imported
 * commander + decklist populate the existing inputs without rewriting
 * DeckInputPanel (4.3 adapter contract preserved).
 *
 * Per autonomous_repair_log soft-safety #5 (AppRouter friction → Dialog
 * modal fallback): this is the top-level route flavor; if WorkspaceView's
 * existing structure resists hash routing, the same component can be
 * rendered inside a Dialog primitive without API changes.
 */
import ImportView, { type ImportCompletePayload } from "./ImportView";
import Button from "../ui/primitives/Button";

export type ImportRouteProps = {
  onBack: () => void;
};

export const IMPORT_STAGED_KEY = "mtgdb:workspace:import:staged_v1";

export type StagedImport = {
  commander: string;
  commander_oracle_id: string | null;
  decklist: string;
  unknowns: ReadonlyArray<{ name: string; reason: string }>;
  source: string;
  staged_at_iso: string;
};

export function readStagedImport(): StagedImport | null {
  try {
    if (typeof window === "undefined" || !window.localStorage) return null;
    const raw = window.localStorage.getItem(IMPORT_STAGED_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as StagedImport;
    if (typeof parsed !== "object" || parsed === null) return null;
    return parsed;
  } catch {
    return null;
  }
}

export function clearStagedImport(): void {
  try {
    if (typeof window === "undefined" || !window.localStorage) return;
    window.localStorage.removeItem(IMPORT_STAGED_KEY);
  } catch {
    /* noop */
  }
}

function _writeStaged(payload: ImportCompletePayload): void {
  try {
    if (typeof window === "undefined" || !window.localStorage) return;
    const staged: StagedImport = {
      commander: payload.commander,
      commander_oracle_id: payload.commander_oracle_id,
      decklist: payload.decklist,
      unknowns: payload.unknowns,
      source: payload.source,
      staged_at_iso: new Date().toISOString(),
    };
    window.localStorage.setItem(IMPORT_STAGED_KEY, JSON.stringify(staged));
  } catch {
    /* noop */
  }
}

export default function ImportRoute(props: ImportRouteProps) {
  const { onBack } = props;
  function handleImportComplete(payload: ImportCompletePayload) {
    _writeStaged(payload);
    onBack();
  }
  return (
    <main className="min-h-screen bg-bg-base text-text-primary p-panel-pad">
      <div className="flex items-center justify-between mb-token-3">
        <h1 className="text-2xl font-semibold">Import deck</h1>
        <Button variant="ghost" onClick={onBack} aria-label="Back to workspace">
          ← Back
        </Button>
      </div>
      <ImportView onImportComplete={handleImportComplete} />
    </main>
  );
}
