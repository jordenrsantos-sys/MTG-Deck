/**
 * LandingView — Phase 4.12b site shell.
 *
 * Welcome page at hash route `#/` (default). 4 entry-point Cards per
 * UI_OVERHAUL_DESIGN sub-task 4.12 split:
 *   - Build a deck    → workspace (#workspace-decks)
 *   - Import existing → #import
 *   - View saved decks → #decks
 *   - Playtest        → #playtest
 *
 * Dark-glass aesthetic; Tailwind responsive grid (1 col phone, 2 col
 * tablet, 4 col desktop) per autonomous_repair_log soft-safety #1.
 * No data fetches; no auth (4.12c gated separately).
 */
import { Card, CardHeader, CardBody } from "../ui/primitives/Card";
import Button from "../ui/primitives/Button";

type EntryPoint = {
  hash: string;
  title: string;
  description: string;
  cta: string;
};

const ENTRY_POINTS: ReadonlyArray<EntryPoint> = [
  {
    hash: "#workspace-decks",
    title: "Build a deck",
    description: "Build or refine a deck. Paste a decklist or import from Archidekt.",
    cta: "Open workspace",
  },
  {
    hash: "#import",
    title: "Import existing",
    description: "Bring in a deck from Archidekt, Arena, MTGO, or plain text.",
    cta: "Open importer",
  },
  {
    hash: "#decks",
    title: "Saved decks",
    description: "Pick up where you left off.",
    cta: "View saved",
  },
  {
    hash: "#playtest",
    title: "Playtest",
    description: "Solo goldfish — draw, play, advance turns. Test the curve.",
    cta: "Start playtest",
  },
];

function _navigate(hash: string): void {
  window.location.hash = hash;
}

export default function LandingView() {
  return (
    <main className="min-h-screen bg-bg-base text-text-primary p-token-3 md:p-panel-pad" aria-label="MTG Engine landing">
      <header className="mb-token-3">
        <h1 className="text-3xl font-semibold">MTG Engine</h1>
        <p className="text-sm text-text-muted mt-token-1">
          Deterministic deck-building engine. Pick where you'd like to start.
        </p>
      </header>
      <section
        className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-token-3"
        aria-label="Entry points"
      >
        {ENTRY_POINTS.map((entry) => (
          <Card key={entry.hash}>
            <CardHeader>{entry.title}</CardHeader>
            <CardBody>
              <p className="text-sm text-text-secondary mb-token-3">{entry.description}</p>
              <Button variant="primary" onClick={() => _navigate(entry.hash)} aria-label={entry.cta}>
                {entry.cta}
              </Button>
            </CardBody>
          </Card>
        ))}
      </section>
    </main>
  );
}
