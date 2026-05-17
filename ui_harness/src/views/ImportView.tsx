/**
 * ImportView — Phase 4.3 deck-import flow.
 *
 * Surfaces 5 ingest paths via Tabs (Phase 4.2 primitive):
 *   - URL Import (Archidekt ACTIVE; Moxfield + EDHREC DEFERRED with
 *     "not available — file paste only" affordance per Engine-4A registry)
 *   - Arena text
 *   - MTGO text
 *   - Plain text
 *   - File upload
 *
 * Hand-off to existing `DeckInputPanel` is via the `onImportComplete`
 * callback — a thin adapter prop carrying `{commander, decklist, unknowns}`.
 * The parent (whoever embeds ImportView) wires that into DeckInputPanel's
 * existing onCommanderChange + onCardsInputChange callbacks. Per spec:
 * DO NOT rewrite DeckInputPanel.
 *
 * Honors UI_OVERHAUL_DESIGN Decision 5 (importer breadth via parser registry)
 * + Decision 8 (frozen contract preservation — UI reads only existing fields).
 */
import { useCallback, useState } from "react";
import type { ChangeEvent } from "react";
import Button from "../ui/primitives/Button";
import Input from "../ui/primitives/Input";
import Badge from "../ui/primitives/Badge";
import { Card, CardHeader, CardBody, CardFooter } from "../ui/primitives/Card";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "../ui/primitives/Tabs";
import { importFromUrl } from "../parsers/archidektUrl";
import { parseArenaText } from "../parsers/arenaText";
import { parseMtgoText } from "../parsers/mtgoText";
import { parsePlainText } from "../parsers/plainText";
import { parseFile } from "../parsers/fileUpload";
import type { ImportResult } from "../parsers/types";

export type ImportCompletePayload = {
  commander: string;
  commander_oracle_id: string | null;
  decklist: string;
  unknowns: ReadonlyArray<{ name: string; reason: string }>;
  source: string;
  raw_result: ImportResult;
};

export type ImportViewProps = {
  onImportComplete: (payload: ImportCompletePayload) => void;
  className?: string;
};

type TabKey = "url" | "arena" | "mtgo" | "plain" | "file";

const DEFERRED_MOXFIELD_NOTE =
  "Moxfield URL imports are not available (Cloudflare-gated). Paste the decklist as text instead.";
const DEFERRED_EDHREC_NOTE =
  "EDHREC URL imports are not available yet (reachability unverified). Paste the decklist as text instead.";

function _resultToCompletePayload(result: ImportResult): ImportCompletePayload {
  const commander = result.deck?.commander?.name ?? "";
  const commanderOracleId = result.deck?.commander?.oracle_id ?? null;
  const cards = result.deck?.cards ?? [];
  const lines: string[] = [];
  if (result.deck?.commander && cards.length > 0) {
    // Commander already conveyed via the commander field; main deck only.
  }
  for (const c of cards) {
    lines.push(`${c.count} ${c.name}`);
  }
  return {
    commander,
    commander_oracle_id: commanderOracleId,
    decklist: lines.join("\n"),
    unknowns: result.unknowns,
    source: result.source,
    raw_result: result,
  };
}

export default function ImportView(props: ImportViewProps) {
  const { onImportComplete, className } = props;
  const [tab, setTab] = useState<TabKey>("url");
  const [url, setUrl] = useState("");
  const [textInput, setTextInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [lastResult, setLastResult] = useState<ImportResult | null>(null);

  const handleUrlImport = useCallback(async () => {
    setBusy(true);
    try {
      const result = await importFromUrl(url);
      setLastResult(result);
      if (result.status === "OK") onImportComplete(_resultToCompletePayload(result));
    } finally {
      setBusy(false);
    }
  }, [url, onImportComplete]);

  const handleTextImport = useCallback(
    (which: TabKey) => {
      const result =
        which === "arena"
          ? parseArenaText(textInput)
          : which === "mtgo"
          ? parseMtgoText(textInput)
          : parsePlainText(textInput);
      setLastResult(result);
      if (result.status === "OK") onImportComplete(_resultToCompletePayload(result));
    },
    [textInput, onImportComplete],
  );

  const handleFileImport = useCallback(
    async (event: ChangeEvent<HTMLInputElement>) => {
      const file = event.target.files?.[0];
      if (!file) return;
      setBusy(true);
      try {
        const result = await parseFile(file);
        setLastResult(result);
        if (result.status === "OK") onImportComplete(_resultToCompletePayload(result));
      } finally {
        setBusy(false);
      }
    },
    [onImportComplete],
  );

  return (
    <section className={["flex flex-col gap-token-3", className].filter(Boolean).join(" ")}>
      <Card>
        <CardHeader>Import Deck</CardHeader>
        <CardBody>
          <Tabs value={tab} onValueChange={(v) => setTab(v as TabKey)}>
            <TabsList>
              <TabsTrigger value="url">URL</TabsTrigger>
              <TabsTrigger value="arena">Arena</TabsTrigger>
              <TabsTrigger value="mtgo">MTGO</TabsTrigger>
              <TabsTrigger value="plain">Plain text</TabsTrigger>
              <TabsTrigger value="file">File</TabsTrigger>
            </TabsList>

            <TabsContent value="url" className="pt-token-3">
              <p className="text-sm text-text-secondary mb-token-2">
                Archidekt URL ✅ &nbsp;|&nbsp; Moxfield <Badge variant="warn">DEFERRED</Badge>
                &nbsp;|&nbsp; EDHREC <Badge variant="warn">DEFERRED</Badge>
              </p>
              <p className="text-xs text-text-muted mb-token-2">{DEFERRED_MOXFIELD_NOTE}</p>
              <p className="text-xs text-text-muted mb-token-3">{DEFERRED_EDHREC_NOTE}</p>
              <div className="flex gap-token-2">
                <Input
                  type="url"
                  placeholder="https://archidekt.com/decks/8000000/"
                  value={url}
                  onChange={(e) => setUrl(e.target.value)}
                />
                <Button onClick={handleUrlImport} disabled={busy || !url.trim()}>
                  {busy ? "Importing…" : "Import"}
                </Button>
              </div>
            </TabsContent>

            <TabsContent value="arena" className="pt-token-3">
              <p className="text-xs text-text-muted mb-token-2">
                Paste an Arena export (Commander → Deck → Sideboard sections; <code>(SET) 123</code> tail OK).
              </p>
              <textarea
                rows={10}
                className="w-full bg-glass-bg border border-glass-border rounded-token-md px-token-3 py-token-2 text-text-primary"
                value={textInput}
                onChange={(e) => setTextInput(e.target.value)}
              />
              <div className="flex justify-end mt-token-2">
                <Button onClick={() => handleTextImport("arena")} disabled={!textInput.trim()}>
                  Parse Arena
                </Button>
              </div>
            </TabsContent>

            <TabsContent value="mtgo" className="pt-token-3">
              <p className="text-xs text-text-muted mb-token-2">
                Paste an MTGO export (blank line separates main from sideboard).
              </p>
              <textarea
                rows={10}
                className="w-full bg-glass-bg border border-glass-border rounded-token-md px-token-3 py-token-2 text-text-primary"
                value={textInput}
                onChange={(e) => setTextInput(e.target.value)}
              />
              <div className="flex justify-end mt-token-2">
                <Button onClick={() => handleTextImport("mtgo")} disabled={!textInput.trim()}>
                  Parse MTGO
                </Button>
              </div>
            </TabsContent>

            <TabsContent value="plain" className="pt-token-3">
              <p className="text-xs text-text-muted mb-token-2">
                Plain text: one card per line. Optional <code>Commander</code>/
                <code>Sideboard</code> headers; <code># //</code> for comments.
              </p>
              <textarea
                rows={10}
                className="w-full bg-glass-bg border border-glass-border rounded-token-md px-token-3 py-token-2 text-text-primary"
                value={textInput}
                onChange={(e) => setTextInput(e.target.value)}
              />
              <div className="flex justify-end mt-token-2">
                <Button onClick={() => handleTextImport("plain")} disabled={!textInput.trim()}>
                  Parse plain text
                </Button>
              </div>
            </TabsContent>

            <TabsContent value="file" className="pt-token-3">
              <p className="text-xs text-text-muted mb-token-2">
                Upload a <code>.txt</code>, <code>.dec</code>, or <code>.json</code> file.
                Format auto-detected by extension + content.
              </p>
              <input
                type="file"
                accept=".txt,.dec,.json"
                onChange={handleFileImport}
                className="text-text-primary"
                disabled={busy}
              />
            </TabsContent>
          </Tabs>
        </CardBody>
        {lastResult ? (
          <CardFooter>
            <ImportResultSummary result={lastResult} />
          </CardFooter>
        ) : null}
      </Card>
    </section>
  );
}

type ImportResultSummaryProps = {
  result: ImportResult;
};

// v1.6.5 — humanize internal status codes for the import summary so users
// don't see "DEFERRED" / "INGEST_FAILED_*" / reason-code-shaped jargon.
function _humanizeStatus(status: string): string {
  if (status === "OK") return "Imported";
  if (status === "DEFERRED") return "Needs review";
  if (status === "FAILED" || status?.startsWith("ERROR")) return "Failed";
  return status;
}

function _humanizeReasonCode(code: string): string {
  // Pattern: UPPER_SNAKE → "Upper snake"
  return code
    .toLowerCase()
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function ImportResultSummary(props: ImportResultSummaryProps) {
  const { result } = props;
  const variant =
    result.status === "OK"
      ? "success"
      : result.status === "DEFERRED"
      ? "warn"
      : "error";
  const cardCount = result.deck?.cards.length ?? 0;
  const unknownCount = result.unknowns.length;
  return (
    <div className="flex items-center gap-token-2">
      <Badge variant={variant}>{_humanizeStatus(result.status)}</Badge>
      <span className="text-xs text-text-muted">via {result.source}</span>
      {result.status === "OK" ? (
        <>
          <span className="text-xs text-text-secondary">{cardCount} cards</span>
          {unknownCount > 0 ? (
            <Badge variant="warn">{unknownCount} unresolved</Badge>
          ) : null}
        </>
      ) : null}
      {result.deferred_reason ? (
        <span className="text-xs text-text-muted">{result.deferred_reason}</span>
      ) : null}
      {result.reason_code ? (
        <span className="text-xs text-text-muted">{_humanizeReasonCode(result.reason_code)}</span>
      ) : null}
    </div>
  );
}
