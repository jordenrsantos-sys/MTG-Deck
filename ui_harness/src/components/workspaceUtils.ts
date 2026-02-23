import type { CardSuggestRow, JsonRecord, ParsedDecklistRow } from "./workspaceTypes";

export const DEFAULT_API_BASE = String(import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000").trim();
const MAX_DECK_COUNT_PER_LINE = 250;
const SQLITE_HEADER_PREFETCH_DB_PATH = "..\\data\\mtg.sqlite";

export function asRecord(value: unknown): JsonRecord | null {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as JsonRecord;
  }
  return null;
}

export function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

export function asOptionalString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const token = value.trim();
  return token === "" ? null : token;
}

export function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const rows: string[] = [];
  for (const raw of value) {
    const token = asOptionalString(raw);
    if (token !== null) {
      rows.push(token);
    }
  }
  return rows;
}

export function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value.trim());
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

export function firstNonEmptyString(...values: unknown[]): string | null {
  for (const value of values) {
    const token = asOptionalString(value);
    if (token !== null) {
      return token;
    }
  }
  return null;
}

export function firstNumber(...values: unknown[]): number | null {
  for (const value of values) {
    const token = asNumber(value);
    if (token !== null) {
      return token;
    }
  }
  return null;
}

export function getPath(root: unknown, path: string[]): unknown {
  let cursor: unknown = root;
  for (const key of path) {
    const row = asRecord(cursor);
    if (!row || !(key in row)) {
      return null;
    }
    cursor = row[key];
  }
  return cursor;
}

export function normalizeApiBase(raw: string): string {
  const token = raw.trim();
  if (token === "") {
    return "http://127.0.0.1:8000";
  }
  return token.endsWith("/") ? token.slice(0, -1) : token;
}

export function safeParseJson(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    return null;
  }
}

export function toSingleLineSnippet(raw: string, maxLength = 180): string {
  const normalized = raw.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength)}...`;
}

export function uniqueSortedStrings(values: string[]): string[] {
  const deduped = new Set<string>();
  for (const value of values) {
    const token = value.trim();
    if (token !== "") {
      deduped.add(token);
    }
  }
  return Array.from(deduped).sort((a: string, b: string) => a.localeCompare(b));
}

export function extractPrimitiveIds(value: unknown): string[] {
  const rows = asArray(value);
  const primitiveIds: string[] = [];

  for (const rawRow of rows) {
    const asText = asOptionalString(rawRow);
    if (asText !== null) {
      primitiveIds.push(asText);
      continue;
    }

    const row = asRecord(rawRow);
    if (!row) {
      continue;
    }

    const primitiveId = firstNonEmptyString(row.primitive_id, row.primitive, row.id, row.tag_id, row.code);
    if (primitiveId !== null) {
      primitiveIds.push(primitiveId);
    }
  }

  return uniqueSortedStrings(primitiveIds);
}

export function normalizeSlotIds(value: unknown): string[] {
  const rows = asArray(value);
  const slotIds: string[] = [];

  for (const rawRow of rows) {
    const asText = asOptionalString(rawRow);
    if (asText !== null) {
      slotIds.push(asText);
      continue;
    }

    const row = asRecord(rawRow);
    if (!row) {
      continue;
    }

    const slotId = firstNonEmptyString(row.slot_id, row.id);
    if (slotId !== null) {
      slotIds.push(slotId);
    }
  }

  return uniqueSortedStrings(slotIds);
}

export function cardNameSortKey(value: string): string {
  return value.trim().toLowerCase();
}

function stripDeckComment(rawLine: string): string {
  const trimmed = rawLine.trim();
  if (trimmed === "") {
    return "";
  }
  if (trimmed.startsWith("#") || trimmed.startsWith("//")) {
    return "";
  }

  const inlineHashIndex = rawLine.search(/\s{2,}#/);
  const inlineSlashIndex = rawLine.search(/\s{2,}\/\//);
  const candidates = [inlineHashIndex, inlineSlashIndex].filter((index: number) => index >= 0);
  if (candidates.length === 0) {
    return trimmed;
  }

  const cutIndex = Math.min(...candidates);
  return rawLine.slice(0, cutIndex).trim();
}

export function parseDecklistInput(rawDecklist: string): ParsedDecklistRow[] {
  const lines = rawDecklist.split(/\r?\n/);
  const rows: ParsedDecklistRow[] = [];

  for (const line of lines) {
    const stripped = stripDeckComment(line);
    if (stripped === "") {
      continue;
    }

    let count = 1;
    let name = stripped;
    const countMatch = stripped.match(/^(\d+)(?:\s*[xX])?\s+(.+)$/);
    if (countMatch) {
      const parsedCount = Number(countMatch[1]);
      const parsedName = countMatch[2].trim();
      if (
        Number.isFinite(parsedCount) &&
        parsedCount >= 1 &&
        parsedCount <= MAX_DECK_COUNT_PER_LINE &&
        parsedName !== ""
      ) {
        count = Math.trunc(parsedCount);
        name = parsedName;
      }
    }

    if (name === "") {
      continue;
    }

    rows.push({
      name,
      count,
      source_order: rows.length,
      original_line: line,
    });
  }

  return rows;
}

export function expandDecklistRowsInInputOrder(rows: ParsedDecklistRow[]): string[] {
  const cards: string[] = [];
  for (const row of rows) {
    const safeCount = Number.isFinite(row.count) ? Math.max(1, Math.trunc(row.count)) : 1;
    for (let i = 0; i < safeCount; i += 1) {
      cards.push(row.name);
    }
  }
  return cards;
}

export function buildNormalizedDeckPreviewLines(cardsInPayloadOrder: string[]): string[] {
  const counters = new Map<string, { name: string; count: number; firstIndex: number }>();

  cardsInPayloadOrder.forEach((name: string, index: number) => {
    const key = cardNameSortKey(name);
    const existing = counters.get(key);
    if (!existing) {
      counters.set(key, { name, count: 1, firstIndex: index });
      return;
    }
    existing.count += 1;
    if (index < existing.firstIndex) {
      existing.firstIndex = index;
      existing.name = name;
    }
  });

  return Array.from(counters.values())
    .sort((a, b) => {
      const byName = cardNameSortKey(a.name).localeCompare(cardNameSortKey(b.name));
      if (byName !== 0) {
        return byName;
      }
      return a.firstIndex - b.firstIndex;
    })
    .map((row) => `${row.count} ${row.name}`);
}

export function clampSuggestLimit(value: number): number {
  if (!Number.isFinite(value)) {
    return 20;
  }
  const rounded = Math.trunc(value);
  return Math.min(Math.max(rounded, 1), 20);
}

export function parseCardSuggestRows(payload: unknown): CardSuggestRow[] {
  const root = asRecord(payload);
  const rawRows = asArray(root?.results);
  const rows: CardSuggestRow[] = [];

  for (const rawRow of rawRows) {
    const row = asRecord(rawRow);
    if (!row) {
      continue;
    }
    const name = asOptionalString(row.name);
    if (name === null) {
      continue;
    }

    rows.push({
      oracle_id: asOptionalString(row.oracle_id) || "",
      name,
      mana_cost: asOptionalString(row.mana_cost),
      type_line: asOptionalString(row.type_line),
      image_uri: asOptionalString(row.image_uri),
    });

    if (rows.length >= 20) {
      break;
    }
  }

  return rows;
}

export function extractLatestSnapshotId(payload: unknown): string {
  const root = asRecord(payload);
  const snapshots = asArray(root?.snapshots);

  for (const raw of snapshots) {
    const asText = asOptionalString(raw);
    if (asText !== null) {
      return asText;
    }

    const row = asRecord(raw);
    if (!row) {
      continue;
    }

    const snapshotId = firstNonEmptyString(row.snapshot_id, row.id, row.db_snapshot_id);
    if (snapshotId !== null) {
      return snapshotId;
    }
  }

  return "";
}

export async function fetchLatestSnapshotIdFromApi(apiBase: string): Promise<string> {
  const response = await fetch(`${normalizeApiBase(apiBase)}/snapshots?limit=1`, {
    method: "GET",
  });
  const text = await response.text();
  const parsed = safeParseJson(text);

  if (!response.ok) {
    throw new Error(`HTTP ${response.status} from /snapshots | response=${toSingleLineSnippet(text) || "(empty)"}`);
  }

  const snapshotId = extractLatestSnapshotId(parsed);
  if (snapshotId === "") {
    throw new Error("No snapshots returned from /snapshots.");
  }

  return snapshotId;
}

export function buildLocalCardImageUrl(
  apiBaseUrl: string,
  oracleId: string,
  size: "normal" | "small" | "large" | "png" | "art_crop" | "border_crop" = "normal",
): string {
  return `${normalizeApiBase(apiBaseUrl)}/cards/image/${encodeURIComponent(oracleId)}?size=${encodeURIComponent(size)}`;
}

export function buildPrefetchCardImagesCommand(snapshotId: string): string {
  return [
    "python -m snapshot_build.prefetch_card_images",
    `--db ${SQLITE_HEADER_PREFETCH_DB_PATH}`,
    `--snapshot_id ${snapshotId || "<snapshot_id>"}`,
    "--out .\\data\\card_images",
    "--sizes normal,small",
    "--workers 4",
    "--resume",
    "--progress 100",
  ].join(" ");
}

export async function copyTextToClipboard(text: string): Promise<void> {
  if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
    await navigator.clipboard.writeText(text);
    return;
  }

  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand("copy");
  document.body.removeChild(textarea);
  if (!copied) {
    throw new Error("Clipboard write failed.");
  }
}

export function toPrettyJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return "{}";
  }
}
