import type { BuildResponsePayload } from "./workspaceTypes";
import { asRecord, firstNonEmptyString, firstNumber, getPath } from "./workspaceUtils";
import Chip from "../ui/primitives/Chip";

type HeaderChipsApiPingSummary = {
  status: "PENDING" | "OK" | "ERROR";
  endpoint: "/health" | "/version" | "-";
  engineVersion: string;
  dbSnapshotId: string;
  rulesetVersion: string;
  bracketDefinitionVersion: string;
};

type HeaderChipsProps = {
  buildResponse: BuildResponsePayload | null;
  apiBase: string;
  uiMode: "DEV" | "PROD";
  uiCommit: string;
  apiPingSummary?: HeaderChipsApiPingSummary;
  loading?: boolean;
  compact?: boolean;
  className?: string;
};

export default function HeaderChips(props: HeaderChipsProps) {
  const {
    buildResponse,
    apiBase,
    uiMode,
    uiCommit,
    apiPingSummary = {
      status: "PENDING",
      endpoint: "-",
      engineVersion: "-",
      dbSnapshotId: "-",
      rulesetVersion: "-",
      bracketDefinitionVersion: "-",
    },
    loading = false,
    compact = false,
    className,
  } = props;
  const result = asRecord(buildResponse?.result);
  const classes = ["workspace-panel-content", className].filter(Boolean).join(" ");
  const snapshotId = firstNonEmptyString(buildResponse?.db_snapshot_id, result?.snapshot_id) || "-";
  const profileId = firstNonEmptyString(buildResponse?.profile_id) || "-";
  const bracketId = firstNonEmptyString(buildResponse?.bracket_id) || "-";
  const status = loading ? "RUNNING" : firstNonEmptyString(buildResponse?.status) || "-";
  const deckStatus = firstNonEmptyString(buildResponse?.deck_status) || "-";
  const deckSize = firstNumber(buildResponse?.deck_size_total);
  const cardsNeeded = firstNumber(buildResponse?.cards_needed);
  const cardsToCut = firstNumber(buildResponse?.cards_to_cut);

  const compactChips: Array<{ label: string; value: string; className?: string }> = [
    {
      label: "snapshot",
      value: snapshotId,
    },
    {
      label: "profile",
      value: profileId,
    },
    {
      label: "bracket",
      value: bracketId,
    },
    {
      label: "status",
      value: status,
      className: "workspace-chip-info",
    },
    {
      label: "ui_mode",
      value: uiMode,
      className: "workspace-chip-info",
    },
    {
      label: "api_ping",
      value: apiPingSummary.status,
      className: apiPingSummary.status === "OK" ? "workspace-chip-info" : undefined,
    },
  ];

  const metadataRows: Array<{ key: string; label: string; value: string }> = [
    {
      key: "ui_mode",
      label: "ui_mode",
      value: uiMode,
    },
    {
      key: "ui_commit",
      label: "ui_commit",
      value: uiCommit || "-",
    },
    {
      key: "api_base",
      label: "api_base",
      value: apiBase || "-",
    },
    {
      key: "api_ping_status",
      label: "api_ping_status",
      value: apiPingSummary.status,
    },
    {
      key: "api_ping_endpoint",
      label: "api_ping_endpoint",
      value: apiPingSummary.endpoint,
    },
    {
      key: "api_ping_engine_version",
      label: "api_ping_engine_version",
      value: apiPingSummary.engineVersion || "-",
    },
    {
      key: "api_ping_db_snapshot_id",
      label: "api_ping_db_snapshot_id",
      value: apiPingSummary.dbSnapshotId || "-",
    },
    {
      key: "api_ping_ruleset_version",
      label: "api_ping_ruleset_version",
      value: apiPingSummary.rulesetVersion || "-",
    },
    {
      key: "api_ping_bracket_definition_version",
      label: "api_ping_bracket_definition_version",
      value: apiPingSummary.bracketDefinitionVersion || "-",
    },
    {
      key: "engine_version",
      label: "engine_version",
      value: firstNonEmptyString(buildResponse?.engine_version) || "-",
    },
    {
      key: "ruleset_version",
      label: "ruleset_version",
      value: firstNonEmptyString(buildResponse?.ruleset_version) || "-",
    },
    {
      key: "bracket_definition_version",
      label: "bracket_definition_version",
      value:
        firstNonEmptyString(
          buildResponse?.bracket_definition_version,
          getPath(result, ["pipeline_versions", "bracket_definition_version"]),
        ) || "-",
    },
    {
      key: "db_snapshot_id",
      label: "db_snapshot_id",
      value: snapshotId,
    },
    {
      key: "profile_id",
      label: "profile_id",
      value: profileId,
    },
    {
      key: "bracket_id",
      label: "bracket_id",
      value: bracketId,
    },
    {
      key: "status",
      label: "status",
      value: status,
    },
    {
      key: "deck_status",
      label: "deck_status",
      value: deckStatus,
    },
    {
      key: "deck_size_total",
      label: "deck_size_total",
      value: deckSize === null ? "-" : String(deckSize),
    },
    {
      key: "cards_needed",
      label: "cards_needed",
      value: cardsNeeded === null ? "-" : String(cardsNeeded),
    },
    {
      key: "cards_to_cut",
      label: "cards_to_cut",
      value: cardsToCut === null ? "-" : String(cardsToCut),
    },
    {
      key: "ui_contract_version",
      label: "ui_contract_version",
      value: firstNonEmptyString(result?.ui_contract_version) || "-",
    },
    {
      key: "graph_bounds_policy_version",
      label: "graph_bounds_policy_version",
      value: firstNonEmptyString(getPath(result, ["pipeline_versions", "graph_bounds_policy_version"])) || "-",
    },
    {
      key: "build_hash",
      label: "build_hash",
      value: firstNonEmptyString(buildResponse?.build_hash_v1, buildResponse?.build_hash) || "-",
    },
  ];

  if (compact) {
    return (
      <section className={classes}>
        <div className="workspace-compact-header-row">
          <div className="workspace-chip-row workspace-compact-header-chips">
            {compactChips.map((chip) => (
              <Chip key={chip.label} className={chip.className}>
                {chip.label}: {chip.value}
              </Chip>
            ))}
          </div>

          <details className="workspace-header-meta-popover">
            <summary aria-label="Show build metadata" title="Show build metadata">
              i
            </summary>

            <div className="workspace-header-meta-popover-card" role="dialog" aria-label="Build metadata">
              <p className="workspace-topbar-title">Build metadata</p>
              <dl className="workspace-header-meta-list">
                {metadataRows.map((row) => (
                  <div key={row.key} className="workspace-header-meta-list-row">
                    <dt>{row.label}</dt>
                    <dd>{row.value}</dd>
                  </div>
                ))}
              </dl>
            </div>
          </details>
        </div>
      </section>
    );
  }

  return (
    <section className={classes}>
      <details open className="workspace-collapsible">
        <summary>Build metadata</summary>

        <div className="workspace-chip-row workspace-compact-header-chips">
          {compactChips.map((chip) => (
            <Chip key={chip.label} className={chip.className}>
              {chip.label}: {chip.value}
            </Chip>
          ))}
        </div>

        <dl className="workspace-header-meta-list workspace-header-meta-list-expanded">
          {metadataRows.map((row) => (
            <div key={row.key} className="workspace-header-meta-list-row">
              <dt>{row.label}</dt>
              <dd>{row.value}</dd>
            </div>
          ))}
        </dl>
      </details>
    </section>
  );
}
