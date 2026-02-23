import type { BuildResponsePayload } from "./workspaceTypes";
import { asRecord, firstNonEmptyString, getPath } from "./workspaceUtils";

type HeaderChipsProps = {
  buildResponse: BuildResponsePayload | null;
};

export default function HeaderChips(props: HeaderChipsProps) {
  const { buildResponse } = props;
  const result = asRecord(buildResponse?.result);

  const chips: Array<{ label: string; value: string }> = [
    {
      label: "engine",
      value: firstNonEmptyString(buildResponse?.engine_version) || "-",
    },
    {
      label: "ruleset",
      value: firstNonEmptyString(buildResponse?.ruleset_version) || "-",
    },
    {
      label: "db snapshot",
      value: firstNonEmptyString(buildResponse?.db_snapshot_id, result?.snapshot_id) || "-",
    },
    {
      label: "profile",
      value: firstNonEmptyString(buildResponse?.profile_id) || "-",
    },
    {
      label: "bracket",
      value: firstNonEmptyString(buildResponse?.bracket_id) || "-",
    },
    {
      label: "ui contract",
      value: firstNonEmptyString(result?.ui_contract_version) || "-",
    },
    {
      label: "graph policy",
      value: firstNonEmptyString(getPath(result, ["pipeline_versions", "graph_bounds_policy_version"])) || "-",
    },
    {
      label: "build hash",
      value: firstNonEmptyString(buildResponse?.build_hash_v1) || "-",
    },
  ];

  return (
    <section className="workspace-panel">
      <details open className="workspace-collapsible">
        <summary>Version Locks</summary>
        <div className="workspace-chip-row">
          {chips.map((chip) => (
            <span key={chip.label} className="workspace-chip">
              {chip.label}: {chip.value}
            </span>
          ))}
        </div>
      </details>
    </section>
  );
}
