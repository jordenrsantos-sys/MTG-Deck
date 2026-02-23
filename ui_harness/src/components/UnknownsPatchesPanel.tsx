import { useState } from "react";

import type { BuildRequestPayload, BuildResponsePayload } from "./workspaceTypes";
import {
  asArray,
  asRecord,
  asStringArray,
  copyTextToClipboard,
  firstNonEmptyString,
  resolveUnknownSignal,
  toPrettyJson,
} from "./workspaceUtils";

type UnknownsPatchesPanelProps = {
  buildResponse: BuildResponsePayload | null;
  requestPayload: BuildRequestPayload | null;
};

export default function UnknownsPatchesPanel(props: UnknownsPatchesPanelProps) {
  const { buildResponse, requestPayload } = props;

  const [copyNotice, setCopyNotice] = useState<string | null>(null);
  const [copyError, setCopyError] = useState<string | null>(null);

  const result = asRecord(buildResponse?.result);

  const unknownSignal = resolveUnknownSignal(buildResponse);
  const unknowns = unknownSignal.topLevelUnknowns;
  const unknownsInResult = unknownSignal.resultUnknowns;
  const unknownsCanonicalTopLevel = unknownSignal.topLevelCanonicalUnknowns;
  const unknownsCanonicalInResult = unknownSignal.resultCanonicalUnknowns;

  const patchLoop = asRecord(result?.patch_loop_v0);
  const primitiveOverrides = asRecord(result?.primitive_overrides);

  const patchIds = asStringArray(patchLoop?.patch_ids_sorted);
  const patchEffects = asArray(patchLoop?.patch_effect_summary);
  const overridePatchRows = asArray(primitiveOverrides?.applied_patches);

  async function handleCopyBugBundle() {
    setCopyNotice(null);
    setCopyError(null);

    const bundle = {
      deck_input: requestPayload,
      build_output: buildResponse,
      versions: {
        engine_version: firstNonEmptyString(buildResponse?.engine_version),
        ruleset_version: firstNonEmptyString(buildResponse?.ruleset_version),
        ui_contract_version: firstNonEmptyString(result?.ui_contract_version),
        bracket_definition_version: firstNonEmptyString(buildResponse?.bracket_definition_version),
        game_changers_version: firstNonEmptyString(buildResponse?.game_changers_version),
      },
      snapshot_id: firstNonEmptyString(buildResponse?.db_snapshot_id, requestPayload?.db_snapshot_id),
      profile_id: firstNonEmptyString(buildResponse?.profile_id, requestPayload?.profile_id),
      bracket_id: firstNonEmptyString(buildResponse?.bracket_id, requestPayload?.bracket_id),
    };

    try {
      await copyTextToClipboard(toPrettyJson(bundle));
      setCopyNotice("Copied bug bundle JSON to clipboard.");
    } catch (error) {
      setCopyError(error instanceof Error ? error.message : "Clipboard write failed.");
    }
  }

  return (
    <section className="workspace-panel-content">
      <details open className="workspace-collapsible">
        <summary>Unknowns + Patches</summary>

        <div className="workspace-action-row">
          <button type="button" onClick={() => void handleCopyBugBundle()}>
            Copy bug bundle
          </button>
          {copyNotice ? <span className="workspace-copy-notice">{copyNotice}</span> : null}
          {copyError ? <span className="workspace-error-inline">{copyError}</span> : null}
        </div>

        <div className="workspace-panel-split">
          <div>
            <h4>Unknowns</h4>
            {unknownSignal.totalCount === 0 ? (
              <p className="workspace-muted">No unknowns reported.</p>
            ) : (
              <>
                {unknowns.length > 0 ? (
                  <div>
                    <h5>Top-level unknowns</h5>
                    <pre className="workspace-json-block">{toPrettyJson(unknowns)}</pre>
                  </div>
                ) : null}
                {unknownsInResult.length > 0 ? (
                  <div>
                    <h5>Result unknowns</h5>
                    <pre className="workspace-json-block">{toPrettyJson(unknownsInResult)}</pre>
                  </div>
                ) : null}
                {unknownsCanonicalTopLevel.length > 0 ? (
                  <div>
                    <h5>Top-level canonical unknowns</h5>
                    <pre className="workspace-json-block">{toPrettyJson(unknownsCanonicalTopLevel)}</pre>
                  </div>
                ) : null}
                {unknownsCanonicalInResult.length > 0 ? (
                  <div>
                    <h5>Canonical unknowns</h5>
                    <pre className="workspace-json-block">{toPrettyJson(unknownsCanonicalInResult)}</pre>
                  </div>
                ) : null}
              </>
            )}
          </div>

          <div>
            <h4>Patches / overrides</h4>
            <div className="workspace-chip-row">
              <span className="workspace-chip">patches_total: {typeof patchLoop?.patches_total === "number" ? patchLoop.patches_total : 0}</span>
              <span className="workspace-chip">
                patches_applied_total: {typeof patchLoop?.patches_applied_total === "number" ? patchLoop.patches_applied_total : 0}
              </span>
              <span className="workspace-chip">
                overrides_available: {primitiveOverrides?.overrides_available === true ? "true" : "false"}
              </span>
            </div>

            {patchIds.length > 0 ? (
              <div>
                <h5>Patch IDs</h5>
                <ul className="workspace-compact-list">
                  {patchIds.map((patchId: string) => (
                    <li key={patchId}>{patchId}</li>
                  ))}
                </ul>
              </div>
            ) : (
              <p className="workspace-muted">No patch IDs reported.</p>
            )}

            {patchEffects.length > 0 ? (
              <div>
                <h5>Patch effect summary</h5>
                <pre className="workspace-json-block">{toPrettyJson(patchEffects)}</pre>
              </div>
            ) : null}

            {overridePatchRows.length > 0 ? (
              <div>
                <h5>Applied override patches</h5>
                <pre className="workspace-json-block">{toPrettyJson(overridePatchRows)}</pre>
              </div>
            ) : null}
          </div>
        </div>
      </details>
    </section>
  );
}
