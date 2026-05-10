import type { HoverCard } from "../workspaceTypes";
import { normalizeApiBase } from "../workspaceUtils";

import GlassPanel from "../../ui/primitives/GlassPanel";

type ArtDockProps = {
  apiBase: string;
  hoverCard: HoverCard | null;
  onClear?: () => void;
  previewImageFailures: Record<string, true>;
  markPreviewImageFailure: (imageUrl: string, oracleId: string) => void;
  clearMissingImageForOracle: (oracleId: string) => void;
};

function buildArtImageUrl(apiBase: string, oracleIdRaw: string): string {
  const oracleId = oracleIdRaw.trim();
  if (oracleId === "") {
    return "";
  }
  return `${normalizeApiBase(apiBase)}/cards/image/${encodeURIComponent(oracleId)}?size=normal`;
}

function renderSourceLabel(source: HoverCard["source"]): string {
  if (source === "deck") {
    return "Deck";
  }
  if (source === "primitive") {
    return "Primitive";
  }
  if (source === "suggest") {
    return "Suggest";
  }
  return "Unknown";
}

export default function ArtDock(props: ArtDockProps) {
  const { apiBase, hoverCard, onClear, previewImageFailures, markPreviewImageFailure, clearMissingImageForOracle } = props;

  const imageUrl = buildArtImageUrl(apiBase, hoverCard?.oracle_id || "");
  const imageFailed = imageUrl !== "" && Boolean(previewImageFailures[imageUrl]);
  const canRenderImage = imageUrl !== "" && !imageFailed;

  return (
    <GlassPanel className="art-dock workspace-panel-content">
      <div className="art-dock-header">
        <h3>Card Preview</h3>

        <div className="art-dock-header-actions">
          {hoverCard ? <span className="workspace-chip workspace-chip-soft art-dock-source">{renderSourceLabel(hoverCard.source)}</span> : null}
          {hoverCard && onClear ? (
            <button
              type="button"
              className="workspace-link-button art-dock-clear-button"
              onClick={() => {
                onClear();
              }}
            >
              Clear
            </button>
          ) : null}
        </div>
      </div>

      <div className="art-dock-body">
        {!hoverCard ? (
          <div className="art-dock-empty">
            <p>Hover a card to preview art.</p>
            <p className="workspace-muted">Preview stays pinned here while you work.</p>
          </div>
        ) : (
          <>
            <p className="art-dock-card-name">{hoverCard.name}</p>
            <p className="art-dock-card-type">{hoverCard.type_line || "Type unavailable"}</p>

            <div className="art-dock-image-wrap">
              {canRenderImage ? (
                <img
                  className="art-dock-image"
                  src={imageUrl}
                  alt={`Card art for ${hoverCard.name}`}
                  loading="lazy"
                  onLoad={() => {
                    clearMissingImageForOracle(hoverCard.oracle_id || "");
                  }}
                  onError={() => {
                    markPreviewImageFailure(imageUrl, hoverCard.oracle_id || "");
                  }}
                />
              ) : (
                <div className="workspace-image-placeholder art-dock-image-placeholder">
                  {imageUrl === "" ? <p>No local oracle ID is available for this card row.</p> : <p>Image missing for this local card.</p>}
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </GlassPanel>
  );
}
