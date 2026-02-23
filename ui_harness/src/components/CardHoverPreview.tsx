import type { HoverCard } from "./workspaceTypes";
import { buildLocalCardImageUrl, buildPrefetchCardImagesCommand } from "./workspaceUtils";

type CardHoverPreviewProps = {
  apiBase: string;
  snapshotId: string;
  card: HoverCard | null;
  failedImageUrls: Record<string, true>;
  onImageError: (url: string) => void;
};

export default function CardHoverPreview(props: CardHoverPreviewProps) {
  const { apiBase, snapshotId, card, failedImageUrls, onImageError } = props;

  if (!card) {
    return (
      <section className="workspace-panel">
        <h3>Card Art Preview (Local Cache)</h3>
        <p className="workspace-muted">Hover a suggestion or primitive card to preview local art.</p>
      </section>
    );
  }

  const oracleId = card.oracle_id.trim();
  const imageUrl = oracleId !== "" ? buildLocalCardImageUrl(apiBase, oracleId, "normal") : "";
  const imageFailed = imageUrl !== "" && Boolean(failedImageUrls[imageUrl]);
  const canRenderImage = imageUrl !== "" && !imageFailed;

  return (
    <section className="workspace-panel workspace-preview-panel">
      <h3>Card Art Preview (Local Cache)</h3>
      <p className="workspace-preview-title">{card.name}</p>
      <p className="workspace-preview-meta">
        {card.type_line || "Type unavailable"} · source: {card.source}
      </p>

      {canRenderImage ? (
        <img
          src={imageUrl}
          alt={`Card art for ${card.name}`}
          loading="lazy"
          onError={() => {
            onImageError(imageUrl);
          }}
        />
      ) : (
        <div className="workspace-image-placeholder">
          <p>Not cached locally yet for this card/size.</p>
          <p className="workspace-muted">Prefetch command:</p>
          <code>{buildPrefetchCardImagesCommand(snapshotId)}</code>
        </div>
      )}

      {card.primitive_tags.length > 0 ? (
        <div className="workspace-inline-tags">
          {card.primitive_tags.map((tag: string) => (
            <span key={tag} className="workspace-chip workspace-chip-soft">
              {tag}
            </span>
          ))}
        </div>
      ) : null}
    </section>
  );
}
