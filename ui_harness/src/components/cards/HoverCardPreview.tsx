import { useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";

import type { HoverCard } from "../workspaceTypes";
import { normalizeApiBase } from "../workspaceUtils";

type HoverCardPreviewProps = {
  apiBase: string;
  hoverCard: HoverCard | null;
  previewImageFailures: Record<string, true>;
  markPreviewImageFailure: (imageUrl: string, oracleId: string) => void;
  clearMissingImageForOracle: (oracleId: string) => void;
};

const PREVIEW_WIDTH_PX = 240;
const PREVIEW_HEIGHT_PX = 380;
const PREVIEW_OFFSET_X_PX = 22;
const PREVIEW_OFFSET_Y_PX = 18;
const PREVIEW_EDGE_PADDING_PX = 10;

function buildArtImageUrl(apiBase: string, oracleIdRaw: string): string {
  const oracleId = oracleIdRaw.trim();
  if (oracleId === "") {
    return "";
  }
  return `${normalizeApiBase(apiBase)}/cards/image/${encodeURIComponent(oracleId)}?size=normal`;
}

function clamp(value: number, minValue: number, maxValue: number): number {
  if (value < minValue) {
    return minValue;
  }
  if (value > maxValue) {
    return maxValue;
  }
  return value;
}

export default function HoverCardPreview(props: HoverCardPreviewProps) {
  const { apiBase, hoverCard, previewImageFailures, markPreviewImageFailure, clearMissingImageForOracle } = props;
  const [pointerPosition, setPointerPosition] = useState(() => ({
    x: PREVIEW_EDGE_PADDING_PX,
    y: PREVIEW_EDGE_PADDING_PX,
  }));
  const [viewportSize, setViewportSize] = useState(() => {
    if (typeof window === "undefined") {
      return {
        width: PREVIEW_WIDTH_PX + PREVIEW_EDGE_PADDING_PX * 2,
        height: PREVIEW_HEIGHT_PX + PREVIEW_EDGE_PADDING_PX * 2,
      };
    }
    return {
      width: window.innerWidth,
      height: window.innerHeight,
    };
  });
  const pointerPositionRef = useRef({
    x: PREVIEW_EDGE_PADDING_PX,
    y: PREVIEW_EDGE_PADDING_PX,
  });

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    function syncViewportSize(): void {
      const nextWidth = window.innerWidth;
      const nextHeight = window.innerHeight;
      setViewportSize((previous) => {
        if (previous.width === nextWidth && previous.height === nextHeight) {
          return previous;
        }
        return {
          width: nextWidth,
          height: nextHeight,
        };
      });
    }

    syncViewportSize();
    window.addEventListener("resize", syncViewportSize, { passive: true });
    return () => {
      window.removeEventListener("resize", syncViewportSize);
    };
  }, []);

  useEffect(() => {
    if (!hoverCard) {
      return;
    }
    setPointerPosition(pointerPositionRef.current);
  }, [hoverCard]);

  useEffect(() => {
    let frameId = 0;

    function handlePointerMove(event: PointerEvent): void {
      const nextPosition = {
        x: event.clientX,
        y: event.clientY,
      };
      pointerPositionRef.current = nextPosition;
      if (!hoverCard) {
        return;
      }
      if (frameId !== 0) {
        window.cancelAnimationFrame(frameId);
      }
      frameId = window.requestAnimationFrame(() => {
        setPointerPosition(nextPosition);
      });
    }

    window.addEventListener("pointermove", handlePointerMove, { passive: true });
    return () => {
      if (frameId !== 0) {
        window.cancelAnimationFrame(frameId);
      }
      window.removeEventListener("pointermove", handlePointerMove);
    };
  }, [hoverCard]);

  const imageUrl = useMemo(() => buildArtImageUrl(apiBase, hoverCard?.oracle_id || ""), [apiBase, hoverCard?.oracle_id]);
  const imageFailed = imageUrl !== "" && Boolean(previewImageFailures[imageUrl]);
  const shouldRenderImage = imageUrl !== "" && !imageFailed;

  const previewStyle = useMemo((): CSSProperties => {
    const viewportWidth = viewportSize.width;
    const viewportHeight = viewportSize.height;
    const previewWidth = Math.min(PREVIEW_WIDTH_PX, viewportWidth * 0.34);
    const previewHeight = PREVIEW_HEIGHT_PX * (previewWidth / PREVIEW_WIDTH_PX);
    const shouldFlipHorizontal = pointerPosition.x + PREVIEW_OFFSET_X_PX + previewWidth + PREVIEW_EDGE_PADDING_PX > viewportWidth;
    const shouldFlipVertical = pointerPosition.y + PREVIEW_OFFSET_Y_PX + previewHeight + PREVIEW_EDGE_PADDING_PX > viewportHeight;

    const preferredLeft = shouldFlipHorizontal
      ? pointerPosition.x - previewWidth - PREVIEW_OFFSET_X_PX
      : pointerPosition.x + PREVIEW_OFFSET_X_PX;
    const preferredTop = shouldFlipVertical
      ? pointerPosition.y - previewHeight - PREVIEW_OFFSET_Y_PX
      : pointerPosition.y + PREVIEW_OFFSET_Y_PX;

    const maxLeft = Math.max(PREVIEW_EDGE_PADDING_PX, viewportWidth - previewWidth - PREVIEW_EDGE_PADDING_PX);
    const maxTop = Math.max(PREVIEW_EDGE_PADDING_PX, viewportHeight - previewHeight - PREVIEW_EDGE_PADDING_PX);
    const left = clamp(preferredLeft, PREVIEW_EDGE_PADDING_PX, maxLeft);
    const top = clamp(preferredTop, PREVIEW_EDGE_PADDING_PX, maxTop);

    return {
      left: `${left}px`,
      top: `${top}px`,
    };
  }, [pointerPosition.x, pointerPosition.y, viewportSize.height, viewportSize.width]);

  if (!hoverCard) {
    return null;
  }

  // Phase 4.14 Stage 4: clearer copy per autonomous_repair_log soft-safety
  // #7. The prior "No local preview available for this card yet." was
  // confusing — users read it as a UI bug. The new copy names the actual
  // condition (image hasn't been cached locally) without implying breakage.
  const placeholderMessage =
    imageUrl === "" ? "Image not cached." : "Image not cached.";

  return (
    <div className="workspace-hover-preview" style={previewStyle} role="presentation" aria-hidden="true">
      <div className="workspace-hover-preview-meta">
        <p className="workspace-hover-preview-name">{hoverCard.name}</p>
        <p className="workspace-hover-preview-type">{hoverCard.type_line || "Type unavailable"}</p>
      </div>

      {shouldRenderImage ? (
        <img
          className="workspace-hover-preview-image"
          src={imageUrl}
          alt=""
          loading="eager"
          onLoad={() => {
            clearMissingImageForOracle(hoverCard.oracle_id || "");
          }}
          onError={() => {
            markPreviewImageFailure(imageUrl, hoverCard.oracle_id || "");
          }}
        />
      ) : (
        <div className="workspace-hover-preview-image-placeholder">
          <p>{placeholderMessage}</p>
        </div>
      )}
    </div>
  );
}
