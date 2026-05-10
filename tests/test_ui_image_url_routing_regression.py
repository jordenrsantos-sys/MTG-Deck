from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


class UiImageUrlRoutingRegressionTests(unittest.TestCase):
    def test_artdock_uses_api_base_for_cards_image_url(self) -> None:
        source = _read("ui_harness/src/components/cards/ArtDock.tsx")
        self.assertIn("type ArtDockProps = {\n  apiBase: string;", source)
        self.assertIn("normalizeApiBase(apiBase)", source)
        self.assertNotIn("return `/cards/image/${encodeURIComponent(oracleId)}?size=normal`", source)

    def test_workspace_hover_preview_url_uses_api_base(self) -> None:
        source = _read("ui_harness/src/views/WorkspaceView.tsx")
        self.assertIn("function buildHoverPreviewImageUrl(apiBase: string, oracleIdRaw: string): string", source)
        self.assertIn("return `${normalizeApiBase(apiBase)}/cards/image/${encodeURIComponent(oracleId)}?size=normal`;", source)
        self.assertIn("const imageUrl = buildHoverPreviewImageUrl(normalizedApiBase, hoverCard?.oracle_id || \"\");", source)

    def test_workspace_passes_api_base_into_hover_preview_and_cardmodal(self) -> None:
        source = _read("ui_harness/src/views/WorkspaceView.tsx")
        self.assertIn("<HoverCardPreview\n          apiBase={normalizedApiBase}", source)
        self.assertIn("<CardModal\n        apiBase={normalizedApiBase}", source)

    def test_hover_preview_flips_near_viewport_edges(self) -> None:
        source = _read("ui_harness/src/components/cards/HoverCardPreview.tsx")
        self.assertIn("const [viewportSize, setViewportSize] = useState(() => {", source)
        self.assertIn('window.addEventListener("resize", syncViewportSize, { passive: true });', source)
        self.assertIn("const shouldFlipHorizontal =", source)
        self.assertIn("const shouldFlipVertical =", source)
        self.assertIn("const previewWidth = Math.min(PREVIEW_WIDTH_PX, viewportWidth * 0.34);", source)
        self.assertIn("const previewHeight = PREVIEW_HEIGHT_PX * (previewWidth / PREVIEW_WIDTH_PX);", source)
        self.assertIn("pointerPosition.x - previewWidth - PREVIEW_OFFSET_X_PX", source)
        self.assertIn("pointerPosition.y - previewHeight - PREVIEW_OFFSET_Y_PX", source)
        self.assertIn("const left = clamp(preferredLeft", source)
        self.assertIn("const top = clamp(preferredTop", source)

    def test_hover_preview_only_reacts_to_pointer_moves_when_card_is_visible(self) -> None:
        source = _read("ui_harness/src/components/cards/HoverCardPreview.tsx")
        self.assertIn("const pointerPositionRef = useRef({", source)
        self.assertIn("if (!hoverCard) {", source)
        self.assertIn("pointerPositionRef.current = nextPosition;", source)
        self.assertIn("setPointerPosition(pointerPositionRef.current);", source)
        self.assertIn("setPointerPosition(nextPosition);", source)

    def test_hover_preview_renders_placeholder_when_image_missing(self) -> None:
        source = _read("ui_harness/src/components/cards/HoverCardPreview.tsx")
        self.assertIn("const shouldRenderImage = imageUrl !== \"\" && !imageFailed;", source)
        self.assertIn("const placeholderMessage =", source)
        # Phase 4.14 Stage 4: copy unified to "Image not cached." for both
        # the no-URL and image-load-failed branches. The prior dual-string
        # ("No local preview available for this card yet." vs "Card image
        # unavailable in local cache.") confused users — both branches now
        # name the actual condition.
        self.assertIn('"Image not cached."', source)
        self.assertIn('className="workspace-hover-preview-image-placeholder"', source)

    def test_hover_preview_placeholder_has_dedicated_styles(self) -> None:
        source = _read("ui_harness/src/styles.css")
        self.assertIn(".workspace-hover-preview-image-placeholder {", source)
        self.assertIn(".workspace-hover-preview-image-placeholder p {", source)

    def test_cardmodal_uses_build_local_card_image_url_with_api_base(self) -> None:
        source = _read("ui_harness/src/components/CardModal.tsx")
        self.assertIn("type CardModalProps = {\n  apiBase: string;", source)
        self.assertIn("return buildLocalCardImageUrl(apiBase, value, \"normal\");", source)
        self.assertNotIn("return `/cards/image/${value}?size=normal`", source)


if __name__ == "__main__":
    unittest.main()
