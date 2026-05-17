/**
 * HamburgerButton — v1.6 Stage 1.
 *
 * Inline-SVG three-line hamburger icon, ~24px, with a button wrapper.
 * Used at the top-left of WorkspaceView (and any layout that exposes
 * the drawer nav) to toggle LeftRail's drawer open/closed.
 *
 * Per HARD #13 (no new dependencies): inline SVG instead of lucide-
 * react / heroicons. The icon optionally renders as an X glyph when
 * the drawer is open (DeckCheck-style affordance).
 */
import Button from "../../ui/primitives/Button";

export type HamburgerButtonProps = {
  open: boolean;
  onToggle: () => void;
  className?: string;
};

export default function HamburgerButton(props: HamburgerButtonProps) {
  const { open, onToggle, className } = props;
  return (
    <Button
      variant="ghost"
      size="md"
      onClick={onToggle}
      aria-label={open ? "Close navigation menu" : "Open navigation menu"}
      aria-expanded={open}
      className={className}
    >
      <svg
        width="24"
        height="24"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        {open ? (
          <>
            <line x1="5" y1="5" x2="19" y2="19" />
            <line x1="19" y1="5" x2="5" y2="19" />
          </>
        ) : (
          <>
            <line x1="4" y1="7" x2="20" y2="7" />
            <line x1="4" y1="12" x2="20" y2="12" />
            <line x1="4" y1="17" x2="20" y2="17" />
          </>
        )}
      </svg>
    </Button>
  );
}
