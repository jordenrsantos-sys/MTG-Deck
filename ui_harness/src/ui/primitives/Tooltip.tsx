/**
 * Tooltip primitive — Phase 4.2 design system foundation.
 *
 * CSS-only hover tooltip; production callers wrap a trigger element with
 * `<Tooltip content="...">`. Positioning is via CSS (top/right/bottom/left).
 * Full Floating UI integration deferred to 4.10 polish.
 *
 * Honors Phase 4.1 Decision 10 ("Why?" explainers everywhere): tooltip text
 * is content-driven; the primitive never generates explainer text.
 */
import { useState } from "react";
import type { ReactNode } from "react";

export type TooltipPlacement = "top" | "right" | "bottom" | "left";

type TooltipProps = {
  content: ReactNode;
  placement?: TooltipPlacement;
  children: ReactNode;
  className?: string;
};

const PLACEMENT_CLASSES: Record<TooltipPlacement, string> = {
  top: "bottom-full left-1/2 -translate-x-1/2 mb-token-1",
  right: "left-full top-1/2 -translate-y-1/2 ml-token-1",
  bottom: "top-full left-1/2 -translate-x-1/2 mt-token-1",
  left: "right-full top-1/2 -translate-y-1/2 mr-token-1",
};

export default function Tooltip(props: TooltipProps) {
  const { content, placement = "top", children, className } = props;
  const [open, setOpen] = useState(false);
  const wrapperClasses = ["relative inline-flex", className].filter(Boolean).join(" ");
  const tipClasses = [
    "absolute z-modal pointer-events-none",
    "px-token-2 py-token-1 text-xs whitespace-nowrap",
    "bg-bg-base text-text-primary border border-glass-border rounded-token-sm shadow-token-soft",
    PLACEMENT_CLASSES[placement],
  ].join(" ");
  return (
    <span
      className={wrapperClasses}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
    >
      {children}
      {open ? (
        <span role="tooltip" className={tipClasses}>
          {content}
        </span>
      ) : null}
    </span>
  );
}
