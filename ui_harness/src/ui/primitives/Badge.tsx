/**
 * Badge primitive — Phase 4.2 design system foundation.
 *
 * Small colored label for status / counts / categorical metadata.
 * Distinguished from Chip: Badge is for inline status display (PASS/WARN/FAIL,
 * counts, version pins); Chip is for selectable/filterable tag-like surfaces.
 *
 * Honors Phase 4.1 Decision 4: sufficiency PASS/WARN/FAIL → Badge variants
 * "success" / "warn" / "error".
 */
import type { ReactNode } from "react";

export type BadgeVariant =
  | "neutral"
  | "info"
  | "success"
  | "warn"
  | "error"
  | "accent";

type BadgeProps = {
  children: ReactNode;
  variant?: BadgeVariant;
  className?: string;
};

const VARIANT_CLASSES: Record<BadgeVariant, string> = {
  neutral: "bg-bg-elev-2 text-text-secondary border-glass-border",
  info: "bg-accent/15 text-accent border-accent/40",
  success: "bg-green-500/15 text-green-300 border-green-500/40",
  warn: "bg-amber-500/15 text-amber-300 border-amber-500/40",
  error: "bg-red-500/15 text-red-300 border-red-500/40",
  accent: "bg-accent text-text-primary border-transparent",
};

export default function Badge(props: BadgeProps) {
  const { children, variant = "neutral", className } = props;
  const classes = [
    "inline-flex items-center px-token-2 py-token-1",
    "text-xs font-medium rounded-token-sm border",
    VARIANT_CLASSES[variant],
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return <span className={classes}>{children}</span>;
}
