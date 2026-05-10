/**
 * Button primitive — Phase 4.2 design system foundation.
 *
 * Wraps a native button with token-driven dark-glass styling. Variants:
 * - "primary" (default): accent-filled
 * - "secondary": glass-border outlined
 * - "ghost": transparent with hover ring
 *
 * Sizes: sm | md (default) | lg.
 *
 * Composition: Tailwind utilities + token CSS variables. No vendor lock.
 */
import type { ButtonHTMLAttributes, ReactNode } from "react";

export type ButtonVariant = "primary" | "secondary" | "ghost";
export type ButtonSize = "sm" | "md" | "lg";

type ButtonProps = {
  children: ReactNode;
  variant?: ButtonVariant;
  size?: ButtonSize;
  className?: string;
} & Omit<ButtonHTMLAttributes<HTMLButtonElement>, "className">;

const VARIANT_CLASSES: Record<ButtonVariant, string> = {
  primary:
    "bg-accent text-text-primary hover:bg-accent-hover active:bg-accent-active border border-transparent",
  secondary:
    "bg-glass-bg text-text-primary border border-glass-border hover:bg-bg-elev-2",
  ghost:
    "bg-transparent text-text-secondary border border-transparent hover:bg-bg-elev-1 hover:text-text-primary",
};

// Phase 4.11: every variant clears the iOS HIG touch-target floor (44px) per
// autonomous_repair_log soft-safety #8. min-h enforces the minimum without
// changing visual height when content is taller; min-w mirrors it for
// icon-only buttons. Per-size paddings + font-size unchanged (keeps existing
// visual layout byte-identical at desktop densities).
const SIZE_CLASSES: Record<ButtonSize, string> = {
  sm: "px-token-2 py-token-1 text-sm rounded-token-sm min-h-[44px] min-w-[44px]",
  md: "px-token-3 py-token-2 text-base rounded-token-md min-h-[44px] min-w-[44px]",
  lg: "px-token-4 py-token-3 text-lg rounded-token-lg min-h-[44px] min-w-[44px]",
};

export default function Button(props: ButtonProps) {
  const { children, variant = "primary", size = "md", className, type = "button", ...rest } = props;
  const classes = [
    "inline-flex items-center justify-center gap-token-2 font-medium",
    "transition-colors duration-fast",
    "focus:outline-none focus-visible:shadow-focus-ring",
    "disabled:opacity-50 disabled:cursor-not-allowed",
    VARIANT_CLASSES[variant],
    SIZE_CLASSES[size],
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <button type={type} className={classes} {...rest}>
      {children}
    </button>
  );
}
