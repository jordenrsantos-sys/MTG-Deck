/**
 * Toast primitive — Phase 4.2 design system foundation.
 *
 * Imperative API stub: full provider/dispatcher wiring lands in 4.10 polish.
 * Phase 4.2 ships the visual presentation primitive; consumers render <Toast>
 * inside their own state container until 4.10's <ToastProvider> + useToast()
 * hook arrive.
 */
import type { ReactNode } from "react";

export type ToastVariant = "info" | "success" | "warn" | "error";

type ToastProps = {
  children: ReactNode;
  variant?: ToastVariant;
  onDismiss?: () => void;
  className?: string;
};

const VARIANT_CLASSES: Record<ToastVariant, string> = {
  info: "border-accent text-text-primary",
  success: "border-green-500 text-text-primary",
  warn: "border-amber-500 text-text-primary",
  error: "border-red-500 text-text-primary",
};

export default function Toast(props: ToastProps) {
  const { children, variant = "info", onDismiss, className } = props;
  const classes = [
    "flex items-start gap-token-2 px-token-3 py-token-2",
    "bg-glass-bg backdrop-blur-glass",
    "border-l-4 rounded-token-md shadow-token-soft",
    VARIANT_CLASSES[variant],
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <div role="status" aria-live="polite" className={classes}>
      <div className="flex-1">{children}</div>
      {onDismiss ? (
        <button
          type="button"
          aria-label="Dismiss"
          onClick={onDismiss}
          className="text-text-muted hover:text-text-primary transition-colors duration-fast"
        >
          ×
        </button>
      ) : null}
    </div>
  );
}
