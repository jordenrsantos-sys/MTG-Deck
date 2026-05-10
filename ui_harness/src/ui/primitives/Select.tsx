/**
 * Select primitive — Phase 4.2 design system foundation.
 *
 * Wraps native select with token-driven styling; for richer popover-driven
 * select, swap to a Radix-style compound component in 4.2.1 if needed.
 */
import type { ReactNode, SelectHTMLAttributes } from "react";

export type SelectSize = "sm" | "md" | "lg";

type SelectProps = {
  children: ReactNode;
  selectSize?: SelectSize;
  invalid?: boolean;
  className?: string;
} & Omit<SelectHTMLAttributes<HTMLSelectElement>, "size" | "className">;

const SIZE_CLASSES: Record<SelectSize, string> = {
  sm: "px-token-2 py-token-1 text-sm rounded-token-sm",
  md: "px-token-3 py-token-2 text-base rounded-token-md",
  lg: "px-token-4 py-token-3 text-lg rounded-token-lg",
};

export default function Select(props: SelectProps) {
  const { children, selectSize = "md", invalid = false, className, ...rest } = props;
  const classes = [
    "w-full bg-glass-bg text-text-primary",
    "border transition-colors duration-fast appearance-none cursor-pointer",
    invalid ? "border-red-500" : "border-glass-border",
    "focus:outline-none focus-visible:shadow-focus-ring focus-visible:border-accent",
    "disabled:opacity-50 disabled:cursor-not-allowed",
    SIZE_CLASSES[selectSize],
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <select className={classes} aria-invalid={invalid || undefined} {...rest}>
      {children}
    </select>
  );
}
