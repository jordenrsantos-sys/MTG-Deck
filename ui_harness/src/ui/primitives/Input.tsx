/**
 * Input primitive — Phase 4.2 design system foundation.
 *
 * Wraps a native input with token-driven dark-glass styling. Sizes: sm | md (default) | lg.
 * Optional invalid state for form validation surfaces.
 */
import type { InputHTMLAttributes } from "react";

export type InputSize = "sm" | "md" | "lg";

type InputProps = {
  inputSize?: InputSize;
  invalid?: boolean;
  className?: string;
} & Omit<InputHTMLAttributes<HTMLInputElement>, "size" | "className">;

const SIZE_CLASSES: Record<InputSize, string> = {
  sm: "px-token-2 py-token-1 text-sm rounded-token-sm",
  md: "px-token-3 py-token-2 text-base rounded-token-md",
  lg: "px-token-4 py-token-3 text-lg rounded-token-lg",
};

export default function Input(props: InputProps) {
  const { inputSize = "md", invalid = false, className, ...rest } = props;
  const classes = [
    "w-full bg-glass-bg text-text-primary placeholder:text-text-muted",
    "border transition-colors duration-fast",
    invalid ? "border-red-500" : "border-glass-border",
    "focus:outline-none focus-visible:shadow-focus-ring focus-visible:border-accent",
    "disabled:opacity-50 disabled:cursor-not-allowed",
    SIZE_CLASSES[inputSize],
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return <input className={classes} aria-invalid={invalid || undefined} {...rest} />;
}
