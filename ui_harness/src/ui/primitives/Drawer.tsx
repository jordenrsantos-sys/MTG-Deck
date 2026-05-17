/**
 * Drawer primitive — v1.6 Stage 1.
 *
 * Left-edge slide-in companion to Dialog (4.2). Shares the same compound
 * shape and accessibility behavior (Escape to close + click-backdrop to
 * close), but positions panel content flush against the left viewport
 * edge and constrains width — DeckCheck-style mobile/desktop drawer.
 *
 * Compound shape mirrors Dialog so callers can swap with minimal churn:
 *
 *   <Drawer open={open} onOpenChange={setOpen}>
 *     <DrawerContent>
 *       <DrawerTitle>...</DrawerTitle>
 *       ...children
 *     </DrawerContent>
 *   </Drawer>
 *
 * Per spec Stage 1 halt condition: Dialog's centering layout doesn't
 * accommodate left-edge slide-in, so this is the additive fallback
 * (Drawer is NEW; Dialog stays BYTE-IDENTICAL).
 */
import { useEffect } from "react";
import type { ReactNode } from "react";

type DrawerProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  children: ReactNode;
};

export function Drawer(props: DrawerProps) {
  const { open, onOpenChange, children } = props;
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onOpenChange(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onOpenChange]);
  if (!open) return null;
  return (
    <div
      className="fixed inset-0 z-modal bg-black/60 backdrop-blur-sm"
      role="presentation"
      onClick={() => onOpenChange(false)}
    >
      {children}
    </div>
  );
}

type DrawerContentProps = {
  children: ReactNode;
  className?: string;
};

export function DrawerContent(props: DrawerContentProps) {
  const { children, className } = props;
  // Slide-in panel: fixed to the left edge, full viewport height,
  // bounded width. Borders + elevation match Dialog visual weight.
  const classes = [
    "fixed top-0 left-0 h-full",
    "bg-bg-base text-text-primary",
    "border-r border-glass-border shadow-token-modal",
    "w-72 max-w-[85vw] p-panel-pad overflow-y-auto",
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <div
      role="dialog"
      aria-modal="true"
      className={classes}
      onClick={(e) => e.stopPropagation()}
    >
      {children}
    </div>
  );
}

type DrawerTitleProps = {
  children: ReactNode;
  className?: string;
};

export function DrawerTitle(props: DrawerTitleProps) {
  const { children, className } = props;
  const classes = ["text-lg font-semibold mb-token-2", className].filter(Boolean).join(" ");
  return <h2 className={classes}>{children}</h2>;
}

export default Drawer;
