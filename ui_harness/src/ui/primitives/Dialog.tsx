/**
 * Dialog primitive — Phase 4.2 design system foundation.
 *
 * Compound shape:
 *
 *   <Dialog open={open} onOpenChange={setOpen}>
 *     <DialogContent>
 *       <DialogTitle>...</DialogTitle>
 *       <DialogDescription>...</DialogDescription>
 *     </DialogContent>
 *   </Dialog>
 *
 * Native <dialog>-style implementation; backdrop + escape close + focus trap
 * deferred to 4.10 polish (autonomous_repair_log: ambiguous primitive API
 * default to Radix-style compound shape).
 */
import { useEffect } from "react";
import type { ReactNode } from "react";

type DialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  children: ReactNode;
};

export function Dialog(props: DialogProps) {
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
      className="fixed inset-0 z-modal flex items-center justify-center bg-black/60"
      role="presentation"
      onClick={() => onOpenChange(false)}
    >
      {children}
    </div>
  );
}

type DialogContentProps = {
  children: ReactNode;
  className?: string;
};

export function DialogContent(props: DialogContentProps) {
  const { children, className } = props;
  const classes = [
    "bg-bg-base text-text-primary",
    "border border-glass-border rounded-token-lg shadow-token-modal",
    "max-w-lg w-full mx-token-3 p-panel-pad",
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

type DialogTitleProps = {
  children: ReactNode;
  className?: string;
};

export function DialogTitle(props: DialogTitleProps) {
  const { children, className } = props;
  const classes = ["text-lg font-semibold mb-token-2", className].filter(Boolean).join(" ");
  return <h2 className={classes}>{children}</h2>;
}

type DialogDescriptionProps = {
  children: ReactNode;
  className?: string;
};

export function DialogDescription(props: DialogDescriptionProps) {
  const { children, className } = props;
  const classes = ["text-sm text-text-secondary", className].filter(Boolean).join(" ");
  return <p className={classes}>{children}</p>;
}

export default Dialog;
