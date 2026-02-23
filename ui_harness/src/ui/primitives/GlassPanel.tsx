import type { MouseEventHandler, ReactNode } from "react";

type GlassPanelProps = {
  children: ReactNode;
  className?: string;
  onClick?: MouseEventHandler<HTMLElement>;
};

export default function GlassPanel(props: GlassPanelProps) {
  const { children, className, onClick } = props;
  const classes = ["glass-panel", className].filter(Boolean).join(" ");

  return (
    <section className={classes} onClick={onClick}>
      {children}
    </section>
  );
}
