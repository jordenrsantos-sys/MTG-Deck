import type { ReactNode } from "react";

type ChipProps = {
  children: ReactNode;
  className?: string;
};

export default function Chip(props: ChipProps) {
  const { children, className } = props;
  const classes = ["ui-chip", className].filter(Boolean).join(" ");

  return <span className={classes}>{children}</span>;
}
