/**
 * Card primitive — Phase 4.2 design system foundation.
 *
 * Visual container for grouped content; less elevated than GlassPanel.
 * Compound shape:
 *
 *   <Card>
 *     <CardHeader>Title</CardHeader>
 *     <CardBody>Content</CardBody>
 *     <CardFooter>Actions</CardFooter>
 *   </Card>
 */
import type { ReactNode } from "react";

type CardProps = {
  children: ReactNode;
  className?: string;
};

export function Card(props: CardProps) {
  const { children, className } = props;
  const classes = [
    "bg-bg-elev-1 border border-glass-border",
    "rounded-token-md shadow-token-soft",
    "p-panel-pad",
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return <article className={classes}>{children}</article>;
}

type CardHeaderProps = {
  children: ReactNode;
  className?: string;
};

export function CardHeader(props: CardHeaderProps) {
  const { children, className } = props;
  const classes = [
    "mb-panel-title-gap text-text-primary text-base font-semibold",
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return <header className={classes}>{children}</header>;
}

type CardBodyProps = {
  children: ReactNode;
  className?: string;
};

export function CardBody(props: CardBodyProps) {
  const { children, className } = props;
  const classes = ["text-text-secondary", className].filter(Boolean).join(" ");
  return <div className={classes}>{children}</div>;
}

type CardFooterProps = {
  children: ReactNode;
  className?: string;
};

export function CardFooter(props: CardFooterProps) {
  const { children, className } = props;
  const classes = [
    "mt-panel-gap pt-panel-title-gap border-t border-glass-border",
    "flex items-center justify-end gap-token-2",
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return <footer className={classes}>{children}</footer>;
}

export default Card;
