import type { ButtonHTMLAttributes, ReactNode } from "react";

type IconButtonProps = {
  children: ReactNode;
  className?: string;
} & Omit<ButtonHTMLAttributes<HTMLButtonElement>, "className">;

export default function IconButton(props: IconButtonProps) {
  const { children, className, type = "button", ...rest } = props;
  const classes = ["icon-button", className].filter(Boolean).join(" ");

  return (
    <button type={type} className={classes} {...rest}>
      {children}
    </button>
  );
}
