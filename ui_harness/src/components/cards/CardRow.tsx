import type { KeyboardEvent, MouseEventHandler, ReactNode } from "react";

type CardRowProps = {
  name: string;
  oracleId?: string | null;
  rightMeta?: ReactNode;
  onOpen?: (oracleId: string) => void;
  tabIndex?: number;
  className?: string;
  onMouseEnter?: MouseEventHandler<HTMLLIElement>;
  onMouseLeave?: MouseEventHandler<HTMLLIElement>;
  onMouseDown?: MouseEventHandler<HTMLLIElement>;
};

export default function CardRow(props: CardRowProps) {
  const {
    name,
    oracleId,
    rightMeta,
    onOpen,
    tabIndex,
    className,
    onMouseEnter,
    onMouseLeave,
    onMouseDown,
  } = props;

  const normalizedOracleId = (oracleId || "").trim();
  const isOpenable = normalizedOracleId !== "" && typeof onOpen === "function";
  const resolvedTabIndex = isOpenable ? tabIndex ?? 0 : tabIndex;
  const classes = ["workspace-card-row", isOpenable ? "workspace-card-openable" : "", className].filter(Boolean).join(" ");

  function handleOpen(): void {
    if (!isOpenable || !onOpen) {
      return;
    }
    onOpen(normalizedOracleId);
  }

  function handleKeyDown(event: KeyboardEvent<HTMLLIElement>): void {
    if (!isOpenable) {
      return;
    }

    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }

    event.preventDefault();
    handleOpen();
  }

  return (
    <li
      className={classes}
      tabIndex={resolvedTabIndex}
      role={isOpenable ? "button" : undefined}
      onClick={isOpenable ? handleOpen : undefined}
      onKeyDown={isOpenable ? handleKeyDown : undefined}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      onMouseDown={onMouseDown}
    >
      <div className="workspace-card-row-main">
        <span className="workspace-card-row-name">{name}</span>
        <div className="workspace-card-row-right">
          {rightMeta ? <div className="workspace-card-row-meta">{rightMeta}</div> : null}
          {isOpenable ? (
            <span className="workspace-card-open-indicator" aria-hidden="true">
              {"\u2197"}
            </span>
          ) : null}
        </div>
      </div>
    </li>
  );
}
