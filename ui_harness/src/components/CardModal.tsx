import { useEffect, useMemo, useRef, useState } from "react";

import IconButton from "../ui/primitives/IconButton";

type CardModalProps = {
  isOpen: boolean;
  oracleId: string | null;
  oracleIds?: string[];
  index?: number;
  onClose(): void;
  onPrev?(): void;
  onNext?(): void;
};

function toCardImageSrc(oracleId: string | null): string {
  if (!oracleId) {
    return "";
  }
  const value = oracleId.trim();
  if (value === "") {
    return "";
  }
  return `/cards/image/${value}?size=normal`;
}

export default function CardModal(props: CardModalProps) {
  const { isOpen, oracleId, oracleIds, index, onClose, onPrev, onNext } = props;

  const [failedOracleIds, setFailedOracleIds] = useState<Record<string, true>>({});
  const shellRef = useRef<HTMLDivElement | null>(null);

  const orderedOracleIds = useMemo(() => {
    const seen = new Set<string>();
    const values: string[] = [];

    for (const rawOracleId of oracleIds || []) {
      const value = rawOracleId.trim();
      if (value === "" || seen.has(value)) {
        continue;
      }
      seen.add(value);
      values.push(value);
    }

    const selected = (oracleId || "").trim();
    if (selected !== "" && !seen.has(selected)) {
      values.unshift(selected);
    }

    return values;
  }, [oracleId, oracleIds]);

  const resolvedIndex = useMemo(() => {
    if (orderedOracleIds.length === 0) {
      return 0;
    }

    if (typeof index === "number" && Number.isFinite(index)) {
      const safeIndex = Math.trunc(index);
      if (safeIndex >= 0 && safeIndex < orderedOracleIds.length) {
        return safeIndex;
      }
    }

    const selected = (oracleId || "").trim();
    const selectedIndex = orderedOracleIds.indexOf(selected);
    if (selectedIndex >= 0) {
      return selectedIndex;
    }

    return 0;
  }, [index, oracleId, orderedOracleIds]);

  const activeOracleId = useMemo(() => {
    if (orderedOracleIds.length > 0) {
      return orderedOracleIds[resolvedIndex] || null;
    }
    const selected = (oracleId || "").trim();
    return selected === "" ? null : selected;
  }, [oracleId, orderedOracleIds, resolvedIndex]);

  const mainImageSrc = toCardImageSrc(activeOracleId);
  const canNavigate = orderedOracleIds.length > 1;
  const prevOracleId =
    canNavigate && onPrev
      ? orderedOracleIds[(resolvedIndex - 1 + orderedOracleIds.length) % orderedOracleIds.length] || null
      : null;
  const nextOracleId = canNavigate && onNext ? orderedOracleIds[(resolvedIndex + 1) % orderedOracleIds.length] || null : null;

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    shellRef.current?.focus();
  }, [isOpen, activeOracleId]);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    function handleKeyDown(event: KeyboardEvent): void {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
        return;
      }

      if (event.key === "ArrowLeft" && onPrev) {
        event.preventDefault();
        onPrev();
        return;
      }

      if (event.key === "ArrowRight" && onNext) {
        event.preventDefault();
        onNext();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [isOpen, onClose, onNext, onPrev]);

  if (!isOpen || !activeOracleId || mainImageSrc === "") {
    return null;
  }

  return (
    <div
      className="card-modal-scrim"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <div
        ref={shellRef}
        className="card-modal-shell"
        role="dialog"
        aria-modal="true"
        aria-label="Card art viewer"
        tabIndex={-1}
      >
        <IconButton
          className="card-modal-close"
          onClick={() => {
            onClose();
          }}
          aria-label="Close card modal"
        >
          ×
        </IconButton>

        {prevOracleId ? (
          <div className="card-modal-peek card-modal-peek-left" aria-hidden="true">
            {failedOracleIds[prevOracleId] ? (
              <div className="card-modal-peek-fallback">Art not cached</div>
            ) : (
              <img
                src={toCardImageSrc(prevOracleId)}
                alt=""
                loading="lazy"
                onError={() => {
                  setFailedOracleIds((previous: Record<string, true>) => ({
                    ...previous,
                    [prevOracleId]: true,
                  }));
                }}
              />
            )}
          </div>
        ) : null}

        <div className="card-modal-main">
          {failedOracleIds[activeOracleId] ? (
            <div className="card-modal-art-fallback">Art not cached</div>
          ) : (
            <img
              className="card-modal-art"
              src={mainImageSrc}
              alt={`Card art for ${activeOracleId}`}
              loading="eager"
              onError={() => {
                setFailedOracleIds((previous: Record<string, true>) => ({
                  ...previous,
                  [activeOracleId]: true,
                }));
              }}
            />
          )}
        </div>

        {nextOracleId ? (
          <div className="card-modal-peek card-modal-peek-right" aria-hidden="true">
            {failedOracleIds[nextOracleId] ? (
              <div className="card-modal-peek-fallback">Art not cached</div>
            ) : (
              <img
                src={toCardImageSrc(nextOracleId)}
                alt=""
                loading="lazy"
                onError={() => {
                  setFailedOracleIds((previous: Record<string, true>) => ({
                    ...previous,
                    [nextOracleId]: true,
                  }));
                }}
              />
            )}
          </div>
        ) : null}

        <div className="card-modal-nav">
          {onPrev ? (
            <IconButton
              className="card-modal-nav-button"
              onClick={() => {
                onPrev();
              }}
              aria-label="Previous card"
            >
              ‹
            </IconButton>
          ) : null}
          {onNext ? (
            <IconButton
              className="card-modal-nav-button"
              onClick={() => {
                onNext();
              }}
              aria-label="Next card"
            >
              ›
            </IconButton>
          ) : null}
        </div>
      </div>
    </div>
  );
}
