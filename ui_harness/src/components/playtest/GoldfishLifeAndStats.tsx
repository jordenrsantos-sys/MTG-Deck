/**
 * GoldfishLifeAndStats — Phase 4.9 Stage 2.
 * Life total +/- buttons + direct input; turn counter; mulligan count.
 */
import { useState } from "react";
import Badge from "../../ui/primitives/Badge";
import Button from "../../ui/primitives/Button";
import Input from "../../ui/primitives/Input";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import type { PlayAction, PlayState } from "../../lib/goldfishState";

export type GoldfishLifeAndStatsProps = {
  state: PlayState;
  dispatch: (action: PlayAction) => void;
};

export default function GoldfishLifeAndStats({ state, dispatch }: GoldfishLifeAndStatsProps) {
  const [adjustText, setAdjustText] = useState("1");
  const adjust = Number.parseInt(adjustText, 10);
  const adjustValue = Number.isFinite(adjust) ? adjust : 1;

  return (
    <Card>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          You <Badge variant="info">turn {state.turn}</Badge>
        </span>
      </CardHeader>
      <CardBody>
        <div className="flex flex-col gap-token-2" aria-label="Life and stats">
          <div className="flex items-center justify-between">
            <span className="text-3xl font-semibold text-text-primary">{state.life}</span>
            <span className="text-xs text-text-muted">life</span>
          </div>
          <div className="flex items-center gap-token-1">
            <Button
              size="sm"
              variant="ghost"
              onClick={() => dispatch({ type: "ADJUST_LIFE", delta: -adjustValue })}
              aria-label={`Subtract ${adjustValue} life`}
            >
              −
            </Button>
            <Input
              value={adjustText}
              onChange={(e) => setAdjustText(e.target.value)}
              inputSize="sm"
              className="w-16 text-center"
              aria-label="Life adjustment amount"
            />
            <Button
              size="sm"
              variant="primary"
              onClick={() => dispatch({ type: "ADJUST_LIFE", delta: adjustValue })}
              aria-label={`Add ${adjustValue} life`}
            >
              +
            </Button>
          </div>
          <div className="flex items-center gap-token-2 text-xs text-text-muted">
            <span>mulligans:</span>
            <Badge variant={state.mulligansTaken > 0 ? "warn" : "neutral"}>{state.mulligansTaken}</Badge>
            <span>· phase: {state.phase}</span>
          </div>
        </div>
      </CardBody>
    </Card>
  );
}
