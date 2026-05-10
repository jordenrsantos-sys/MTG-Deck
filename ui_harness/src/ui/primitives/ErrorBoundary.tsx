/**
 * ErrorBoundary — Phase 4 BUNDLE Stage 4.10 (polish).
 *
 * Standard React error boundary that catches render errors in any wrapped
 * subtree and surfaces a friendly fallback Toast/Card instead of crashing
 * the whole shell. Per UI_OVERHAUL_DESIGN's robustness goal: each new
 * Phase 4 panel (Sufficiency / Swap / Recommendation / SeedBuilder) is
 * wrapped individually so a bug in one panel does not nuke the others.
 *
 * Usage:
 *   <ErrorBoundary panelLabel="Sufficiency">
 *     <SufficiencyDashboard ... />
 *   </ErrorBoundary>
 */
import { Component } from "react";
import type { ErrorInfo, ReactNode } from "react";
import { Card, CardHeader, CardBody } from "./Card";
import { devError } from "../../lib/devLog";

type Props = {
  children: ReactNode;
  panelLabel?: string;
  fallback?: ReactNode;
};

type State = {
  hasError: boolean;
  message: string | null;
};

export default class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, message: null };
  }

  static getDerivedStateFromError(err: unknown): State {
    return {
      hasError: true,
      message: err instanceof Error ? err.message : String(err),
    };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    devError(`[ErrorBoundary:${this.props.panelLabel ?? "panel"}]`, error, info);
  }

  render(): ReactNode {
    if (this.state.hasError) {
      if (this.props.fallback !== undefined) return this.props.fallback;
      const label = this.props.panelLabel ?? "Panel";
      return (
        <Card>
          <CardHeader>{label} unavailable</CardHeader>
          <CardBody>
            <div role="alert" className="text-sm text-text-secondary">
              {label} hit a render error and was isolated. The rest of the page
              is still usable. Reload to retry.
            </div>
            {this.state.message ? (
              <pre className="mt-token-2 text-xs text-text-muted whitespace-pre-wrap">
                {this.state.message}
              </pre>
            ) : null}
          </CardBody>
        </Card>
      );
    }
    return this.props.children;
  }
}
