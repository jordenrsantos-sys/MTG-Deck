/**
 * Tabs primitive — Phase 4.2 design system foundation.
 *
 * Compound shape (Radix-style per autonomous_repair_log soft-safety #3 default
 * for ambiguous primitive APIs):
 *
 *   <Tabs value={tab} onValueChange={setTab}>
 *     <TabsList>
 *       <TabsTrigger value="edit">Edit</TabsTrigger>
 *       <TabsTrigger value="view">View</TabsTrigger>
 *     </TabsList>
 *     <TabsContent value="edit">...</TabsContent>
 *   </Tabs>
 */
import { createContext, useContext } from "react";
import type { ReactNode } from "react";

type TabsContextValue = {
  value: string;
  onValueChange: (value: string) => void;
};

const TabsContext = createContext<TabsContextValue | null>(null);

type TabsProps = {
  value: string;
  onValueChange: (value: string) => void;
  children: ReactNode;
  className?: string;
};

export function Tabs(props: TabsProps) {
  const { value, onValueChange, children, className } = props;
  const classes = ["flex flex-col gap-token-2", className].filter(Boolean).join(" ");
  return (
    <TabsContext.Provider value={{ value, onValueChange }}>
      <div className={classes}>{children}</div>
    </TabsContext.Provider>
  );
}

type TabsListProps = {
  children: ReactNode;
  className?: string;
};

export function TabsList(props: TabsListProps) {
  const { children, className } = props;
  // Phase 4.11: flex-wrap so 5-tab lists (e.g. ImportView) wrap on narrow
  // viewports instead of overflowing horizontally; gap-y-1 keeps wrapped rows
  // visually grouped under the same border.
  const classes = [
    "flex flex-wrap items-center gap-token-1 gap-y-1 border-b border-glass-border",
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <div role="tablist" className={classes}>
      {children}
    </div>
  );
}

type TabsTriggerProps = {
  value: string;
  children: ReactNode;
  className?: string;
};

export function TabsTrigger(props: TabsTriggerProps) {
  const { value, children, className } = props;
  const ctx = useContext(TabsContext);
  if (!ctx) throw new Error("TabsTrigger must be inside <Tabs>");
  const active = ctx.value === value;
  const classes = [
    "px-token-3 py-token-2 text-sm font-medium transition-colors duration-fast",
    "border-b-2",
    active
      ? "text-text-primary border-accent"
      : "text-text-muted border-transparent hover:text-text-secondary",
    "focus:outline-none focus-visible:shadow-focus-ring",
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      tabIndex={active ? 0 : -1}
      className={classes}
      onClick={() => ctx.onValueChange(value)}
    >
      {children}
    </button>
  );
}

type TabsContentProps = {
  value: string;
  children: ReactNode;
  className?: string;
};

export function TabsContent(props: TabsContentProps) {
  const { value, children, className } = props;
  const ctx = useContext(TabsContext);
  if (!ctx) throw new Error("TabsContent must be inside <Tabs>");
  if (ctx.value !== value) return null;
  return (
    <div role="tabpanel" className={className}>
      {children}
    </div>
  );
}

export default Tabs;
