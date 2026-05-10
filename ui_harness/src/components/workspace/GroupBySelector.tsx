/**
 * GroupBySelector — Phase 4.6.
 *
 * Dropdown letting the user pick the active grouping axis. Backed by
 * lib/preferences for global+per-deck-override semantics; emits onChange
 * when the user picks a different axis.
 */
import Select from "../../ui/primitives/Select";
import { ALL_GROUP_AXES, GROUP_AXIS_LABELS, type GroupAxis } from "../../lib/grouping";

export type GroupBySelectorProps = {
  value: GroupAxis;
  onChange: (axis: GroupAxis) => void;
  label?: string;
  className?: string;
  disabled?: boolean;
};

export default function GroupBySelector(props: GroupBySelectorProps) {
  const { value, onChange, label, className, disabled = false } = props;
  return (
    <label className={["flex items-center gap-token-2", className].filter(Boolean).join(" ")}>
      {label ? (
        <span className="text-xs text-text-muted whitespace-nowrap">{label}</span>
      ) : null}
      <Select
        value={value}
        onChange={(e) => onChange(e.target.value as GroupAxis)}
        disabled={disabled}
      >
        {ALL_GROUP_AXES.map((axis) => (
          <option key={axis} value={axis}>
            {GROUP_AXIS_LABELS[axis]}
          </option>
        ))}
      </Select>
    </label>
  );
}
