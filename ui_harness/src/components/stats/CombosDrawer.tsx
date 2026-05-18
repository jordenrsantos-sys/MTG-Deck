/**
 * CombosDrawer — wraps DeckCombosPanel inside a right-anchored Drawer.
 *
 * Surfaces deck-combo insights (detected combos + missing partners) in a
 * dismissible right-rail drawer instead of inline in the workspace render
 * tree. The toolbar "Combos" button in WorkspaceView controls `open`.
 *
 * The wrapper is intentionally thin — DeckCombosPanel still owns all the
 * combo rendering / null-render contract. The drawer only adds the
 * positioning + open/close affordance.
 */
import { Drawer, DrawerContent, DrawerTitle } from "../../ui/primitives/Drawer";
import DeckCombosPanel, {
  type DetectedComboEntry,
  type MissingPartnerEntry,
} from "./DeckCombosPanel";


export type CombosDrawerProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  detected_combos_v1?: ReadonlyArray<DetectedComboEntry>;
  missing_partners_v1?: ReadonlyArray<MissingPartnerEntry>;
};


export default function CombosDrawer(props: CombosDrawerProps) {
  const { open, onOpenChange, detected_combos_v1, missing_partners_v1 } = props;
  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent side="right" className="w-[28rem]" >
        <DrawerTitle>Deck combos</DrawerTitle>
        <DeckCombosPanel
          detected_combos_v1={detected_combos_v1}
          missing_partners_v1={missing_partners_v1}
        />
      </DrawerContent>
    </Drawer>
  );
}
