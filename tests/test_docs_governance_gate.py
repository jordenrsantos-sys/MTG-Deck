from __future__ import annotations

from pathlib import Path
import unittest


class DocsGovernanceGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo_root = Path(__file__).resolve().parents[1]

        cls.inventory_path = cls.repo_root / "docs" / "ENGINE_TASK_INVENTORY_V1.md"
        cls.spec_path = cls.repo_root / "docs" / "SUFFICIENCY_SPEC_V1.md"
        cls.plan_path = cls.repo_root / "docs" / "ENGINE_IMPLEMENTATION_PLAN_V1.md"

        cls.inventory_text = cls.inventory_path.read_text(encoding="utf-8")
        cls.spec_text = cls.spec_path.read_text(encoding="utf-8")
        cls.plan_text = cls.plan_path.read_text(encoding="utf-8")

    def test_required_doc_sections_exist(self) -> None:
        requirements = [
            (
                self.plan_path,
                self.plan_text,
                "DOCUMENT SYNC CHECKLIST (MANDATORY AFTER EACH STEP)",
            ),
            (
                self.spec_path,
                self.spec_text,
                "half-up must be implemented via Decimal quantize",
            ),
            (
                self.spec_path,
                self.spec_text,
                "K DISCRETIZATION POLICY",
            ),
            (
                self.spec_path,
                self.spec_text,
                "Graph structure must NOT influence overlap_score in v1.",
            ),
            (
                self.spec_path,
                self.spec_text,
                "XVII. COMBO_PACK_PIPELINE_V1 CONTRACT",
            ),
            (
                self.plan_path,
                self.plan_text,
                "STEP 13 — combo_pack_pipeline_v1",
            ),
            (
                self.inventory_path,
                self.inventory_text,
                "SECTION 2 — PHASE 3: SUFFICIENCY ENGINE",
            ),
        ]

        missing = []
        for path, text, needle in requirements:
            if needle not in text:
                missing.append(f"{path.relative_to(self.repo_root)} missing required text: {needle!r}")

        self.assertFalse(
            missing,
            "Required governance doc sections are missing:\n- " + "\n- ".join(sorted(missing)),
        )

    def test_inventory_covers_existing_phase3_layers(self) -> None:
        phase3_layer_filenames = [
            "engine_requirement_detection_v1.py",
            "engine_coherence_v1.py",
            "mulligan_model_v1.py",
            "substitution_engine_v1.py",
            "weight_multiplier_engine_v1.py",
            "probability_math_core_v1.py",
            "probability_checkpoint_layer_v1.py",
            "stress_model_definition_v1.py",
            "stress_transform_engine_v1.py",
            "resilience_math_engine_v1.py",
            "commander_reliability_model_v1.py",
            "sufficiency_summary_v1.py",
        ]

        layer_dir = self.repo_root / "api" / "engine" / "layers"
        existing_layer_files = sorted(
            filename for filename in phase3_layer_filenames if (layer_dir / filename).is_file()
        )

        missing_component_refs = sorted(
            Path(filename).stem
            for filename in existing_layer_files
            if Path(filename).stem not in self.inventory_text
        )

        self.assertFalse(
            missing_component_refs,
            "Inventory missing references for existing Phase 3 layer modules. "
            f"Missing: {missing_component_refs}. "
            f"Existing matched files: {existing_layer_files}",
        )

    def test_inventory_covers_sufficiency_data_pack_json_files(self) -> None:
        sufficiency_dir = self.repo_root / "api" / "engine" / "data" / "sufficiency"
        json_files = sorted(path.name for path in sufficiency_dir.glob("*.json")) if sufficiency_dir.is_dir() else []

        missing_json_refs = sorted(
            filename for filename in json_files if filename not in self.inventory_text
        )

        self.assertFalse(
            missing_json_refs,
            "Inventory missing references for sufficiency data pack JSON files. "
            f"Missing: {missing_json_refs}. "
            f"Discovered JSON files: {json_files}",
        )

    def test_inventory_covers_combo_data_pack_json_files(self) -> None:
        combos_dir = self.repo_root / "api" / "engine" / "data" / "combos"
        json_files = sorted(path.name for path in combos_dir.glob("*.json")) if combos_dir.is_dir() else []

        missing_json_refs = sorted(
            filename for filename in json_files if filename not in self.inventory_text
        )

        self.assertFalse(
            missing_json_refs,
            "Inventory missing references for combo data pack JSON files. "
            f"Missing: {missing_json_refs}. "
            f"Discovered JSON files: {json_files}",
        )


if __name__ == "__main__":
    unittest.main()
