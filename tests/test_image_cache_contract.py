from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from engine.image_cache_contract import (
    ensure_size_dir,
    image_relpath,
    resolve_local_image_path,
)

ORACLE_ID = "123e4567-e89b-12d3-a456-426614174000"


class ImageCacheContractTests(unittest.TestCase):
    def test_image_relpath_uses_contract_layout(self) -> None:
        relpath = image_relpath(oracle_id=ORACLE_ID, size="normal", ext="jpg")
        self.assertEqual(relpath, f"normal/{ORACLE_ID}.jpg")

    def test_resolve_local_image_path_prefers_extension_order(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "normal").mkdir(parents=True, exist_ok=True)

            # Preferred order: jpg -> jpeg -> png -> webp
            (root / "normal" / f"{ORACLE_ID}.webp").write_bytes(b"webp")
            (root / "normal" / f"{ORACLE_ID}.png").write_bytes(b"png")

            resolved = resolve_local_image_path(
                cache_root=str(root),
                oracle_id=ORACLE_ID,
                size="normal",
            )

            self.assertEqual(resolved, str((root / "normal" / f"{ORACLE_ID}.png").resolve()))

            (root / "normal" / f"{ORACLE_ID}.jpeg").write_bytes(b"jpeg")
            resolved_with_jpeg = resolve_local_image_path(
                cache_root=str(root),
                oracle_id=ORACLE_ID,
                size="normal",
            )
            self.assertEqual(resolved_with_jpeg, str((root / "normal" / f"{ORACLE_ID}.jpeg").resolve()))

    def test_ensure_size_dir_creates_size_directory(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            ensured = ensure_size_dir(cache_root=str(root), size="small")
            self.assertEqual(ensured, str((root / "small").resolve()))
            self.assertTrue((root / "small").is_dir())

    @unittest.skipUnless(os.name == "nt", "Windows-only extended-length path behavior")
    def test_contract_accepts_windows_extended_path_prefixes(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir).resolve()
            (root / "normal").mkdir(parents=True, exist_ok=True)
            image_path = root / "normal" / f"{ORACLE_ID}.jpg"
            image_path.write_bytes(b"jpg")

            extended_root = f"\\\\?\\{root}"
            resolved = resolve_local_image_path(
                cache_root=extended_root,
                oracle_id=ORACLE_ID,
                size="normal",
            )

            self.assertEqual(Path(resolved) if resolved else None, image_path.resolve())

            ensured = ensure_size_dir(cache_root=extended_root, size="small")
            self.assertEqual(Path(ensured), (root / "small").resolve())
            self.assertTrue((root / "small").is_dir())


if __name__ == "__main__":
    unittest.main()
