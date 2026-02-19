import py_compile
import sys

FILES = [
    "engine/db_tags.py",
    "taxonomy/__init__.py",
    "taxonomy/exporter.py",
    "taxonomy/loader.py",
    "snapshot_build/index_build.py",
    "snapshot_build/tag_snapshot.py",
    "api/engine/pipeline_build.py",
    "api/main.py",
    "tests/test_taxonomy_compiler.py",
    "tests/test_runtime_tags.py",
]

for f in FILES:
    try:
        py_compile.compile(f, doraise=True)
        print(f"OK: {f}")
    except Exception as e:
        print(f"ERROR in {f}: {e}")
        sys.exit(1)

print("All files compiled successfully.")
