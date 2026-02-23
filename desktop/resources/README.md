# Desktop bundled UI resources

This directory is populated by:

```bash
npm run sync:resources
```

Generated artifacts:
- `ui_dist/` (copied from `../ui_harness/dist`)
- `ui_dist_version.txt` (sync marker used by runtime extraction logic)
- `mtg.sqlite` (copied from `../data/mtg.sqlite` baseline DB)

Do not edit generated files manually.
