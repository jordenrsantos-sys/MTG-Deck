import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const desktopRoot = process.cwd();
const repoRoot = path.resolve(desktopRoot, "..");
const sourceDist = path.join(repoRoot, "ui_harness", "dist");
const targetResourcesRoot = path.join(desktopRoot, "resources");
const targetUiDist = path.join(targetResourcesRoot, "ui_dist");
const markerPath = path.join(targetResourcesRoot, "ui_dist_version.txt");

function fail(message) {
  console.error(message);
  process.exit(1);
}

if (!fs.existsSync(sourceDist) || !fs.existsSync(path.join(sourceDist, "index.html"))) {
  fail("UI dist missing. Run 'cd ui_harness && npm run build' before syncing desktop resources.");
}

fs.mkdirSync(targetResourcesRoot, { recursive: true });
fs.rmSync(targetUiDist, { recursive: true, force: true });
fs.cpSync(sourceDist, targetUiDist, { recursive: true });

const markerValue = `synced_at=${new Date().toISOString()}\nsource=${sourceDist}\nmode=resource_bundle_v1\n`;
fs.writeFileSync(markerPath, markerValue, { encoding: "utf8" });

console.log(`Synced UI dist to ${targetUiDist}`);
console.log(`Wrote UI dist marker ${markerPath}`);
