import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const desktopRoot = process.cwd();
const repoRoot = path.resolve(desktopRoot, "..");
const sourceDb = path.join(repoRoot, "data", "mtg.sqlite");
const targetResourcesRoot = path.join(desktopRoot, "resources");
const targetDb = path.join(targetResourcesRoot, "mtg.sqlite");

function fail(message) {
  console.error(message);
  process.exit(1);
}

if (!fs.existsSync(sourceDb)) {
  fail("Baseline DB missing at data/mtg.sqlite. Generate or restore it before syncing desktop resources.");
}

fs.mkdirSync(targetResourcesRoot, { recursive: true });
fs.copyFileSync(sourceDb, targetDb);

console.log(`Synced baseline DB to ${targetDb}`);
