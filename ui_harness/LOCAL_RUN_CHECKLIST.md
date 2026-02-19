# Local Run Checklist (ui_harness)

1. Install Node.js LTS (Windows PowerShell):
   ```powershell
   winget install OpenJS.NodeJS.LTS
   ```
   Then close and reopen your terminal.

2. Verify Node/npm:
   ```powershell
   node -v
   npm -v
   ```

3. Install dependencies:
   ```powershell
   npm install
   ```

4. Build (catches TypeScript/build errors early):
   ```powershell
   npm run build
   ```

5. Run dev server:
   ```powershell
   npm run dev
   ```

Optional bootstrap scripts:
- PowerShell: `.\scripts\bootstrap_dev.ps1`
- Bash: `./scripts/bootstrap_dev.sh`

Environment variable:
- `VITE_API_BASE_URL` (default: `http://localhost:8000`)
