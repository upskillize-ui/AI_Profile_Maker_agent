<#
═══════════════════════════════════════════════════════════
  Upskillize Agent v3 — One-Command Migration (Windows)
  
  Run this from your project ROOT folder:
    cd C:\path\to\Agent@5\upskillize-agent
    powershell -ExecutionPolicy Bypass -File migrate_v3.ps1
═══════════════════════════════════════════════════════════
#>

Write-Host ""
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Upskillize Agent v3 — Migration Script" -ForegroundColor Cyan
Write-Host "  Zero API Cost | Security Hardened | Rule-Based" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# ── Step 0: Verify we're in the right directory ──
if (-not (Test-Path "app/agents")) {
    Write-Host "ERROR: Run this from your upskillize-agent/ folder!" -ForegroundColor Red
    Write-Host "  cd C:\path\to\upskillize-agent" -ForegroundColor Yellow
    exit 1
}

# ── Step 1: Safety backup ──
Write-Host "[1/8] Creating safety backup..." -ForegroundColor Yellow
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupDir = "_backup_$timestamp"
New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
Copy-Item -Path "app/agents/*" -Destination "$backupDir/agents/" -Recurse -Force
Copy-Item -Path "app/api/deps.py" -Destination "$backupDir/" -Force
Copy-Item -Path "app/config.py" -Destination "$backupDir/" -Force
Copy-Item -Path "app/services/data_collector.py" -Destination "$backupDir/" -Force
Copy-Item -Path "requirements.txt" -Destination "$backupDir/" -Force
if (Test-Path ".env") { Copy-Item ".env" "$backupDir/.env.bak" -Force }
Write-Host "  Backed up to $backupDir/" -ForegroundColor Green

# ── Step 2: Delete dangerous backup folder ──
Write-Host "[2/8] Removing exposed backup folder..." -ForegroundColor Yellow
if (Test-Path "../upskillize-agent-backup") {
    Remove-Item "../upskillize-agent-backup" -Recurse -Force
    Write-Host "  DELETED upskillize-agent-backup/ (had your real API key!)" -ForegroundColor Red
} else {
    Write-Host "  Not found (already removed or different location)" -ForegroundColor Gray
}

# ── Step 3: Write .gitignore ──
Write-Host "[3/8] Writing secure .gitignore..." -ForegroundColor Yellow
@"
# SECURITY: Block ALL secrets
.env
.env.*
!.env.example
__pycache__/
*.pyc
*.pyo
*.egg-info/
dist/
build/
.venv/
venv/
.vscode/
.idea/
*.swp
*~
.DS_Store
Thumbs.db
*-backup/
*_backup/
_backup_*/
*.bak
*.log
logs/
*.pem
*.key
*.crt
.huggingface/
"@ | Set-Content ".gitignore" -Encoding UTF8
Write-Host "  .gitignore updated" -ForegroundColor Green

# ── Step 4: Write config.py (NO API KEY) ──
Write-Host "[4/8] Updating config (removing API key dependency)..." -ForegroundColor Yellow
@"
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    APP_NAME: str = "Upskillize Profile Agent"
    APP_VERSION: str = "3.0.0"
    DEBUG: bool = False
    BASE_URL: str = "https://upskillize.com"
    PROFILE_URL_PREFIX: str = "https://upskillize.com/profile"

    DATABASE_URL: str = "mysql+pymysql://root:password@localhost:3306/upskillize_lms"

    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"

    # NO AI KEYS NEEDED - rule-based generation

    MAIL_SERVER: str = "smtp.gmail.com"
    MAIL_PORT: int = 587
    MAIL_USERNAME: str = ""
    MAIL_PASSWORD: str = ""
    MAIL_FROM: str = "noreply@upskillize.com"

    PROFILE_CACHE_TTL: int = 3600
    MAX_CASE_STUDIES_SHOWN: int = 5
    MAX_SKILLS_SHOWN: int = 10

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings():
    return Settings()
"@ | Set-Content "app/config.py" -Encoding UTF8
Write-Host "  config.py updated (API key removed)" -ForegroundColor Green

# ── Step 5: Fix deps.py (JWT security) ──
Write-Host "[5/8] Fixing JWT security in deps.py..." -ForegroundColor Yellow
$depsContent = Get-Content "app/api/deps.py" -Raw
$depsContent = $depsContent -replace 'JWT_SECRET = os\.environ\.get\("JWT_SECRET", "YOUR_REAL_25_CHAR_SECRET_HERE"\)', @'
JWT_SECRET = os.environ.get("JWT_SECRET", "")
if not JWT_SECRET or JWT_SECRET == "YOUR_REAL_25_CHAR_SECRET_HERE":
    logger.critical("JWT_SECRET is not set! Authentication will reject ALL tokens.")
'@
$depsContent | Set-Content "app/api/deps.py" -Encoding UTF8
Write-Host "  JWT_SECRET hardcoded fallback removed" -ForegroundColor Green

# ── Step 6: Fix data_collector.py (student ID bug) ──
Write-Host "[6/8] Fixing personality data bug in data_collector.py..." -ForegroundColor Yellow
$dcContent = Get-Content "app/services/data_collector.py" -Raw

# Fix the personality call to pass both IDs
$dcContent = $dcContent -replace 'personality = self\._get_personality\(student_id\)', 'personality = self._get_personality(student_id, stu_id)'

# Fix the method signature
$dcContent = $dcContent -replace 'def _get_personality\(self, student_id: int\)', 'def _get_personality(self, user_id: int, stu_id: int)'

# Fix the psycho_result query to use user_id
$dcContent = $dcContent -replace '(\s+)("SELECT psycho_result FROM users WHERE id = :sid LIMIT 1"[\s\S]*?\{"sid": )student_id(\})', '$1$2user_id$3'

# Fix the fallback calls to use stu_id
$dcContent = $dcContent -replace 'return self\._derive_personality\(student_id\)', 'return self._derive_personality(stu_id)'

$dcContent | Set-Content "app/services/data_collector.py" -Encoding UTF8
Write-Host "  _get_personality() now uses correct student ID" -ForegroundColor Green

# ── Step 7: Fix template (remove FinTech hardcoding) ──
Write-Host "[7/8] Fixing template hardcoded fallbacks..." -ForegroundColor Yellow
$tplPath = "app/templates/profile_template.html"
if (Test-Path $tplPath) {
    $tplContent = Get-Content $tplPath -Raw
    $tplContent = $tplContent -replace "FinTech Professional \| Upskillize", '{{ student_headline }} | Upskillize'
    $tplContent = $tplContent -replace "student_headline or 'FinTech Professional'", "student_headline or 'Upskillize Learner'"
    $tplContent | Set-Content $tplPath -Encoding UTF8
    Write-Host "  Removed FinTech Professional hardcoding" -ForegroundColor Green
}

# ── Step 8: Fix routes.py (remove AI_MODEL reference) ──
Write-Host "[8/8] Fixing routes.py..." -ForegroundColor Yellow
$routesContent = Get-Content "app/api/routes.py" -Raw
$routesContent = $routesContent -replace 'existing\.ai_model_used = settings\.AI_MODEL', 'existing.ai_model_used = "rule-based-v3"'
$routesContent | Set-Content "app/api/routes.py" -Encoding UTF8
Write-Host "  routes.py fixed" -ForegroundColor Green

# ── Done ──
Write-Host ""
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Green
Write-Host "  Migration complete!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "STILL NEED TO DO MANUALLY:" -ForegroundColor Yellow
Write-Host "  1. Download the v3 zip from Claude chat" -ForegroundColor White
Write-Host "  2. Copy these 3 files FROM the zip (they're full rewrites):" -ForegroundColor White
Write-Host "     - app/agents/summary_agent.py" -ForegroundColor Cyan
Write-Host "     - app/agents/skills_agent.py" -ForegroundColor Cyan
Write-Host "     - app/agents/profile_orchestrator.py" -ForegroundColor Cyan
Write-Host ""
Write-Host "  3. Revoke API key at console.anthropic.com" -ForegroundColor Red
Write-Host "  4. Change Aiven DB password" -ForegroundColor Red
Write-Host "  5. Set JWT_SECRET in .env" -ForegroundColor Red
Write-Host ""
Write-Host "  6. Update requirements:" -ForegroundColor White
Write-Host "     pip uninstall anthropic langchain-anthropic langchain-core -y" -ForegroundColor Cyan
Write-Host ""
