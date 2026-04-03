#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  Upskillize Agent v3 — One-Command Migration (Linux/Mac)
#  
#  USAGE:
#    cd /path/to/upskillize-agent
#    bash migrate_v3.sh /path/to/upskillize-agent-v3-secure.zip
# ═══════════════════════════════════════════════════════════

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo ""
echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Upskillize Agent v3 — Migration Script${NC}"
echo -e "${CYAN}  Zero API Cost | Security Hardened | Rule-Based${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo ""

ZIP_PATH="$1"

# ── Verify location ──
if [ ! -d "app/agents" ]; then
    echo -e "${RED}ERROR: Run this from your upskillize-agent/ folder!${NC}"
    echo "  cd /path/to/upskillize-agent"
    exit 1
fi

if [ -z "$ZIP_PATH" ] || [ ! -f "$ZIP_PATH" ]; then
    echo -e "${RED}ERROR: Provide path to the v3 zip file${NC}"
    echo "  bash migrate_v3.sh /path/to/upskillize-agent-v3-secure.zip"
    exit 1
fi

# ── Step 1: Safety backup ──
echo -e "${YELLOW}[1/7] Creating safety backup...${NC}"
BACKUP="_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP"
cp -r app/agents/ "$BACKUP/agents/"
cp app/api/deps.py "$BACKUP/" 2>/dev/null || true
cp app/config.py "$BACKUP/" 2>/dev/null || true
cp app/services/data_collector.py "$BACKUP/" 2>/dev/null || true
cp requirements.txt "$BACKUP/" 2>/dev/null || true
cp .env "$BACKUP/.env.bak" 2>/dev/null || true
echo -e "${GREEN}  Backed up to $BACKUP/${NC}"

# ── Step 2: Delete dangerous backup folder ──
echo -e "${YELLOW}[2/7] Removing exposed backup folder...${NC}"
if [ -d "../upskillize-agent-backup" ]; then
    rm -rf "../upskillize-agent-backup"
    echo -e "${RED}  DELETED upskillize-agent-backup/ (had your real API key!)${NC}"
else
    echo -e "  Not found (already removed)"
fi

# ── Step 3: Extract v3 files over current project ──
echo -e "${YELLOW}[3/7] Extracting v3 files...${NC}"
unzip -o "$ZIP_PATH" -d . -x "tests/*" "Dockerfile" "docker-compose.yml"
echo -e "${GREEN}  All v3 files extracted${NC}"

# ── Step 4: Remove old AI dependencies ──
echo -e "${YELLOW}[4/7] Removing AI dependencies...${NC}"
pip uninstall anthropic langchain-anthropic langchain-core -y 2>/dev/null || true
echo -e "${GREEN}  Removed anthropic, langchain-anthropic, langchain-core${NC}"

# ── Step 5: Install updated requirements ──
echo -e "${YELLOW}[5/7] Installing updated requirements...${NC}"
pip install -r requirements.txt --quiet 2>/dev/null || pip install -r requirements.txt
echo -e "${GREEN}  Requirements installed${NC}"

# ── Step 6: Clean pycache ──
echo -e "${YELLOW}[6/7] Cleaning Python cache...${NC}"
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true
echo -e "${GREEN}  Cache cleaned${NC}"

# ── Step 7: Verify ──
echo -e "${YELLOW}[7/7] Verifying...${NC}"

# Check no API imports
if grep -rq "from anthropic\|import anthropic\|from langchain" app/ --include="*.py" 2>/dev/null; then
    echo -e "${RED}  WARNING: Found anthropic/langchain imports still present${NC}"
else
    echo -e "${GREEN}  ✓ No API imports found${NC}"
fi

# Check no hardcoded FinTech
if grep -rq "'FinTech Professional'" app/templates/ --include="*.html" 2>/dev/null; then
    echo -e "${RED}  WARNING: FinTech Professional still in template${NC}"
else
    echo -e "${GREEN}  ✓ No hardcoded FinTech fallbacks${NC}"
fi

# Check no secrets
if grep -rq "sk-ant-api" . --include="*.py" --include="*.env" 2>/dev/null; then
    echo -e "${RED}  WARNING: API key found in files!${NC}"
else
    echo -e "${GREEN}  ✓ No exposed API keys${NC}"
fi

# Check .gitignore
if grep -q "\.env" .gitignore 2>/dev/null; then
    echo -e "${GREEN}  ✓ .gitignore blocks .env files${NC}"
else
    echo -e "${RED}  WARNING: .gitignore missing .env rule${NC}"
fi

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Migration complete!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo ""
echo -e "${RED}DO THESE NOW:${NC}"
echo -e "  1. Revoke API key at ${CYAN}console.anthropic.com${NC}"
echo -e "  2. Change Aiven DB password"
echo -e "  3. Set a real JWT_SECRET in your .env:"
echo -e "     ${CYAN}openssl rand -hex 32${NC}"
echo ""
echo -e "Test it:"
echo -e "  ${CYAN}uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload${NC}"
echo ""
