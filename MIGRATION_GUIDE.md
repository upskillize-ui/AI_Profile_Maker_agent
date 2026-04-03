# Upskillize Agent v3 — Migration Guide
# 100% Rule-Based | Zero API Cost | Security Hardened

---

## 🚨 DO THESE FIRST (within 5 minutes)

### 1. Revoke your Anthropic API key
Your key was exposed in `upskillize-agent-backup/.env`.
→ Go to https://console.anthropic.com → API Keys → Revoke the compromised key

### 2. Change your database password
Your Aiven DB credentials were exposed in the same file.
→ Go to Aiven console → Change the database password → Update your .env

### 3. Set a real JWT_SECRET
The old code had: `JWT_SECRET = "YOUR_REAL_25_CHAR_SECRET_HERE"` as fallback.
→ Generate a strong secret: `openssl rand -hex 32`
→ Add it to your .env AND your Node.js LMS backend .env

### 4. Delete the backup folder
`upskillize-agent-backup/` contains your real `.env` with all credentials.
→ Delete it from your machine, from git history, and from any shared drives.

### 5. Check GitHub + HuggingFace
Your remotes are:
- `github.com/upskillize-ui/upskillize-profile-agent`
- `huggingface.co/spaces/upskill25/Ai_Enhancer`

Check both for any committed .env files. If found, you need to purge git history:
```bash
git filter-branch --force --index-filter \
  'git rm --cached --ignore-unmatch **/.env' HEAD
git push --force
```

---

## How your $27 was stolen

Your zip file contained `upskillize-agent-backup/.env` with your REAL API key.
The root `.gitignore` had `.env` but that only blocks `.env` at the root level — 
NOT inside subdirectories like `upskillize-agent-backup/.env`.

Additionally, anyone with access to your HuggingFace Space or GitHub repo
could have found the key if it was ever committed.

The new `.gitignore` blocks:
- `.env` at all levels
- `*-backup/` folders entirely  
- `*.pem`, `*.key` certificate files

---

## What changed (v2 → v3)

### Removed entirely
| Component | Why |
|-----------|-----|
| `anthropic` SDK | No API calls needed |
| `langchain-anthropic` | No API calls needed |
| `langchain-core` | No API calls needed |
| `ANTHROPIC_API_KEY` in config | Not needed |
| `AI_MODEL` in config | Using "rule-based-v3" |
| `rubric_grading_agent.py` | Requires API (endpoint disabled with message) |

### Files changed

| File | What changed |
|------|-------------|
| `app/agents/summary_agent.py` | **Rewritten** — template-based, 3 tiers (high/mid/low), name-hash variety |
| `app/agents/skills_agent.py` | **Rewritten** — course→skill mapping, mathematical scoring formula |
| `app/agents/profile_orchestrator.py` | Removed AI fallbacks, uses rule-based agents |
| `app/config.py` | Removed `ANTHROPIC_API_KEY`, `AI_MODEL`, `AI_MAX_TOKENS` |
| `app/api/deps.py` | **Security fix**: JWT_SECRET validates on startup, no hardcoded fallback |
| `app/api/routes.py` | Removed `settings.AI_MODEL` refs, disabled rubric grading endpoint |
| `app/services/data_collector.py` | **Bug fix**: `_get_personality()` now receives correct student ID |
| `app/templates/profile_template.html` | Removed "FinTech Professional" hardcoded fallbacks |
| `requirements.txt` | Removed anthropic, langchain-anthropic, langchain-core |
| `.gitignore` | Blocks .env everywhere, blocks backup folders |
| `.env.example` | No real credentials, clear security warnings |

---

## Bugs fixed

### 1. Personality data was always empty (CRITICAL)
**Location**: `data_collector.py` line 59

**Problem**: `_get_personality(student_id)` was called with `users.id`,  
but `_derive_personality()` queries `quiz_attempts`, `enrollments`, and  
`case_study_submissions` — which use `students.id` (a different table).

This means the personality section showed generic defaults for ALL students  
because the activity queries returned 0 rows with the wrong ID.

**Fix**: `_get_personality()` now takes both `user_id` and `stu_id`.  
- `user_id` → queries `users.psycho_result` (correct table)
- `stu_id` → queries activity tables (correct table)

### 2. Template showed "FinTech Professional" for everyone
**Location**: `profile_template.html` lines 8, 411

**Problem**: Hardcoded fallback `'FinTech Professional'` in hero section  
and og:title. Any student without a headline saw this generic text.

**Fix**: Changed to `'Upskillize Learner'` and uses `student_headline`.

### 3. Summary prompt hardcoded "BFSI & FinTech programs"
**Location**: Old `summary_agent.py` line 59

**Problem**: When a student had no courses, the AI prompt used  
`"BFSI & FinTech programs"` as fallback, generating fake domain references.

**Fix**: Rule-based agent only uses actual course names. No fallback domain.

### 4. JWT_SECRET placeholder allowed token forgery
**Location**: `deps.py` line 21

**Problem**: Default value `"YOUR_REAL_25_CHAR_SECRET_HERE"` meant if the  
env var wasn't set, anyone could forge valid JWT tokens.

**Fix**: Empty default. Startup logs a CRITICAL warning. All auth requests  
return 500 until a real secret is configured.

### 5. Sync API call blocked async event loop
**Location**: Old `summary_agent.py` line 81

**Problem**: `client.messages.create()` (synchronous) inside `async def`  
blocked the event loop — defeating `asyncio.gather()` parallel execution.

**Fix**: No API calls at all. Rule-based generation is instant.

---

## How the rule-based system works

### Summary generation
- 3 performance tiers: HIGH (≥60%), MID (25-60%), LOW (<25%)
- 3 templates per tier = 9 unique summary structures
- Student name hash selects which template → different students get variety
- ALL variables filled from actual computed metrics
- Domain derived from course name keywords

### Skills scoring formula
```
For each skill derived from courses:
  base        = course_progress × 0.4        (0-40 points)
  test_bonus  = avg_test_score × 0.3         (0-30 points)  
  activity    = min(30, quizzes×2 + cases×5 + assignments×3)
  completion  = +15 if course completed
  final_score = min(95, base + test_bonus + activity + completion)
```

Skills are mapped from course names using a keyword dictionary:
- "banking" → Banking Operations, Financial Products
- "fintech" → FinTech Solutions, Digital Banking, Payment Technologies
- etc.

Soft skills are derived from behavior:
- Self-Directed Learning: from total assessment count
- Consistency: from score variance
- Problem Solving: from case study count + scores
- Growth Mindset: from improvement percentage

### Cost comparison
| Metric | v2 (API-based) | v3 (Rule-based) |
|--------|----------------|-----------------|
| Cost per profile | ~$0.01 | $0.00 |
| 1,000 profiles | ~$10 | $0.00 |
| 100,000 profiles | ~$1,065 | $0.00 |
| Generation speed | 3-8 seconds | <100ms |
| Requires API key | Yes | No |
| Works offline | No | Yes |

---

## Deployment

### Local
```bash
cp .env.example .env
# Edit .env with your REAL database URL, JWT_SECRET, etc.
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Docker
```bash
cp .env.example .env
# Edit .env
docker-compose up --build
```

### HuggingFace Spaces
1. Set secrets in Space Settings (NOT in files):
   - `DATABASE_URL`
   - `JWT_SECRET`
   - `REDIS_URL`
2. Push code (without .env)
3. The `.gitignore` will prevent accidental secret exposure

---

## Re-enabling AI features (optional, later)

If you later want AI-powered rubric grading:

1. Get a new API key from console.anthropic.com
2. Add to .env: `ANTHROPIC_API_KEY=your-new-key-here`
3. Add to requirements.txt: `anthropic>=0.42.0`
4. Uncomment the grading code in `routes.py`
5. NEVER share the .env file

Profile generation will stay rule-based regardless — it's faster and free.
