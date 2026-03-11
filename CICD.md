# CI/CD Guide

## Pipeline Overview

Workflow file: `.github/workflows/main.yml`

Triggers:
- `pull_request` to `main`: runs CI, then CD build validation (no push/deploy).
- `push` to `main`: runs CI, then full CD (build + publish + deploy).

Execution order on `push`:
1. CI matrix: `python 3.11`, `python 3.12`
2. Docker build and push to GHCR
3. Deployment webhook trigger

CD is gated by CI using `needs: ci`.

## CI Stages

CI runs these checks:
- Install dependencies
- Ruff lint (`E9,F63,F7,F82`)
- Unit tests: `-m "not db and not api"`
- DB integration tests (`-m "db"`) only on `3.11` when DB secrets are present
- API integration tests (`-m "api"`) only on `3.11` when `FMP_API_KEY` is present
- Per-file coverage gate at `50%` on selected production-critical modules
- Upload `coverage.json` artifact per Python version

## CD Stages

CD runs after CI in both trigger types:
- PR: Docker build validation only
- Push to `main`: full publish and deploy flow

CD steps:
- Build Docker image from repo root `Dockerfile`
- On push to `main`: push image to GHCR with `latest` and `sha` tags
- On push to `main`: trigger deployment webhook with payload containing repository, commit SHA, and image tag

Deployment is required:
- On `push` to `main`, if `DEPLOY_WEBHOOK_URL` is not configured, CD fails intentionally.

## Required Secrets

For full CI+CD behavior:
- `DEPLOY_WEBHOOK_URL`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- Optional: `DB_SSLMODE`
- `FMP_API_KEY`

## Local Test Commands

Use PowerShell from repo root.

1. Setup environment and tools:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
python -m pip install ruff pytest pytest-cov pip-audit
```

2. Lint:

```powershell
python -m ruff check src tests scripts --select E9,F63,F7,F82
```

3. Unit tests + coverage JSON:

```powershell
python -m pytest -q -m "not db and not api" --cov=src --cov-report=term-missing --cov-report=json
```

4. Per-file coverage gate (same scope as CI):

```powershell
python scripts/check_per_file_coverage.py --min 50 --coverage-file coverage.json --include-glob "src/backtest/*.py" --include-glob "src/common/auth/*.py" --include-glob "src/orchestrator/marketData/fmpMarketData.py" --include-glob "src/portfolios/indicators/*.py" --include-glob "src/portfolios/strategy_api.py" --include-glob "src/main_backtest.py"
```

5. DB integration tests (if secrets configured):

```powershell
python -m pytest -q -m "db" --cov=src --cov-append --cov-report=term-missing --cov-report=json
```

6. API integration tests (if key configured):

```powershell
python -m pytest -q -m "api" --cov=src --cov-append --cov-report=term-missing --cov-report=json
```

7. Optional dependency security scan:

```powershell
python -m pip_audit -r requirements.txt
```

## Per-File Coverage Gate Scope

The per-file gate intentionally targets tested, production-critical modules and enforces a hard floor of `50%`:
- `src/backtest/*.py`
- `src/common/auth/*.py`
- `src/orchestrator/marketData/fmpMarketData.py`
- `src/portfolios/indicators/*.py`
- `src/portfolios/strategy_api.py`
- `src/main_backtest.py`

As additional modules get tests, add them to the include list in `.github/workflows/main.yml`.

## Action Pinning Policy

All workflow actions are pinned to commit SHAs.

Maintenance:
- Keep source-tag comments next to each SHA.
- Refresh pins monthly or immediately for security advisories.
- Validate updates with full CI before merge.

## Latest Validation Notes

Most recent local CI-like run summary:
- Lint (`ruff`): passed.
- Unit test run (`-m "not db and not api"`): `39 passed`.
- Scoped per-file coverage gate (50% on selected critical modules): passed.
- Total repository coverage from the same run: `33%` (not a failing gate currently).
- DB/API integration tests: not executed in that run (missing local secrets context).

Interpretation:
- Pass rate for executed tests is good (`100%` of executed tests passed).
- Overall project test health is mixed, because broad repository coverage is still low and integration lanes were not exercised in that local run.
