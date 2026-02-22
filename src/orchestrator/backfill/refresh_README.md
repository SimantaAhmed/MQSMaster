# `refresh.py` CLI Arguments

This document covers arguments for:

```bash
python src/orchestrator/backfill/refresh.py [options]
```

## What `refresh.py` does

1. Loads existing tickers from `tickers.json`.
2. Fetches latest S&P 500, commodity, and crypto tickers.
3. Merges + de-duplicates ticker list and writes updated `tickers.json`.
4. Optionally runs concurrent backfill unless `--skip-backfill` is set.

## Arguments

### `--start DDMMYY`
- Start date for backfill (inclusive).
- Format must be `DDMMYY` (example: `010126` = 01 Jan 2026).
- Default: 30 days before end date.

### `--end DDMMYY`
- End date for backfill (inclusive).
- Format must be `DDMMYY`.
- Default: today.

### `--interval INT`
- Bar interval in minutes.
- Default: `1`.

### `--threads INT`
- Number of worker threads for concurrent backfill.
- Default: `8`.

### `--exchange STR`
- Exchange passed to backfill layer.
- Default: `NYSE`.

### `--on-conflict {ignore,fail}`
- Conflict mode for DB insertion.
- Default: `ignore`.

### `--dry-run`
- Fetch/process data but skip database inserts.
- Useful for validation and timing checks.

### `--skip-backfill`
- Only refreshes `tickers.json`.
- Does **not** run backfill.

## Validation behavior

- Invalid date format for `--start` / `--end` exits with code `2`.
- If `start_date > end_date`, script exits with code `2`.

## Examples

### Refresh tickers only

```bash
python src/orchestrator/backfill/refresh.py --skip-backfill
```

### Dry-run backfill for explicit date range

```bash
python src/orchestrator/backfill/refresh.py \
  --start 010126 --end 010226 \
  --threads 8 --interval 1 \
  --dry-run --on-conflict ignore
```

### Full backfill with defaults

```bash
python src/orchestrator/backfill/refresh.py
```

## Notes

- Ticker file path is fixed to `src/orchestrator/backfill/tickers.json`.
- `--exchange` is lowercased before passing into `concurrent_backfill`.
