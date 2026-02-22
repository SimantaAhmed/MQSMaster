"""Fetch top crypto from CoinGecko and output FMP-style records.

Also supports symbol matching against a provided reference list (like a large FMP
crypto symbol universe) so your top-500 list aligns with that symbol style.
"""

import json
import logging
import os
import re
import time

import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXTRA_TICKERS_DIR = os.path.join(SCRIPT_DIR, "extra_tickers")
TICKERS_JSON_PATH = os.path.join(EXTRA_TICKERS_DIR, "nasdaq_tickers.json")
CRYPTO_TICKERS_PATH = os.path.join(EXTRA_TICKERS_DIR, "crypto_tickers.json")
TOP_CRYPTO_LIMIT = 500
REFERENCE_JSON_PATH = os.path.join(EXTRA_TICKERS_DIR, "reference_crypto.json")


def normalize_symbol(symbol: str) -> str:
    value = (symbol or "").strip().upper()
    if value.endswith("USD"):
        value = value[:-3]
    return re.sub(r"[^A-Z0-9]", "", value)


def get_top_crypto_from_coingecko(limit: int = 500) -> list[dict]:

    all_coins = []
    per_page = 250
    pages_needed = (limit + per_page - 1) // per_page

    for page in range(1, pages_needed + 1):
        url = (
            "https://api.coingecko.com/api/v3/coins/markets"
            f"?vs_currency=usd&order=market_cap_desc&per_page={per_page}&page={page}"
        )
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            coins = response.json()
            all_coins.extend(coins)
            time.sleep(0.2)
        except Exception as exc:
            logging.error(f"  Error on page {page}: {exc}")
            break

    return all_coins[:limit]


def load_reference_list() -> list[dict]:
    if not os.path.exists(REFERENCE_JSON_PATH):
        logging.error(
            f"No reference file found at {REFERENCE_JSON_PATH}, skipping symbol matching."
        )
        return []

    try:
        with open(REFERENCE_JSON_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, list):
                return data
    except Exception as exc:
        logging.warning(f"Failed to read reference file: {exc}")
    return []


def build_reference_index(reference_rows: list[dict]) -> tuple[dict, dict]:
    by_exact_symbol = {}
    by_base = {}

    for row in reference_rows:
        symbol = (row.get("symbol") or "").strip().upper()
        if not symbol:
            continue

        by_exact_symbol[symbol] = row
        base = normalize_symbol(symbol)
        by_base.setdefault(base, []).append(row)

    return by_exact_symbol, by_base


def pick_reference_symbol(
    base: str, by_exact_symbol: dict, by_base: dict
) -> str | None:
    exact_usd = f"{base}USD"
    if exact_usd in by_exact_symbol:
        return by_exact_symbol[exact_usd]["symbol"].upper()
    if base in by_exact_symbol:
        return by_exact_symbol[base]["symbol"].upper()

    matches = by_base.get(base, [])
    if not matches:
        return None

    for row in matches:
        symbol = (row.get("symbol") or "").upper()
        if symbol.endswith("USD"):
            return symbol

    return (matches[0].get("symbol") or "").upper() or None


def format_for_fmp(
    coins: list[dict], reference_rows: list[dict] | None = None
) -> tuple[list[dict], int]:
    fmp_tickers = []
    matched_count = 0

    by_exact_symbol, by_base = build_reference_index(reference_rows or [])

    for coin in coins:
        raw_symbol = (coin.get("symbol") or "").strip().upper()
        if not raw_symbol:
            continue

        base = normalize_symbol(raw_symbol)
        if not base:
            continue

        matched_symbol = pick_reference_symbol(base, by_exact_symbol, by_base)
        final_symbol = matched_symbol or f"{base}USD"
        if matched_symbol:
            matched_count += 1

        fmp_tickers.append(
            {
                "symbol": final_symbol,
                "price": coin.get("current_price"),
                "change": coin.get("price_change_24h") or 0,
                "volume": coin.get("total_volume"),
            }
        )

    return fmp_tickers, matched_count


def save_crypto_details(crypto_tickers: list[dict], log_summary: bool = True) -> None:
    symbols = []
    for ticker in crypto_tickers:
        symbol = ticker.get("symbol") if isinstance(ticker, dict) else None
        if isinstance(symbol, str) and symbol.strip():
            symbols.append(symbol)

    original_count = len(symbols)
    seen = set()
    deduped_symbols = []
    for symbol in symbols:
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped_symbols.append(symbol)

    duplicate_count = original_count - len(deduped_symbols)

    os.makedirs(os.path.dirname(CRYPTO_TICKERS_PATH), exist_ok=True)
    with open(CRYPTO_TICKERS_PATH, "w") as handle:
        json.dump(deduped_symbols, handle, indent=2)
    if log_summary:
        logging.info(
            "Saved %s entries to crypto_tickers.json (duplicate_count=%s)",
            len(deduped_symbols),
            duplicate_count,
        )


def main() -> None:
    print("=" * 60)
    print("🚀 CRYPTO TICKERS UPDATER")
    print("=" * 60)

    coins = get_top_crypto_from_coingecko(TOP_CRYPTO_LIMIT)
    if not coins:
        print("❌ Failed to fetch crypto data")
        return

    print(f"✅ Fetched {len(coins)} cryptocurrencies")

    reference_rows = load_reference_list()
    fmp_tickers, matched_count = format_for_fmp(coins, reference_rows)

    print(f"✅ Formatted {len(fmp_tickers)} tickers for FMP")
    if reference_rows:
        print(
            f"🔗 Matched {matched_count}/{len(fmp_tickers)} symbols to reference list style"
        )

    save_crypto_details(fmp_tickers)

    print("\n" + "=" * 60)
    print("📋 TOP 20 CRYPTO TICKERS:")
    print("=" * 60)
    for index, coin in enumerate(fmp_tickers[:20], 1):
        print(f"  {index:2}. {coin['symbol']:18} price={coin['price']}")

    print("\n✅ Done! Run this script anytime to refresh crypto list.")


if __name__ == "__main__":
    main()
