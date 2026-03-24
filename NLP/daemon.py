#!/usr/bin/env python3
"""
daemon.py
Python daemon for NLP article scraping with batched ticker processing.
Runs every 5 minutes with 3 batches and 2-minute intervals between batches.
Optimized for 24/7 VM operation with resource management.
"""

import os
import sys
import time
import json
import logging
import subprocess
import gc
import psutil
from datetime import datetime, timedelta
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Add project root to path
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if proj_root not in sys.path:
    sys.path.insert(0, proj_root)

# Configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
LOG_FILE = SCRIPT_DIR / "daemon.log"

# Portfolio config paths (P1 -> P2 -> P3, later portfolios can add tickers)
PORTFOLIO_CONFIGS = [
    PROJECT_ROOT / "src/portfolios/portfolio_1/config.json",
    PROJECT_ROOT / "src/portfolios/portfolio_2/config.json",
    PROJECT_ROOT / "src/portfolios/portfolio_3/config.json",
]

# Tickers to exclude (e.g. indices that have no news)
EXCLUDED_TICKERS = {"^VIX"}


def load_tickers_from_portfolios() -> list:
    """
    Load and deduplicate tickers from P1 -> P2 -> P3 portfolio configs.
    Preserves insertion order (P1 tickers first, then any new ones from P2/P3).
    """
    seen = set()
    tickers = []
    for config_path in PORTFOLIO_CONFIGS:
        try:
            with open(config_path) as f:
                config = json.load(f)
            for ticker in config.get("TICKERS", []):
                if ticker not in seen and ticker not in EXCLUDED_TICKERS:
                    seen.add(ticker)
                    tickers.append(ticker)
        except Exception as e:
            logging.warning(f"Could not load portfolio config {config_path}: {e}")
    return tickers


def build_batches(tickers: list) -> list:
    """
    Split ticker list into 3 roughly equal batches.
    Returns a list of 3 lists.
    """
    n = len(tickers)
    size = max(1, (n + 2) // 3)  # ceiling division into 3
    return [tickers[i:i + size] for i in range(0, n, size)]


# Load tickers dynamically and split into batches
_all_tickers = load_tickers_from_portfolios()
_batches = build_batches(_all_tickers)
BATCH_1 = _batches[0] if len(_batches) > 0 else []
BATCH_2 = _batches[1] if len(_batches) > 1 else []
BATCH_3 = _batches[2] if len(_batches) > 2 else []

# Timing configuration
SCRAPE_INTERVAL = 300  # 5 minutes in seconds
BATCH_INTERVAL = 120   # 2 minutes between batches

# VM Optimization settings
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10MB max log file
LOG_BACKUP_COUNT = 3  # Keep 3 backup log files
MEMORY_CLEANUP_INTERVAL = 10  # Clean memory every 10 cycles
SKIP_THRESHOLD = 0.8  # Skip sentiment if 80%+ of recent cycles had no new articles

# Setup rotating log handler for VM optimization
log_handler = RotatingFileHandler(
    LOG_FILE, 
    maxBytes=MAX_LOG_SIZE, 
    backupCount=LOG_BACKUP_COUNT
)
log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        log_handler,
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Global tracking for optimization
cycle_count = 0
skip_stats = {ticker: [] for ticker in _all_tickers}


def log_message(message):
    """Log a message with timestamp and memory usage."""
    memory_usage = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    logger.info(f"{message} [Memory: {memory_usage:.1f}MB]")


def cleanup_memory():
    """Force garbage collection and log memory usage."""
    gc.collect()
    memory_usage = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    log_message(f"Memory cleanup completed - Current usage: {memory_usage:.1f}MB")


def update_skip_stats(ticker, had_new_articles):
    """Update skip statistics for intelligent processing."""
    global skip_stats
    
    # Keep only last 10 cycles for each ticker
    if len(skip_stats[ticker]) >= 10:
        skip_stats[ticker].pop(0)
    
    skip_stats[ticker].append(had_new_articles)


def should_skip_sentiment(ticker):
    """Determine if we should skip sentiment processing based on recent history."""
    if len(skip_stats[ticker]) < 5:
        return False  # Not enough data, process normally
    
    # Calculate percentage of recent cycles with new articles
    recent_new_articles = sum(skip_stats[ticker][-5:])  # Last 5 cycles
    new_article_rate = recent_new_articles / 5
    
    # Skip sentiment if very few new articles recently
    if new_article_rate < (1 - SKIP_THRESHOLD):
        log_message(f"Intelligent skip for {ticker} - only {recent_new_articles}/5 recent cycles had new articles")
        return True
    
    return False


def get_date_range():
    """Get date range for the last 7 days."""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    return start_date, end_date


def run_ticker_batch(batch_name, tickers):
    """Run processing for a batch of tickers with VM optimization."""
    log_message(f"Starting batch {batch_name} with tickers: {tickers}")
    
    start_date, end_date = get_date_range()
    batch_start_time = time.time()
    
    for ticker in tickers:
        ticker_start_time = time.time()
        log_message(f"Processing ticker: {ticker}")
        
        try:
            # Step 1: Fetch articles
            log_message(f"Step 1: Fetching articles for {ticker}")
            fetch_cmd = [
                sys.executable, "-m", "NLP.fetch_articles", 
                ticker, start_date, end_date
            ]
            
            result = subprocess.run(
                fetch_cmd, 
                cwd=PROJECT_ROOT,
                capture_output=True, 
                text=True, 
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                # Check if new articles were found by parsing the output
                output = result.stdout
                new_articles_found = check_for_new_articles(output, ticker)
                
                # Update skip statistics
                update_skip_stats(ticker, new_articles_found)
                
                if new_articles_found:
                    log_message(f"Successfully fetched articles for {ticker} - NEW ARTICLES FOUND")
                    
                    # Check if we should skip sentiment processing based on history
                    if should_skip_sentiment(ticker):
                        log_message(f"INTELLIGENT SKIP: Skipping sentiment processing for {ticker} based on recent history")
                    else:
                        # Step 2: Process sentiment and update database
                        log_message(f"Step 2: Processing sentiment and updating database for {ticker}")
                        sentiment_cmd = [
                            sys.executable, "-m", "NLP.process_sentiment_pipeline", 
                            ticker
                        ]
                        
                        result = subprocess.run(
                            sentiment_cmd,
                            cwd=PROJECT_ROOT,
                            capture_output=True,
                            text=True,
                            timeout=600  # 10 minute timeout
                        )
                        
                        if result.returncode == 0:
                            log_message(f"Successfully processed sentiment and updated database for {ticker}")
                        else:
                            log_message(f"ERROR: Failed to process sentiment for {ticker}")
                            log_message(f"Error output: {result.stderr}")
                else:
                    log_message(f"SKIPPING {ticker} - No new articles found since last cycle")
            else:
                log_message(f"ERROR: Failed to fetch articles for {ticker}")
                log_message(f"Error output: {result.stderr}")
                # Update skip stats even for errors
                update_skip_stats(ticker, False)
                
        except subprocess.TimeoutExpired:
            log_message(f"ERROR: Timeout processing {ticker}")
            update_skip_stats(ticker, False)
        except Exception as e:
            log_message(f"ERROR: Exception processing {ticker}: {e}")
            update_skip_stats(ticker, False)
        
        # Log ticker processing time
        ticker_duration = int(time.time() - ticker_start_time)
        log_message(f"Ticker {ticker} processed in {ticker_duration}s")
        
        # Small delay between tickers for VM stability
        time.sleep(5)
    
    batch_duration = int(time.time() - batch_start_time)
    log_message(f"Completed batch {batch_name} in {batch_duration}s")


def check_for_new_articles(fetch_output, ticker):
    """
    Parse fetch_articles.py stdout to determine if new articles were found.
    Relies solely on explicit output lines — no file mod time fallbacks.
    """
    import re

    for line in fetch_output.split('\n'):
        # "Added N new articles" → trust the count directly
        match = re.search(rf'\[{ticker}\] Added (\d+) new articles', line)
        if match:
            count = int(match.group(1))
            log_message(f"{ticker}: {count} new articles found")
            return count > 0

        # "0 new articles (all X were duplicates)"
        if f"[{ticker}] 0 new articles" in line:
            log_message(f"{ticker}: 0 new articles (duplicates)")
            return False

        # "No new articles found in this batch"
        if f"[{ticker}] No new articles found" in line:
            log_message(f"{ticker}: no new articles")
            return False

    # If none of the expected lines appeared, skip to be safe
    log_message(f"{ticker}: could not parse fetch output, skipping")
    return False


def run_scraping_cycle():
    """Run a complete scraping cycle with all batches and VM optimization."""
    global cycle_count
    cycle_count += 1
    
    log_message(f"=== Starting scraping cycle #{cycle_count} ===")
    cycle_start = time.time()
    
    try:
        # Run batch 1
        run_ticker_batch("1", BATCH_1)
        
        # Wait 2 minutes before batch 2
        log_message(f"Waiting {BATCH_INTERVAL} seconds before batch 2...")
        time.sleep(BATCH_INTERVAL)
        
        # Run batch 2
        run_ticker_batch("2", BATCH_2)
        
        # Wait 2 minutes before batch 3
        log_message(f"Waiting {BATCH_INTERVAL} seconds before batch 3...")
        time.sleep(BATCH_INTERVAL)
        
        # Run batch 3
        run_ticker_batch("3", BATCH_3)
        
        cycle_end = time.time()
        cycle_duration = int(cycle_end - cycle_start)
        log_message(f"=== Scraping cycle #{cycle_count} completed in {cycle_duration} seconds ===")
        
        # Memory cleanup every N cycles for VM optimization
        if cycle_count % MEMORY_CLEANUP_INTERVAL == 0:
            cleanup_memory()
        
        # Log skip statistics for monitoring
        total_skips = sum(len([x for x in skip_stats[ticker] if not x]) for ticker in skip_stats)
        total_processes = sum(len(skip_stats[ticker]) for ticker in skip_stats)
        if total_processes > 0:
            skip_rate = (total_skips / total_processes) * 100
            log_message(f"Current skip rate: {skip_rate:.1f}% ({total_skips}/{total_processes})")
        
        return True
        
    except Exception as e:
        log_message(f"ERROR: Exception in scraping cycle: {e}")
        return False


def start_daemon():
    """Start the daemon loop with VM optimization features."""
    log_message("Starting NLP scraper daemon with batched processing and VM optimization...")
    log_message(f"Batch 1: {BATCH_1}")
    log_message(f"Batch 2: {BATCH_2}")
    log_message(f"Batch 3: {BATCH_3}")
    log_message(f"Interval: Every {SCRAPE_INTERVAL} seconds")
    log_message(f"Batch delay: {BATCH_INTERVAL} seconds between batches")
    log_message(f"VM Optimizations: Log rotation ({MAX_LOG_SIZE/1024/1024:.0f}MB), Memory cleanup every {MEMORY_CLEANUP_INTERVAL} cycles")
    
    # Initial memory status
    cleanup_memory()
    
    while True:
        try:
            cycle_start = time.time()
            
            # Run the scraping cycle
            if run_scraping_cycle():
                log_message("Scraping cycle completed successfully")
            else:
                log_message("ERROR: Scraping cycle failed")
            
            # Calculate how long to sleep until next cycle
            cycle_end = time.time()
            cycle_duration = int(cycle_end - cycle_start)
            sleep_time = SCRAPE_INTERVAL - cycle_duration
            
            if sleep_time > 0:
                log_message(f"Sleeping for {sleep_time} seconds until next cycle...")
                time.sleep(sleep_time)
            else:
                log_message(f"WARNING: Cycle took longer than interval ({cycle_duration} > {SCRAPE_INTERVAL} seconds)")
                log_message("Consider increasing SCRAPE_INTERVAL or optimizing processing time")
                
        except KeyboardInterrupt:
            log_message("Daemon stopped by user")
            break
        except Exception as e:
            log_message(f"ERROR: Exception in daemon loop: {e}")
            log_message("Waiting 60 seconds before retry...")
            time.sleep(60)


def main():
    """Main function with enhanced help text."""
    if len(sys.argv) > 1 and sys.argv[1] == "start":
        start_daemon()
    else:
        print("NLP Article Scraper Daemon - VM Optimized")
        print("=" * 50)
        print("Usage: python daemon.py start")
        print()
        print("This daemon runs 24/7 with the following optimizations:")
        print(f"• Batched processing: {BATCH_1}, {BATCH_2}, {BATCH_3}")
        print(f"• Runs every {SCRAPE_INTERVAL} seconds with {BATCH_INTERVAL}s between batches")
        print(f"• Intelligent skip logic to avoid unnecessary processing")
        print(f"• Log rotation ({MAX_LOG_SIZE/1024/1024:.0f}MB max, {LOG_BACKUP_COUNT} backups)")
        print(f"• Memory cleanup every {MEMORY_CLEANUP_INTERVAL} cycles")
        print(f"• Skip threshold: {SKIP_THRESHOLD*100:.0f}% for sentiment processing")
        print()
        print("VM Resource Usage:")
        print("• Low CPU usage when no new articles (90%+ skip rate expected)")
        print("• Memory cleanup prevents memory leaks")
        print("• Rotating logs prevent disk space issues")
        print("• Timeout protection prevents hanging processes")


if __name__ == "__main__":
    main()