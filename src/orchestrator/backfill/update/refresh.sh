0 0 * * 0 cd /MQSMaster && . MQS/bin/activate && mkdir -p logs && python src/orchestrator/backfill/update/refresh.py --threads 8 --interval 1 --start $(date -v-30d +\%d\%m\%y) --end $(date +\%d\%m\%y) >> logs/refresh_$(date +\%Y\%m\%d).log 2>&1
#? Script to refresh extra_tickers/nasdaq_tickers.json with latest S&P 500, commodity, and crypto tickers
#?      This runs at midnight (00:00) every Sunday. Breaking it down:
#?          0 0 - at 00:00 (midnight)
#?          * * 0 - every day of month, every month, on Sunday (0)
#? The cd changes to your project directory before running the script