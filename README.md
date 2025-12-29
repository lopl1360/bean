# Watchlist Pattern Scanner

CLI-based watchlist scanner that streams Alpaca market data, runs pluggable detectors, and sends Telegram alerts.

## Features
- Load up to 1000 symbols from MySQL watchlist.
- Alpaca WebSocket streaming with batching and rotation when subscription limits are exceeded.
- Pluggable detector framework with an example price-cross detector.
- Telegram notifications with retry logic and alert deduplication backed by MySQL plus in-memory fallback.
- Dockerfile and compose stack for DigitalOcean deployment.

## Requirements
- Python 3.11+
- MySQL 8+
- Alpaca API keys with market data access
- Telegram bot token and chat ID

## Setup
1. Create a `.env` file with required variables:
```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=watchlist
MYSQL_USER=watcher
MYSQL_PASSWORD=secret
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret
ALPACA_ENV=paper
ALPACA_DATA_FEED=sip
ALPACA_MAX_QUOTES=200
ALERT_COOLDOWN_SEC=300
TELEGRAM_BOT_TOKEN=xxx
TELEGRAM_CHAT_ID=123
WATCHLIST_MAX=1000
EXAMPLE_DETECTOR_THRESHOLD=100
ROTATION_INTERVAL_SEC=300
```

2. Install dependencies and run tests:
```
pip install -r requirements.txt
pytest
```

3. Run migrations against your MySQL instance:
```
mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DB < db/migrations/001_init.sql
```

4. Populate watchlist:
```
python -m cli.main watchlist-add AAPL
```

5. Start scanner:
```
python -m cli.main run --log-level INFO
```
Use `--dry-run` to log detections without sending Telegram messages.

## CLI Commands
- `watchlist-add SYMBOL`
- `watchlist-remove SYMBOL`
- `watchlist-list`
- `run` with options `--symbols-limit`, `--rotate-interval-sec`, `--log-level`, `--dry-run`
- `test-connection` to validate DB, Alpaca, and Telegram access

## Deployment
- Use `docker/docker-compose.yml` for local and DigitalOcean deployments.
- Container uses `python -m cli.main run` as entrypoint with restart policy.
- Logs are emitted to stdout for collection via `docker logs` or platform tooling.
- Apply migrations before first run.
- Recommended droplet: 1vCPU/1GB for testing; scale up for larger watchlists.

## Testing Notes
Unit tests cover message mapping, detector triggering, and dedupe behavior.
