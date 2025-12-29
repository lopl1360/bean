from __future__ import annotations

import functools
from typing import Optional, Set

from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    # Database
    mysql_host: str = Field(..., env="MYSQL_HOST")
    mysql_port: int = Field(3306, env="MYSQL_PORT")
    mysql_db: str = Field(..., env="MYSQL_DB")
    mysql_user: str = Field(..., env="MYSQL_USER")
    mysql_password: str = Field(..., env="MYSQL_PASSWORD")

    # Alpaca
    alpaca_api_key: str = Field(..., env="ALPACA_API_KEY")
    alpaca_secret_key: str = Field(..., env="ALPACA_SECRET_KEY")
    alpaca_env: str = Field("paper", env="ALPACA_ENV")
    alpaca_data_feed: Optional[str] = Field(None, env="ALPACA_DATA_FEED")
    alpaca_max_quotes: int = Field(200, env="ALPACA_MAX_QUOTES")

    # Alerts
    alert_cooldown_sec: int = Field(300, env="ALERT_COOLDOWN_SEC")

    # Telegram
    telegram_bot_token: str = Field(..., env="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field(..., env="TELEGRAM_CHAT_ID")

    # Watchlist
    watchlist_max: int = Field(1000, env="WATCHLIST_MAX")

    # Runner
    rotation_interval_sec: int = Field(300, env="ROTATION_INTERVAL_SEC")
    symbols_limit: Optional[int] = Field(None, env="SYMBOLS_LIMIT")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    dry_run: bool = Field(False, env="DRY_RUN")

    # Example detector
    example_detector_threshold: float = Field(100.0, env="EXAMPLE_DETECTOR_THRESHOLD")

    class Config:
        env_file = ".env"
        case_sensitive = False

    @validator("alpaca_env")
    def validate_alpaca_env(cls, value: str) -> str:
        allowed = {"paper", "live"}
        if value not in allowed:
            raise ValueError(f"ALPACA_ENV must be one of {allowed}")
        return value


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
