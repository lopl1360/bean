from __future__ import annotations

import asyncio
import json
from typing import Optional

import typer

from app.runner import run_service
from core.config import get_settings
from core.logging import configure_logging
from db.mysql import create_pool_from_settings
from db.repo import WatchlistRepository

app = typer.Typer(help="Watchlist pattern scanner")


@app.command()
def watchlist_add(symbol: str) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    pool = create_pool_from_settings(settings)
    async def _add() -> None:
        await pool.connect()
        repo = WatchlistRepository(pool)
        await repo.add_symbol(symbol.upper())
        await pool.close()
    asyncio.run(_add())


@app.command()
def watchlist_remove(symbol: str) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    pool = create_pool_from_settings(settings)
    async def _remove() -> None:
        await pool.connect()
        repo = WatchlistRepository(pool)
        await repo.remove_symbol(symbol.upper())
        await pool.close()
    asyncio.run(_remove())


@app.command()
def watchlist_list() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    pool = create_pool_from_settings(settings)
    async def _list() -> None:
        await pool.connect()
        repo = WatchlistRepository(pool)
        symbols = await repo.list_symbols(settings.watchlist_max)
        typer.echo(json.dumps(symbols))
        await pool.close()
    asyncio.run(_list())


@app.command()
def run(
    symbols_limit: Optional[int] = typer.Option(None, help="Override symbols limit"),
    rotate_interval_sec: Optional[int] = typer.Option(None, help="Override rotation interval"),
    log_level: Optional[str] = typer.Option(None, help="Log level"),
    dry_run: bool = typer.Option(False, help="Print detections instead of notifying"),
) -> None:
    settings = get_settings()
    configure_logging(log_level or settings.log_level)
    asyncio.run(
        run_service(
            settings=settings,
            symbols_limit=symbols_limit,
            rotate_interval_sec=rotate_interval_sec,
            log_level=log_level,
            dry_run=dry_run,
        )
    )


@app.command()
def test_connection() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    pool = create_pool_from_settings(settings)

    async def _test() -> None:
        await pool.connect()
        from data_sources.alpaca.client import AlpacaClient
        from notifier.telegram import TelegramNotifier

        client = AlpacaClient(settings.alpaca_api_key, settings.alpaca_secret_key, settings.alpaca_env, settings.alpaca_data_feed)
        await pool.ping()
        await client.ping()
        notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
        await notifier.notify("Test message from Watchlist Scanner")
        await pool.close()

    asyncio.run(_test())


if __name__ == "__main__":
    app()
