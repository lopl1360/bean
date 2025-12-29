from __future__ import annotations

import asyncio
import json
from typing import Iterable, List, Optional, Set

from core.config import Settings
from core.logging import get_logger
from core.models import Detection, MarketEvent
from data_sources.alpaca.websocket import AlpacaWebSocket
from detectors.base import Detector
from detectors.registry import build_detectors
from notifier.telegram import TelegramNotifier
from db.repo import AlertsLogRepository, DetectorStateRepository, InMemoryDedupe, WatchlistRepository
from db.mysql import create_pool_from_settings

logger = get_logger(__name__)


async def run_service(
    settings: Settings,
    symbols_limit: Optional[int] = None,
    rotate_interval_sec: Optional[int] = None,
    log_level: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    pool = create_pool_from_settings(settings)
    await pool.connect()
    watchlist_repo = WatchlistRepository(pool)
    state_repo = DetectorStateRepository(pool)
    alerts_repo = AlertsLogRepository(pool)
    in_memory_dedupe = InMemoryDedupe()

    symbols = await watchlist_repo.list_symbols(limit=symbols_limit or settings.watchlist_max)
    logger.info("Loaded watchlist", extra={"count": len(symbols)})

    detector_instances = build_detectors(["example_price_cross"], threshold=settings.example_detector_threshold)
    required_channels: Set[str] = set()
    for detector in detector_instances:
        required_channels |= detector.required_channels

    data_source = AlpacaWebSocket(
        api_key=settings.alpaca_api_key,
        secret_key=settings.alpaca_secret_key,
        data_feed=settings.alpaca_data_feed or "sip",
        max_quotes=settings.symbols_limit or settings.alpaca_max_quotes,
        rotation_interval=rotate_interval_sec or settings.rotation_interval_sec,
    )

    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)

    async def process_detection(detection: Detection, event: MarketEvent) -> None:
        message = f"[{detection.detector_name}] {detection.symbol} @ {detection.data.get('price')} ({detection.severity})"
        should_alert_db = await state_repo.should_alert(detection.symbol, detection.detector_name, detection.message, settings.alert_cooldown_sec)
        should_alert_memory = in_memory_dedupe.should_alert(detection.symbol, detection.detector_name, settings.alert_cooldown_sec)
        if not (should_alert_db and should_alert_memory):
            return
        if dry_run:
            logger.info("DRY RUN detection", extra={"message": message})
        else:
            await notifier.notify(message)
            await alerts_repo.insert_alert(detection.symbol, detection.detector_name, message, json.dumps(event.raw or {}))

    await data_source.connect()
    await data_source.subscribe(symbols=symbols, channels=required_channels)

    try:
        async for event in data_source.stream():
            for detector in detector_instances:
                detections = await detector.on_event(event)
                for detection in detections:
                    await process_detection(detection, event)
    except asyncio.CancelledError:  # pragma: no cover - runner loop
        raise
    finally:
        await data_source.close()
        await pool.close()
