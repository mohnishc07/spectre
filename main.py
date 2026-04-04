from __future__ import annotations

import argparse
import asyncio
import traceback

from config.settings import Settings
from core.orchestrator import run_parallel
from utils.logger import configure_logging, get_logger


async def async_main() -> int:
    configure_logging()
    log = get_logger(__name__)
    parser = argparse.ArgumentParser(description="Spectre moat analysis engine")
    parser.add_argument("--ticker", required=True, help="Ticker symbol, e.g. AAPL")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Enable dry-run mode (proxies log-only)",
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        help="Run modules via arq (requires Redis and worker process)",
    )
    ns = parser.parse_args()

    settings = Settings()
    updates: dict[str, object] = {}
    if ns.dry_run:
        updates["DRY_RUN"] = True
    if ns.queue:
        updates["USE_TASK_QUEUE"] = True
    if updates:
        settings = settings.model_copy(update=updates)

    try:
        await run_parallel(
            ns.ticker,
            use_queue=settings.USE_TASK_QUEUE,
            settings=settings,
        )
    except Exception as exc:
        log.error(
            "spectre_fatal",
            error=str(exc),
            stack=traceback.format_exc(),
        )
        return 1
    return 0


def main() -> None:
    try:
        raise SystemExit(asyncio.run(async_main()))
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
