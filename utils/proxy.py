from __future__ import annotations

import itertools
from typing import TYPE_CHECKING

from utils.logger import get_logger

if TYPE_CHECKING:
    from config.settings import Settings

_log = get_logger(__name__)


class ProxyPool:
    """Rotates through configured proxies; supports dry-run (no outbound routing)."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._cycle = (
            itertools.cycle(settings.PROXY_LIST) if settings.PROXY_LIST else None
        )
        self._index = 0

    def next_proxy(self) -> str | None:
        if self._settings.DRY_RUN:
            proxy = None
            if self._cycle:
                proxy = next(self._cycle)
            _log.info("proxy_dry_run", would_use=proxy)
            return None
        if not self._cycle:
            return None
        return next(self._cycle)

    def next_user_agent(self) -> str:
        pool = self._settings.USER_AGENT_POOL
        if not pool:
            return "Spectre/0.1"
        ua = pool[self._index % len(pool)]
        self._index += 1
        return ua
