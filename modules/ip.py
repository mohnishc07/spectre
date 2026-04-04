from __future__ import annotations

from core.schema import IPMineFieldData, ModuleResult, utcnow
from modules.base import AbstractModule
from utils.agent_debug_log import agent_log


class IPModule(AbstractModule):
    name = "ip"

    def validate(self) -> bool:
        return True

    def run(self, ticker: str) -> ModuleResult:
        now = utcnow()
        try:
            import patent_client  # noqa: F401
        except ImportError:
            # #region agent log
            agent_log(
                "IP",
                "modules/ip.py:run",
                "ip_exit",
                {
                    "ticker": ticker,
                    "status": "error",
                    "reason": "patent_client_import_failed",
                },
            )
            # #endregion
            return ModuleResult(
                name=self.name,
                status="error",
                started_at=now,
                completed_at=utcnow(),
                error_message=(
                    "patent-client not installed (lxml build often fails on Py3.13 "
                    "Windows). Use Python 3.12+ with lxml wheels. PatentsView is gone "
                    "(410)."
                ),
                ip=IPMineFieldData(
                    patent_count=None,
                    notes="Add patent-client or USPTO API client",
                ),
            )

        # #region agent log
        agent_log(
            "IP",
            "modules/ip.py:run",
            "ip_exit",
            {"ticker": ticker, "status": "error", "reason": "patent_not_wired"},
        )
        # #endregion
        return ModuleResult(
            name=self.name,
            status="error",
            started_at=now,
            completed_at=utcnow(),
            error_message=(
                "patent-client importable; patent query not implemented in Spectre yet"
            ),
            ip=IPMineFieldData(
                patent_count=None,
                notes="Wire patent-client or USPTO assignee search",
            ),
        )
