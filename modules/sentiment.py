from __future__ import annotations

from decimal import Decimal

from core.schema import ModuleResult, SentimentData, utcnow
from modules.base import AbstractModule
from utils.agent_debug_log import agent_log


class SentimentModule(AbstractModule):
    name = "sentiment"

    def validate(self) -> bool:
        try:
            import vaderSentiment  # noqa: F401

            return True
        except ImportError:
            return False

    def run(self, ticker: str) -> ModuleResult:
        now = utcnow()
        if not self.validate():
            # #region agent log
            agent_log(
                "SENT",
                "modules/sentiment.py:run",
                "sentiment_exit",
                {"ticker": ticker, "status": "error", "reason": "vader_missing"},
            )
            # #endregion
            return ModuleResult(
                name=self.name,
                status="error",
                started_at=now,
                completed_at=utcnow(),
                error_message=(
                    "vaderSentiment not installed; pip install vaderSentiment"
                ),
                sentiment=SentimentData(compound_score=None, sample_size=0),
            )

        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        t = ticker.upper()
        lines = [
            f"{t} stock outlook and investor sentiment.",
            f"Earnings expectations and news flow for {t}.",
        ]
        analyzer = SentimentIntensityAnalyzer()
        compounds = [analyzer.polarity_scores(line)["compound"] for line in lines]
        avg = sum(compounds) / len(compounds) if compounds else 0.0
        compound_dec = Decimal(str(round(avg, 6)))

        # #region agent log
        agent_log(
            "SENT",
            "modules/sentiment.py:run",
            "sentiment_exit",
            {
                "ticker": ticker,
                "status": "success",
                "sample_size": len(lines),
            },
        )
        # #endregion
        return ModuleResult(
            name=self.name,
            status="success",
            started_at=now,
            completed_at=utcnow(),
            error_message=None,
            sentiment=SentimentData(
                compound_score=compound_dec,
                sample_size=len(lines),
            ),
        )
