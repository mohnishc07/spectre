from __future__ import annotations

import math
from decimal import Decimal

import pandas as pd
from edgar import Company, set_identity
from edgar.xbrl import XBRLS

from config.settings import get_settings
from core.schema import FinancialFortressData, ModuleResult, PeriodMetrics, utcnow
from modules.base import AbstractModule
from utils.logger import get_logger

_log = get_logger(__name__)

MAX_PERIODS = 5


def _to_decimal(val: object) -> Decimal | None:
    try:
        if val is None:
            return None
        x = float(val)
        if math.isnan(x) or math.isinf(x):
            return None
        return Decimal(str(x))
    except (TypeError, ValueError, ArithmeticError):
        return None


def _safe_divide(
    numerator: Decimal | None, denominator: Decimal | None
) -> Decimal | None:
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return Decimal(str(round(float(numerator) / float(denominator), 6)))


def _find_row_value(
    df: pd.DataFrame, label_pattern: str, col: str
) -> Decimal | None:
    """Search the 'label' column for a case-insensitive match and return the cell."""
    if "label" not in df.columns:
        return None
    matches = df[df["label"].str.contains(label_pattern, case=False, na=False)]
    if matches.empty or col not in matches.columns:
        return None
    return _to_decimal(matches.iloc[0][col])


def _build_period_metrics(
    income_df: pd.DataFrame,
    balance_df: pd.DataFrame,
    period_cols: list[str],
) -> list[PeriodMetrics]:
    results: list[PeriodMetrics] = []
    for col in period_cols:
        revenue = _find_row_value(income_df, r"(?:^|\b)Revenue", col)
        net_income = _find_row_value(income_df, r"Net Income", col)
        total_assets = _find_row_value(balance_df, r"(?:Total\s+)?Assets$", col)
        total_equity = _find_row_value(
            balance_df, r"Stockholders.*Equity|Shareholders.*Equity", col
        )

        profit_margin = _safe_divide(net_income, revenue)
        asset_turnover = _safe_divide(revenue, total_assets)
        financial_leverage = _safe_divide(total_assets, total_equity)

        roe: Decimal | None = None
        if profit_margin and asset_turnover and financial_leverage:
            roe = Decimal(
                str(
                    round(
                        float(profit_margin)
                        * float(asset_turnover)
                        * float(financial_leverage),
                        6,
                    )
                )
            )

        results.append(
            PeriodMetrics(
                period=col,
                revenue_usd=revenue,
                net_income_usd=net_income,
                total_assets_usd=total_assets,
                total_equity_usd=total_equity,
                profit_margin=profit_margin,
                asset_turnover=asset_turnover,
                financial_leverage=financial_leverage,
                roe=roe,
            )
        )
    return results


def _date_columns(df: pd.DataFrame) -> list[str]:
    """Return columns that look like fiscal period dates (YYYY-MM-DD)."""
    cols: list[str] = []
    for c in df.columns:
        if isinstance(c, str) and len(c) == 10 and c[4] == "-" and c[7] == "-":
            cols.append(c)
    return cols


class FinancialModule(AbstractModule):
    name = "financial"

    def validate(self) -> bool:
        settings = get_settings()
        return bool((settings.EDGAR_IDENTITY or "").strip())

    def run(self, ticker: str) -> ModuleResult:
        now = utcnow()
        settings = get_settings()
        if not self.validate():
            return ModuleResult(
                name=self.name,
                status="error",
                started_at=now,
                completed_at=utcnow(),
                error_message=(
                    "EDGAR_IDENTITY not set; required by edgartools SEC policy"
                ),
            )

        set_identity(settings.EDGAR_IDENTITY)

        headline: str | None = None
        revenue: Decimal | None = None
        total_assets: Decimal | None = None
        periods: list[PeriodMetrics] = []
        err: str | None = None
        final_status: str = "success"

        try:
            company = Company(ticker.upper())
            headline = f"{company.name} (CIK {company.cik})"

            filings = company.get_filings(form="10-K").filter(
                amendments=False
            ).head(MAX_PERIODS)

            xbrls = XBRLS.from_filings(filings)
            income_stmt = xbrls.statements.income_statement(max_periods=MAX_PERIODS)
            balance_stmt = xbrls.statements.balance_sheet(max_periods=MAX_PERIODS)

            inc_df = income_stmt.to_dataframe()
            bs_df = balance_stmt.to_dataframe()

            period_cols = _date_columns(inc_df)
            bs_period_cols = _date_columns(bs_df)
            all_periods = sorted(
                set(period_cols) | set(bs_period_cols), reverse=True
            )[:MAX_PERIODS]

            periods = _build_period_metrics(inc_df, bs_df, all_periods)

            if periods:
                latest = periods[0]
                revenue = latest.revenue_usd
                total_assets = latest.total_assets_usd

            if headline and not periods:
                err = "financials_present_but_no_period_data_extracted"
                final_status = "error"

        except Exception as e:
            _log.error("financial_module_error", ticker=ticker, error=str(e))
            final_status = "error"
            err = str(e)

        payload = FinancialFortressData(
            headline=headline,
            revenue_usd=revenue,
            total_assets_usd=total_assets,
            periods=periods,
        )
        return ModuleResult(
            name=self.name,
            status=final_status if final_status == "success" else "error",
            started_at=now,
            completed_at=utcnow(),
            error_message=err,
            financial=payload,
        )
