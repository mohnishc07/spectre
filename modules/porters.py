from __future__ import annotations

import math
from decimal import Decimal

from edgar import Company, set_identity

from config.settings import get_settings
from core.schema import (
    CompetitorMetrics,
    ModuleResult,
    PortersFiveForcesData,
    utcnow,
)
from modules.base import AbstractModule
from utils.logger import get_logger

_log = get_logger(__name__)

MAX_TEXT_CHARS = 5000
MAX_COMPETITORS = 4


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


def _truncate(text: str | None, limit: int = MAX_TEXT_CHARS) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit] + "... [truncated]"


def _extract_10k_section(company: Company, item_key: str) -> str | None:
    """Extract a section from the latest 10-K filing."""
    try:
        filing = company.get_filings(form="10-K").latest()
        tenk = filing.obj()
        section = tenk[item_key]
        if section is None:
            return None
        return str(section)
    except Exception:
        return None


def _get_company_margins(
    company: Company,
) -> tuple[str | None, Decimal | None, Decimal | None]:
    """Return (name, operating_margin, rd_to_revenue)."""
    try:
        financials = company.get_financials()
        if financials is None:
            return (company.name, None, None)

        revenue = _to_decimal(financials.get_revenue())
        operating_income = _to_decimal(financials.get_net_income())
        rd_expense: Decimal | None = None

        income_stmt = financials.income_statement()
        if income_stmt is not None:
            inc_df = income_stmt.to_dataframe(view="summary")
            if "label" in inc_df.columns:
                period_cols = [
                    c
                    for c in inc_df.columns
                    if isinstance(c, str)
                    and len(c) == 10
                    and c[4] == "-"
                    and c[7] == "-"
                ]
                if period_cols:
                    col = period_cols[0]
                    op_rows = inc_df[
                        inc_df["label"].str.contains(
                            "Operating Income", case=False, na=False
                        )
                    ]
                    if not op_rows.empty:
                        operating_income = _to_decimal(op_rows.iloc[0][col])

                    rd_rows = inc_df[
                        inc_df["label"].str.contains(
                            r"Research|R&D", case=False, na=False
                        )
                    ]
                    if not rd_rows.empty:
                        rd_expense = _to_decimal(rd_rows.iloc[0][col])

        op_margin = _safe_divide(operating_income, revenue)
        rd_ratio = _safe_divide(rd_expense, revenue)
        return (company.name, op_margin, rd_ratio)
    except Exception:
        return (getattr(company, "name", None), None, None)


class PortersModule(AbstractModule):
    name = "porters"

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
                error_message="EDGAR_IDENTITY not set",
            )

        set_identity(settings.EDGAR_IDENTITY)

        capex: Decimal | None = None
        fcf: Decimal | None = None
        op_margin: Decimal | None = None
        rd_ratio: Decimal | None = None
        risk_factors: str | None = None
        mda: str | None = None
        competitors: list[CompetitorMetrics] = []
        err: str | None = None
        final_status = "success"

        try:
            company = Company(ticker.upper())

            # --- Capital requirements from cash flow ---
            try:
                financials = company.get_financials()
                if financials is not None:
                    capex = _to_decimal(financials.get_capital_expenditures())
                    fcf = _to_decimal(financials.get_free_cash_flow())

                    name, op_margin, rd_ratio = _get_company_margins(company)
            except Exception as e:
                _log.warning("porters_financials_error", error=str(e))

            # --- Risk Factors (Item 1A) & MD&A (Item 7) ---
            try:
                risk_factors = _truncate(
                    _extract_10k_section(company, "Item 1A")
                )
                mda = _truncate(_extract_10k_section(company, "Item 7"))
            except Exception as e:
                _log.warning("porters_10k_parse_error", error=str(e))

            # --- Competitor comparison via SIC code ---
            try:
                sic = getattr(company, "sic", None)
                if sic:
                    from edgar import find_companies

                    peers = find_companies(sic=sic)
                    peer_tickers: list[str] = []
                    for peer in peers:
                        pticker = getattr(peer, "tickers", None)
                        if pticker and isinstance(pticker, list) and pticker:
                            pticker_str = pticker[0]
                        else:
                            pticker_str = getattr(peer, "ticker", None)

                        if (
                            pticker_str
                            and pticker_str.upper() != ticker.upper()
                            and len(peer_tickers) < MAX_COMPETITORS
                        ):
                            peer_tickers.append(pticker_str)

                    for pt in peer_tickers[:MAX_COMPETITORS]:
                        try:
                            peer_company = Company(pt)
                            cname, c_op, c_rd = _get_company_margins(peer_company)
                            competitors.append(
                                CompetitorMetrics(
                                    ticker=pt.upper(),
                                    company_name=cname,
                                    operating_margin=c_op,
                                    rd_to_revenue_ratio=c_rd,
                                )
                            )
                        except Exception:
                            continue
            except Exception as e:
                _log.warning("porters_competitor_error", error=str(e))

        except Exception as e:
            _log.error("porters_module_error", ticker=ticker, error=str(e))
            final_status = "error"
            err = str(e)

        payload = PortersFiveForcesData(
            capex_usd=capex,
            free_cash_flow_usd=fcf,
            operating_margin=op_margin,
            rd_to_revenue_ratio=rd_ratio,
            risk_factors_excerpt=risk_factors,
            mda_excerpt=mda,
            competitor_metrics=competitors,
        )
        return ModuleResult(
            name=self.name,
            status=final_status if final_status == "success" else "error",
            started_at=now,
            completed_at=utcnow(),
            error_message=err,
            porters=payload,
        )
