from __future__ import annotations

import math
from decimal import Decimal

from edgar import Company, set_identity

from config.settings import get_settings
from core.schema import (
    MaterialEvent,
    ModuleResult,
    PESTELData,
    RevenueBreakdown,
    utcnow,
)
from modules.base import AbstractModule
from utils.logger import get_logger

_log = get_logger(__name__)

MAX_8K_FILINGS = 10
MAX_TEXT_CHARS = 5000


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


def _truncate(text: str | None, limit: int = MAX_TEXT_CHARS) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit] + "... [truncated]"


def _extract_dimensional_revenue(
    company: Company,
) -> tuple[list[RevenueBreakdown], list[RevenueBreakdown]]:
    """Extract geographic and product revenue from dimensional XBRL data."""
    geographic: list[RevenueBreakdown] = []
    product: list[RevenueBreakdown] = []

    try:
        filing = company.get_filings(form="10-K").latest()
        xbrl = filing.xbrl()
        income = xbrl.statements.income_statement()
        df = income.to_dataframe(view="detailed")

        if "label" not in df.columns:
            return geographic, product

        period_cols = [
            c
            for c in df.columns
            if isinstance(c, str) and len(c) == 10 and c[4] == "-" and c[7] == "-"
        ]
        if not period_cols:
            return geographic, product

        col = period_cols[0]

        geo_keywords = (
            "americas",
            "europe",
            "asia",
            "china",
            "japan",
            "united states",
            "international",
            "greater china",
            "rest of",
            "emea",
            "apac",
            "latin",
            "canada",
            "india",
        )
        product_keywords = (
            "product",
            "service",
            "iphone",
            "mac",
            "ipad",
            "wearable",
            "cloud",
            "office",
            "windows",
            "gaming",
            "linkedin",
            "advertising",
            "subscription",
            "hardware",
            "software",
            "intelligent cloud",
            "personal computing",
            "productivity",
            "aws",
        )

        revenue_rows = df[
            df["label"].str.contains(
                r"Revenue|Sales|Net sales", case=False, na=False
            )
        ]

        for _, row in revenue_rows.iterrows():
            label = str(row["label"]).strip()
            val = _to_decimal(row.get(col))
            if val is None:
                continue

            label_lower = label.lower()
            if any(kw in label_lower for kw in geo_keywords):
                geographic.append(RevenueBreakdown(segment=label, revenue_usd=val))
            elif any(kw in label_lower for kw in product_keywords):
                product.append(RevenueBreakdown(segment=label, revenue_usd=val))

    except Exception as e:
        _log.warning("dimensional_revenue_error", error=str(e))

    return geographic, product


def _extract_8k_events(company: Company) -> list[MaterialEvent]:
    events: list[MaterialEvent] = []
    try:
        filings = company.get_filings(form="8-K").head(MAX_8K_FILINGS)
        for filing in filings:
            try:
                parsed = filing.obj()
                description = ""

                if parsed is not None:
                    items = getattr(parsed, "items", None)
                    if items and isinstance(items, list):
                        description = "; ".join(str(item) for item in items)
                    elif items and isinstance(items, str):
                        description = items
                    elif hasattr(parsed, "header"):
                        description = str(getattr(parsed.header, "items", ""))

                if not description:
                    description = getattr(filing, "description", "") or str(
                        filing.form
                    )

                events.append(
                    MaterialEvent(
                        filed_date=str(filing.filing_date),
                        form_type=str(filing.form),
                        description=description[:1000],
                    )
                )
            except Exception:
                events.append(
                    MaterialEvent(
                        filed_date=str(filing.filing_date),
                        form_type=str(filing.form),
                        description=getattr(filing, "description", "8-K filing"),
                    )
                )
    except Exception as e:
        _log.warning("8k_extraction_error", error=str(e))
    return events


def _extract_esg_governance(company: Company) -> str | None:
    """Extract ESG-related text from 10-K Item 1 (Business) and proxy statement."""
    parts: list[str] = []

    try:
        filing = company.get_filings(form="10-K").latest()
        tenk = filing.obj()
        if tenk is not None:
            for item_key in ("Item 1", "Item 1A"):
                section = tenk[item_key]
                if section is not None:
                    text = str(section)
                    esg_terms = (
                        "environmental",
                        "sustainability",
                        "climate",
                        "carbon",
                        "social responsibility",
                        "diversity",
                        "governance",
                        "esg",
                        "human capital",
                    )
                    paragraphs = text.split("\n")
                    for para in paragraphs:
                        if any(term in para.lower() for term in esg_terms):
                            parts.append(para.strip())
    except Exception as e:
        _log.warning("esg_10k_error", error=str(e))

    try:
        proxy_filings = company.get_filings(form="DEF 14A").head(1)
        for filing in proxy_filings:
            parsed = filing.obj()
            if parsed is not None:
                proxy_text = str(parsed)
                esg_terms = (
                    "environmental",
                    "sustainability",
                    "corporate governance",
                    "board diversity",
                )
                for para in proxy_text.split("\n"):
                    if any(term in para.lower() for term in esg_terms):
                        parts.append(para.strip())
    except Exception as e:
        _log.warning("esg_proxy_error", error=str(e))

    if not parts:
        return None
    return _truncate("\n".join(parts))


class PESTELModule(AbstractModule):
    name = "pestel"

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

        geographic: list[RevenueBreakdown] = []
        product_rev: list[RevenueBreakdown] = []
        events: list[MaterialEvent] = []
        esg: str | None = None
        err: str | None = None
        final_status = "success"

        try:
            company = Company(ticker.upper())

            # --- Dimensional revenue breakdown ---
            try:
                geographic, product_rev = _extract_dimensional_revenue(company)
            except Exception as e:
                _log.warning("pestel_revenue_error", error=str(e))

            # --- 8-K material events ---
            try:
                events = _extract_8k_events(company)
            except Exception as e:
                _log.warning("pestel_8k_error", error=str(e))

            # --- ESG / Governance ---
            try:
                esg = _extract_esg_governance(company)
            except Exception as e:
                _log.warning("pestel_esg_error", error=str(e))

        except Exception as e:
            _log.error("pestel_module_error", ticker=ticker, error=str(e))
            final_status = "error"
            err = str(e)

        payload = PESTELData(
            geographic_revenue=geographic,
            product_revenue=product_rev,
            material_events=events,
            esg_governance_excerpt=esg,
        )
        return ModuleResult(
            name=self.name,
            status=final_status if final_status == "success" else "error",
            started_at=now,
            completed_at=utcnow(),
            error_message=err,
            pestel=payload,
        )
