from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, model_validator

ModuleStatus = Literal["success", "error", "not_implemented"]

_DECIMAL_FIELDS_SENTINEL = object()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_decimal(v: Decimal | None) -> str | None:
    if v is None:
        return None
    return format(v, "f")


# ---------------------------------------------------------------------------
# Financial Fortress
# ---------------------------------------------------------------------------


class PeriodMetrics(BaseModel):
    model_config = ConfigDict(strict=True)

    period: str
    revenue_usd: Decimal | None = None
    net_income_usd: Decimal | None = None
    total_assets_usd: Decimal | None = None
    total_equity_usd: Decimal | None = None
    profit_margin: Decimal | None = None
    asset_turnover: Decimal | None = None
    financial_leverage: Decimal | None = None
    roe: Decimal | None = None

    @field_serializer(
        "revenue_usd",
        "net_income_usd",
        "total_assets_usd",
        "total_equity_usd",
        "profit_margin",
        "asset_turnover",
        "financial_leverage",
        "roe",
        when_used="json",
    )
    def serialize_decimal(self, v: Decimal | None) -> str | None:
        return _serialize_decimal(v)


class FinancialFortressData(BaseModel):
    model_config = ConfigDict(strict=True)

    headline: str | None = None
    revenue_usd: Decimal | None = None
    total_assets_usd: Decimal | None = None
    periods: list[PeriodMetrics] = Field(default_factory=list)

    @field_serializer("revenue_usd", "total_assets_usd", when_used="json")
    def serialize_decimal(self, v: Decimal | None) -> str | None:
        return _serialize_decimal(v)


# ---------------------------------------------------------------------------
# IP / Legal
# ---------------------------------------------------------------------------


class IPMineFieldData(BaseModel):
    model_config = ConfigDict(strict=True)

    patent_count: int | None = None
    notes: str | None = None


# ---------------------------------------------------------------------------
# Sentiment
# ---------------------------------------------------------------------------


class SentimentData(BaseModel):
    model_config = ConfigDict(strict=True)

    compound_score: Decimal | None = None
    sample_size: int | None = None

    @field_serializer("compound_score", when_used="json")
    def serialize_decimal(self, v: Decimal | None) -> str | None:
        return _serialize_decimal(v)


# ---------------------------------------------------------------------------
# Porter's Five Forces
# ---------------------------------------------------------------------------


class CompetitorMetrics(BaseModel):
    model_config = ConfigDict(strict=True)

    ticker: str
    company_name: str | None = None
    operating_margin: Decimal | None = None
    rd_to_revenue_ratio: Decimal | None = None

    @field_serializer("operating_margin", "rd_to_revenue_ratio", when_used="json")
    def serialize_decimal(self, v: Decimal | None) -> str | None:
        return _serialize_decimal(v)


class PortersFiveForcesData(BaseModel):
    model_config = ConfigDict(strict=True)

    capex_usd: Decimal | None = None
    free_cash_flow_usd: Decimal | None = None
    operating_margin: Decimal | None = None
    rd_to_revenue_ratio: Decimal | None = None
    risk_factors_excerpt: str | None = None
    mda_excerpt: str | None = None
    competitor_metrics: list[CompetitorMetrics] = Field(default_factory=list)

    @field_serializer(
        "capex_usd",
        "free_cash_flow_usd",
        "operating_margin",
        "rd_to_revenue_ratio",
        when_used="json",
    )
    def serialize_decimal(self, v: Decimal | None) -> str | None:
        return _serialize_decimal(v)


# ---------------------------------------------------------------------------
# VRIO Framework
# ---------------------------------------------------------------------------


class InsiderTransaction(BaseModel):
    model_config = ConfigDict(strict=True)

    filed_date: str
    insider_name: str
    title: str | None = None
    transaction_type: str
    shares: Decimal | None = None
    price_per_share: Decimal | None = None

    @field_serializer("shares", "price_per_share", when_used="json")
    def serialize_decimal(self, v: Decimal | None) -> str | None:
        return _serialize_decimal(v)


class ExecutiveCompensation(BaseModel):
    model_config = ConfigDict(strict=True)

    year: str
    name: str
    title: str | None = None
    total_compensation_usd: Decimal | None = None

    @field_serializer("total_compensation_usd", when_used="json")
    def serialize_decimal(self, v: Decimal | None) -> str | None:
        return _serialize_decimal(v)


class VRIOData(BaseModel):
    model_config = ConfigDict(strict=True)

    intangible_assets_usd: Decimal | None = None
    ppe_usd: Decimal | None = None
    executive_compensation: list[ExecutiveCompensation] = Field(default_factory=list)
    insider_transactions: list[InsiderTransaction] = Field(default_factory=list)

    @field_serializer("intangible_assets_usd", "ppe_usd", when_used="json")
    def serialize_decimal(self, v: Decimal | None) -> str | None:
        return _serialize_decimal(v)


# ---------------------------------------------------------------------------
# PESTEL Model
# ---------------------------------------------------------------------------


class RevenueBreakdown(BaseModel):
    model_config = ConfigDict(strict=True)

    segment: str
    revenue_usd: Decimal | None = None

    @field_serializer("revenue_usd", when_used="json")
    def serialize_decimal(self, v: Decimal | None) -> str | None:
        return _serialize_decimal(v)


class MaterialEvent(BaseModel):
    model_config = ConfigDict(strict=True)

    filed_date: str
    form_type: str
    description: str


class PESTELData(BaseModel):
    model_config = ConfigDict(strict=True)

    geographic_revenue: list[RevenueBreakdown] = Field(default_factory=list)
    product_revenue: list[RevenueBreakdown] = Field(default_factory=list)
    material_events: list[MaterialEvent] = Field(default_factory=list)
    esg_governance_excerpt: str | None = None


# ---------------------------------------------------------------------------
# Module Result & Top-Level
# ---------------------------------------------------------------------------


class ModuleResult(BaseModel):
    """Result for one analysis module; tolerates sparse arq error payloads."""

    model_config = ConfigDict(extra="ignore", strict=False)

    name: str
    status: ModuleStatus | str
    started_at: datetime = Field(default_factory=utcnow)
    completed_at: datetime = Field(default_factory=utcnow)
    error_message: str | None = None
    financial: FinancialFortressData | None = None
    ip: IPMineFieldData | None = None
    sentiment: SentimentData | None = None
    porters: PortersFiveForcesData | None = None
    vrio: VRIOData | None = None
    pestel: PESTELData | None = None

    @model_validator(mode="before")
    @classmethod
    def fill_sparse_error(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if data.get("status") == "error" and "name" not in data:
            data = {**data, "name": "unknown"}
        if "started_at" not in data:
            data = {**data, "started_at": utcnow()}
        if "completed_at" not in data:
            data = {**data, "completed_at": utcnow()}
        return data


class SpectreResult(BaseModel):
    model_config = ConfigDict(strict=True)

    ticker: str
    generated_at: datetime = Field(default_factory=utcnow)
    modules: list[ModuleResult]
