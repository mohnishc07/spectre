from __future__ import annotations

import math
from decimal import Decimal

from edgar import Company, set_identity

from config.settings import get_settings
from core.schema import (
    ExecutiveCompensation,
    InsiderTransaction,
    ModuleResult,
    VRIOData,
    utcnow,
)
from modules.base import AbstractModule
from utils.logger import get_logger

_log = get_logger(__name__)

MAX_INSIDER_FILINGS = 20
MAX_PROXY_FILINGS = 5


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


def _find_balance_sheet_item(
    company: Company, label_pattern: str
) -> Decimal | None:
    """Extract a balance-sheet line item by label pattern."""
    try:
        financials = company.get_financials()
        if financials is None:
            return None
        bs = financials.balance_sheet()
        if bs is None:
            return None
        bs_df = bs.to_dataframe(view="summary")
        if "label" not in bs_df.columns:
            return None

        period_cols = [
            c
            for c in bs_df.columns
            if isinstance(c, str) and len(c) == 10 and c[4] == "-" and c[7] == "-"
        ]
        if not period_cols:
            return None

        col = period_cols[0]
        matches = bs_df[
            bs_df["label"].str.contains(label_pattern, case=False, na=False)
        ]
        if matches.empty:
            return None
        return _to_decimal(matches.iloc[0][col])
    except Exception:
        return None


def _extract_insider_transactions(
    company: Company,
) -> list[InsiderTransaction]:
    results: list[InsiderTransaction] = []
    try:
        for form_type in ("4", "3", "5"):
            filings = company.get_filings(form=form_type).head(MAX_INSIDER_FILINGS)
            for filing in filings:
                try:
                    parsed = filing.obj()
                    if parsed is None:
                        continue

                    owner_name = "Unknown"
                    owner_title: str | None = None
                    if hasattr(parsed, "reporting_owner"):
                        owner = parsed.reporting_owner
                        if hasattr(owner, "name"):
                            owner_name = str(owner.name)
                        if hasattr(owner, "officer_title"):
                            owner_title = (
                                str(owner.officer_title)
                                if owner.officer_title
                                else None
                            )

                    transactions = getattr(parsed, "transactions", None)
                    if transactions is None:
                        transactions = getattr(parsed, "derivative_transactions", [])
                        non_deriv = getattr(parsed, "non_derivative_transactions", [])
                        if non_deriv:
                            transactions = list(non_deriv) + list(transactions or [])

                    if not transactions:
                        continue

                    for txn in transactions:
                        txn_type = "Unknown"
                        if hasattr(txn, "transaction_code"):
                            code = str(txn.transaction_code)
                            txn_type = {
                                "P": "Purchase",
                                "S": "Sale",
                                "A": "Grant/Award",
                                "M": "Option Exercise",
                                "C": "Conversion",
                                "G": "Gift",
                            }.get(code, code)

                        shares = _to_decimal(
                            getattr(txn, "transaction_shares", None)
                            or getattr(txn, "shares", None)
                        )
                        price = _to_decimal(
                            getattr(txn, "transaction_price_per_share", None)
                            or getattr(txn, "price_per_share", None)
                        )

                        results.append(
                            InsiderTransaction(
                                filed_date=str(filing.filing_date),
                                insider_name=owner_name,
                                title=owner_title,
                                transaction_type=txn_type,
                                shares=shares,
                                price_per_share=price,
                            )
                        )
                except Exception:
                    continue
    except Exception as e:
        _log.warning("insider_txn_extraction_error", error=str(e))
    return results


def _extract_executive_compensation(
    company: Company,
) -> list[ExecutiveCompensation]:
    results: list[ExecutiveCompensation] = []
    try:
        filings = company.get_filings(form="DEF 14A").head(MAX_PROXY_FILINGS)
        for filing in filings:
            try:
                parsed = filing.obj()
                if parsed is None:
                    continue

                year = str(filing.filing_date)[:4]

                comp_data = None
                for attr in (
                    "executive_compensation",
                    "compensation",
                    "summary_compensation",
                ):
                    comp_data = getattr(parsed, attr, None)
                    if comp_data is not None:
                        break

                if comp_data is None:
                    continue

                if hasattr(comp_data, "iterrows"):
                    for _, row in comp_data.iterrows():
                        name = str(row.get("Name", row.get("name", "Unknown")))
                        title = row.get("Title", row.get("title", None))
                        total = _to_decimal(
                            row.get("Total", row.get("total", None))
                        )
                        results.append(
                            ExecutiveCompensation(
                                year=year,
                                name=name,
                                title=str(title) if title else None,
                                total_compensation_usd=total,
                            )
                        )
                elif isinstance(comp_data, list):
                    for entry in comp_data:
                        if isinstance(entry, dict):
                            results.append(
                                ExecutiveCompensation(
                                    year=year,
                                    name=str(entry.get("name", "Unknown")),
                                    title=entry.get("title"),
                                    total_compensation_usd=_to_decimal(
                                        entry.get("total")
                                    ),
                                )
                            )
            except Exception:
                continue
    except Exception as e:
        _log.warning("exec_comp_extraction_error", error=str(e))
    return results


class VRIOModule(AbstractModule):
    name = "vrio"

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

        intangible_assets: Decimal | None = None
        ppe: Decimal | None = None
        exec_comp: list[ExecutiveCompensation] = []
        insider_txns: list[InsiderTransaction] = []
        err: str | None = None
        final_status = "success"

        try:
            company = Company(ticker.upper())

            # --- Balance sheet: intangible assets & PP&E ---
            try:
                intangible_assets = _find_balance_sheet_item(
                    company, r"Intangible"
                )
                ppe = _find_balance_sheet_item(
                    company, r"Property.*Plant|PP&E"
                )
            except Exception as e:
                _log.warning("vrio_balance_sheet_error", error=str(e))

            # --- DEF 14A: Executive compensation ---
            try:
                exec_comp = _extract_executive_compensation(company)
            except Exception as e:
                _log.warning("vrio_exec_comp_error", error=str(e))

            # --- Form 3/4/5: Insider transactions ---
            try:
                insider_txns = _extract_insider_transactions(company)
            except Exception as e:
                _log.warning("vrio_insider_error", error=str(e))

        except Exception as e:
            _log.error("vrio_module_error", ticker=ticker, error=str(e))
            final_status = "error"
            err = str(e)

        payload = VRIOData(
            intangible_assets_usd=intangible_assets,
            ppe_usd=ppe,
            executive_compensation=exec_comp,
            insider_transactions=insider_txns,
        )
        return ModuleResult(
            name=self.name,
            status=final_status if final_status == "success" else "error",
            started_at=now,
            completed_at=utcnow(),
            error_message=err,
            vrio=payload,
        )
