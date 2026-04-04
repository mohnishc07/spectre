from __future__ import annotations

from unittest.mock import MagicMock, patch

from modules.vrio import VRIOModule


@patch("modules.vrio.set_identity")
@patch("modules.vrio.Company")
def test_vrio_balance_sheet_extraction(
    mock_company: MagicMock,
    _mock_si: MagicMock,
) -> None:
    import pandas as pd

    bs_df = pd.DataFrame(
        {
            "label": [
                "Intangible Assets, Net",
                "Property, Plant and Equipment, Net",
                "Total Assets",
            ],
            "2025-09-27": [50.0e9, 40.0e9, 300.0e9],
        }
    )
    bs_stmt = MagicMock()
    bs_stmt.to_dataframe = MagicMock(return_value=bs_df)
    mock_financials = MagicMock()
    mock_financials.balance_sheet = MagicMock(return_value=bs_stmt)

    mock_co = MagicMock()
    mock_co.name = "Test Corp"
    mock_co.get_financials = MagicMock(return_value=mock_financials)

    empty_filings = MagicMock()
    empty_filings.head = MagicMock(return_value=[])
    empty_filings.__iter__ = MagicMock(return_value=iter([]))
    mock_co.get_filings = MagicMock(return_value=empty_filings)
    mock_company.return_value = mock_co

    mod = VRIOModule()
    r = mod.run("TST")
    assert r.name == "vrio"
    assert r.status == "success"
    assert r.vrio is not None
    assert r.vrio.intangible_assets_usd is not None
    assert r.vrio.ppe_usd is not None
    assert float(r.vrio.intangible_assets_usd) == 50.0e9
    assert float(r.vrio.ppe_usd) == 40.0e9


@patch("modules.vrio.set_identity")
@patch("modules.vrio.Company")
def test_vrio_insider_transactions(
    mock_company: MagicMock,
    _mock_si: MagicMock,
) -> None:
    mock_co = MagicMock()
    mock_co.name = "Test Corp"
    mock_co.get_financials = MagicMock(return_value=None)

    mock_owner = MagicMock()
    mock_owner.name = "Jane Doe"
    mock_owner.officer_title = "CFO"

    mock_txn = MagicMock()
    mock_txn.transaction_code = "P"
    mock_txn.transaction_shares = 1000
    mock_txn.transaction_price_per_share = 150.0

    mock_parsed = MagicMock()
    mock_parsed.reporting_owner = mock_owner
    mock_parsed.transactions = None
    mock_parsed.non_derivative_transactions = [mock_txn]
    mock_parsed.derivative_transactions = []

    mock_filing = MagicMock()
    mock_filing.filing_date = "2025-03-15"
    mock_filing.obj = MagicMock(return_value=mock_parsed)

    def _get_filings(form: str) -> MagicMock:
        f = MagicMock()
        if form in ("4", "3", "5"):
            f.head = MagicMock(return_value=[mock_filing])
            f.__iter__ = MagicMock(return_value=iter([mock_filing]))
        else:
            f.head = MagicMock(return_value=[])
            f.__iter__ = MagicMock(return_value=iter([]))
        return f

    mock_co.get_filings = MagicMock(side_effect=_get_filings)
    mock_company.return_value = mock_co

    mod = VRIOModule()
    r = mod.run("TST")
    assert r.vrio is not None
    assert len(r.vrio.insider_transactions) > 0
    txn = r.vrio.insider_transactions[0]
    assert txn.insider_name == "Jane Doe"
    assert txn.transaction_type == "Purchase"
    assert txn.shares is not None


def test_vrio_errors_without_edgar_identity(monkeypatch: object) -> None:
    monkeypatch.setenv("EDGAR_IDENTITY", "   ")  # type: ignore[union-attr]
    mod = VRIOModule()
    assert mod.validate() is False
    r = mod.run("AAPL")
    assert r.status == "error"
