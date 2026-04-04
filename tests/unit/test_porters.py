from __future__ import annotations

from unittest.mock import MagicMock, patch

from modules.porters import PortersModule


@patch("modules.porters.set_identity")
@patch("modules.porters.Company")
def test_porters_success_with_financials(
    mock_company: MagicMock,
    _mock_si: MagicMock,
) -> None:
    import pandas as pd

    mock_co = MagicMock()
    mock_co.name = "Test Corp"
    mock_co.sic = None

    mock_financials = MagicMock()
    mock_financials.get_capital_expenditures = MagicMock(return_value=-5.0e9)
    mock_financials.get_free_cash_flow = MagicMock(return_value=15.0e9)
    mock_financials.get_revenue = MagicMock(return_value=100.0e9)
    mock_financials.get_net_income = MagicMock(return_value=20.0e9)

    inc_df = pd.DataFrame(
        {
            "label": [
                "Operating Income",
                "Research and Development",
            ],
            "2025-09-27": [30.0e9, 10.0e9],
        }
    )
    inc_stmt = MagicMock()
    inc_stmt.to_dataframe = MagicMock(return_value=inc_df)
    mock_financials.income_statement = MagicMock(return_value=inc_stmt)

    mock_co.get_financials = MagicMock(return_value=mock_financials)

    mock_tenk = MagicMock()
    mock_tenk.__getitem__ = MagicMock(
        side_effect=lambda k: "Risk text" if "1A" in k else "MDA text"
    )
    mock_filing = MagicMock()
    mock_filing.obj = MagicMock(return_value=mock_tenk)
    mock_filings = MagicMock()
    mock_filings.latest = MagicMock(return_value=mock_filing)
    mock_co.get_filings = MagicMock(return_value=mock_filings)

    mock_company.return_value = mock_co

    mod = PortersModule()
    r = mod.run("TST")
    assert r.name == "porters"
    assert r.status == "success"
    assert r.porters is not None
    assert r.porters.capex_usd is not None
    assert r.porters.free_cash_flow_usd is not None
    assert r.porters.risk_factors_excerpt is not None
    assert r.porters.mda_excerpt is not None


@patch("modules.porters.set_identity")
@patch("modules.porters.Company")
def test_porters_text_truncation(
    mock_company: MagicMock,
    _mock_si: MagicMock,
) -> None:
    mock_co = MagicMock()
    mock_co.name = "Big Corp"
    mock_co.sic = None
    mock_co.get_financials = MagicMock(return_value=None)

    long_text = "A" * 10000
    mock_tenk = MagicMock()
    mock_tenk.__getitem__ = MagicMock(return_value=long_text)
    mock_filing = MagicMock()
    mock_filing.obj = MagicMock(return_value=mock_tenk)
    mock_filings = MagicMock()
    mock_filings.latest = MagicMock(return_value=mock_filing)
    mock_co.get_filings = MagicMock(return_value=mock_filings)
    mock_company.return_value = mock_co

    mod = PortersModule()
    r = mod.run("BIG")
    assert r.porters is not None
    if r.porters.risk_factors_excerpt:
        assert len(r.porters.risk_factors_excerpt) <= 5020


def test_porters_errors_without_edgar_identity(monkeypatch: object) -> None:
    monkeypatch.setenv("EDGAR_IDENTITY", "   ")  # type: ignore[union-attr]
    mod = PortersModule()
    assert mod.validate() is False
    r = mod.run("AAPL")
    assert r.status == "error"
