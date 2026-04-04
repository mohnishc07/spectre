from __future__ import annotations

from unittest.mock import MagicMock, patch

from modules.financial import FinancialModule


@patch("modules.financial.set_identity")
@patch("modules.financial.XBRLS")
@patch("modules.financial.Company")
def test_financial_success_multi_period(
    mock_company: MagicMock,
    mock_xbrls_cls: MagicMock,
    _mock_si: MagicMock,
) -> None:
    import pandas as pd

    inc_df = pd.DataFrame(
        {
            "label": ["Revenue", "Net Income"],
            "2025-09-27": [100.0e9, 20.0e9],
            "2024-09-28": [90.0e9, 18.0e9],
        }
    )
    bs_df = pd.DataFrame(
        {
            "label": ["Assets", "Stockholders Equity"],
            "2025-09-27": [300.0e9, 100.0e9],
            "2024-09-28": [280.0e9, 90.0e9],
        }
    )
    inc_stmt = MagicMock()
    inc_stmt.to_dataframe = MagicMock(return_value=inc_df)
    bs_stmt = MagicMock()
    bs_stmt.to_dataframe = MagicMock(return_value=bs_df)

    mock_stmts = MagicMock()
    mock_stmts.income_statement = MagicMock(return_value=inc_stmt)
    mock_stmts.balance_sheet = MagicMock(return_value=bs_stmt)

    mock_xbrls = MagicMock()
    mock_xbrls.statements = mock_stmts
    mock_xbrls_cls.from_filings = MagicMock(return_value=mock_xbrls)

    mock_co = MagicMock()
    mock_co.name = "Mock Co"
    mock_co.cik = "0000000000"

    mock_filings = MagicMock()
    mock_filings.filter = MagicMock(return_value=mock_filings)
    mock_filings.head = MagicMock(return_value=mock_filings)
    mock_co.get_filings = MagicMock(return_value=mock_filings)
    mock_company.return_value = mock_co

    mod = FinancialModule()
    r = mod.run("TST")
    assert r.name == "financial"
    assert r.status == "success"
    assert r.financial is not None
    assert r.financial.headline is not None
    assert r.financial.revenue_usd is not None
    assert len(r.financial.periods) == 2


@patch("modules.financial.set_identity")
@patch("modules.financial.XBRLS")
@patch("modules.financial.Company")
def test_financial_dupont_ratios(
    mock_company: MagicMock,
    mock_xbrls_cls: MagicMock,
    _mock_si: MagicMock,
) -> None:
    import pandas as pd

    inc_df = pd.DataFrame(
        {
            "label": ["Revenue", "Net Income"],
            "2025-09-27": [100.0, 25.0],
        }
    )
    bs_df = pd.DataFrame(
        {
            "label": ["Assets", "Stockholders Equity"],
            "2025-09-27": [200.0, 50.0],
        }
    )
    inc_stmt = MagicMock()
    inc_stmt.to_dataframe = MagicMock(return_value=inc_df)
    bs_stmt = MagicMock()
    bs_stmt.to_dataframe = MagicMock(return_value=bs_df)

    mock_stmts = MagicMock()
    mock_stmts.income_statement = MagicMock(return_value=inc_stmt)
    mock_stmts.balance_sheet = MagicMock(return_value=bs_stmt)

    mock_xbrls = MagicMock()
    mock_xbrls.statements = mock_stmts
    mock_xbrls_cls.from_filings = MagicMock(return_value=mock_xbrls)

    mock_co = MagicMock()
    mock_co.name = "Test"
    mock_co.cik = "1"
    mock_filings = MagicMock()
    mock_filings.filter = MagicMock(return_value=mock_filings)
    mock_filings.head = MagicMock(return_value=mock_filings)
    mock_co.get_filings = MagicMock(return_value=mock_filings)
    mock_company.return_value = mock_co

    mod = FinancialModule()
    r = mod.run("TST")
    assert r.status == "success"
    assert r.financial is not None
    p = r.financial.periods[0]
    assert p.profit_margin is not None
    assert p.asset_turnover is not None
    assert p.financial_leverage is not None
    assert p.roe is not None
    assert float(p.profit_margin) == 0.25
    assert float(p.asset_turnover) == 0.5
    assert float(p.financial_leverage) == 4.0
    assert float(p.roe) == 0.5


def test_financial_errors_without_edgar_identity(monkeypatch: object) -> None:
    monkeypatch.setenv("EDGAR_IDENTITY", "   ")  # type: ignore[union-attr]
    mod = FinancialModule()
    assert mod.validate() is False
    r = mod.run("AAPL")
    assert r.status == "error"
    assert r.error_message is not None
