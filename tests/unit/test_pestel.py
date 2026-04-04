from __future__ import annotations

from unittest.mock import MagicMock, patch

from modules.pestel import PESTELModule


@patch("modules.pestel.set_identity")
@patch("modules.pestel.Company")
def test_pestel_dimensional_revenue(
    mock_company: MagicMock,
    _mock_si: MagicMock,
) -> None:
    import pandas as pd

    detailed_df = pd.DataFrame(
        {
            "label": [
                "Revenue - Americas",
                "Revenue - Europe",
                "Product Revenue",
                "Service Revenue",
                "Total Revenue",
            ],
            "2025-09-27": [60.0e9, 30.0e9, 50.0e9, 40.0e9, 90.0e9],
        }
    )
    inc_stmt = MagicMock()
    inc_stmt.to_dataframe = MagicMock(return_value=detailed_df)

    mock_xbrl = MagicMock()
    mock_xbrl.statements.income_statement = MagicMock(return_value=inc_stmt)

    mock_filing = MagicMock()
    mock_filing.xbrl = MagicMock(return_value=mock_xbrl)
    mock_filing.filing_date = "2025-11-01"
    mock_filing.form = "10-K"

    mock_8k_filing = MagicMock()
    mock_8k_filing.filing_date = "2025-10-15"
    mock_8k_filing.form = "8-K"
    mock_8k_filing.description = "Quarterly results"
    mock_8k_parsed = MagicMock()
    mock_8k_parsed.items = ["Item 2.02 - Results of Operations"]
    mock_8k_filing.obj = MagicMock(return_value=mock_8k_parsed)

    mock_co = MagicMock()
    mock_co.name = "Test Corp"

    def _get_filings(form: str) -> MagicMock:
        f = MagicMock()
        if form == "10-K":
            f.latest = MagicMock(return_value=mock_filing)
            f.head = MagicMock(return_value=[mock_filing])
        elif form == "8-K":
            f.head = MagicMock(return_value=[mock_8k_filing])
            f.__iter__ = MagicMock(return_value=iter([mock_8k_filing]))
        elif form == "DEF 14A":
            f.head = MagicMock(return_value=[])
            f.__iter__ = MagicMock(return_value=iter([]))
        return f

    mock_co.get_filings = MagicMock(side_effect=_get_filings)
    mock_company.return_value = mock_co

    mod = PESTELModule()
    r = mod.run("TST")
    assert r.name == "pestel"
    assert r.status == "success"
    assert r.pestel is not None
    assert len(r.pestel.geographic_revenue) >= 2
    assert len(r.pestel.product_revenue) >= 2
    assert len(r.pestel.material_events) >= 1


@patch("modules.pestel.set_identity")
@patch("modules.pestel.Company")
def test_pestel_8k_events(
    mock_company: MagicMock,
    _mock_si: MagicMock,
) -> None:
    mock_co = MagicMock()
    mock_co.name = "Event Corp"

    mock_8k = MagicMock()
    mock_8k.filing_date = "2025-06-01"
    mock_8k.form = "8-K"
    mock_8k.description = "Material acquisition"
    mock_8k_parsed = MagicMock()
    mock_8k_parsed.items = ["Item 1.01 - Entry into Material Agreement"]
    mock_8k.obj = MagicMock(return_value=mock_8k_parsed)

    def _get_filings(form: str) -> MagicMock:
        f = MagicMock()
        if form == "8-K":
            f.head = MagicMock(return_value=[mock_8k])
            f.__iter__ = MagicMock(return_value=iter([mock_8k]))
        elif form == "10-K":
            f.latest = MagicMock(side_effect=Exception("no 10-K"))
            f.head = MagicMock(return_value=[])
        else:
            f.head = MagicMock(return_value=[])
            f.__iter__ = MagicMock(return_value=iter([]))
        return f

    mock_co.get_filings = MagicMock(side_effect=_get_filings)
    mock_company.return_value = mock_co

    mod = PESTELModule()
    r = mod.run("EVT")
    assert r.pestel is not None
    assert len(r.pestel.material_events) == 1
    assert "Material Agreement" in r.pestel.material_events[0].description


def test_pestel_errors_without_edgar_identity(monkeypatch: object) -> None:
    monkeypatch.setenv("EDGAR_IDENTITY", "   ")  # type: ignore[union-attr]
    mod = PESTELModule()
    assert mod.validate() is False
    r = mod.run("AAPL")
    assert r.status == "error"
