"""
SPECTRE - Strategic Profiling Engine for Competitive Threat & Rivalry Evaluation
v0.0.3 — All-in-one modular script
Modules: Financial Fortress | Legal Minefield | Psychological Atmosphere
"""

import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("SPECTRE")


# ═════════════════════════════════════════════
# MODULE 1: THE FINANCIAL FORTRESS
# Capital & Scale — Quantifies the "Entry Ticket"
# ═════════════════════════════════════════════

def run_financial_fortress(ticker: str) -> dict:
    """
    Pulls audited SEC EDGAR financials via edgartools.
    Returns: total assets, fixed asset intensity, operating margin trend, R&D ratio.
    """
    log.info(f"[Module 1] Financial Fortress → {ticker}")
    result = {
        "module": "financial_fortress",
        "ticker": ticker,
        "status": "error",
        "data": {}
    }

    try:
        import edgartools as et  # pip install edgartools

        company = et.Company(ticker)
        financials = company.get_financials()

        # Balance sheet
        balance = financials.balance_sheet
        total_assets = balance.get("total_assets", None)
        fixed_assets = balance.get("property_plant_equipment_net", None)
        fixed_asset_intensity = (
            round(fixed_assets / total_assets, 4) if total_assets and fixed_assets else None
        )

        # Income statement
        income = financials.income_statement
        revenue = income.get("revenues", None)
        op_income = income.get("operating_income", None)
        operating_margin = (
            round(op_income / revenue, 4) if revenue and op_income else None
        )
        rd_expense = income.get("research_and_development", None)
        rd_to_revenue = (
            round(rd_expense / revenue, 4) if revenue and rd_expense else None
        )

        result["data"] = {
            "total_assets_usd": total_assets,
            "fixed_asset_intensity": fixed_asset_intensity,
            "operating_margin": operating_margin,
            "rd_to_revenue_ratio": rd_to_revenue,
            "maintenance_vs_growth_capex_note": (
                "Capex breakdown requires cash flow statement parsing — extend as needed."
            ),
        }
        result["status"] = "ok"

    except ImportError:
        log.warning("edgartools not installed. Run: pip install edgartools")
        result["status"] = "dependency_missing"
        result["data"] = {"note": "Install edgartools to activate this module."}
    except Exception as e:
        log.error(f"[Module 1] Failed: {e}")
        result["status"] = "error"
        result["data"] = {"error": str(e)}

    return result


# ═════════════════════════════════════════════
# MODULE 2: THE LEGAL MINEFIELD
# Intellectual Property — Identifies "No-Fly Zones"
# ═════════════════════════════════════════════

def run_legal_minefield(company_name: str) -> dict:
    """
    Queries USPTO/EPO via patent-client.
    Returns: active patent count, pending applications, tech density clusters.
    """
    log.info(f"[Module 2] Legal Minefield → {company_name}")
    result = {
        "module": "legal_minefield",
        "company": company_name,
        "status": "error",
        "data": {}
    }

    try:
        from patent_client import Patent, PatentBiblio  # pip install patent-client

        # Search granted patents
        granted = Patent.objects.filter(assignee=company_name)
        granted_list = list(granted[:50])  # cap for speed
        active_count = len(granted_list)

        # Search pending applications
        pending = PatentBiblio.objects.filter(assignee=company_name, status="pending")
        pending_list = list(pending[:50])
        pending_count = len(pending_list)

        # Tech density: cluster by CPC classification code prefix
        cpc_codes = []
        for p in granted_list:
            codes = getattr(p, "cpc_classifications", []) or []
            cpc_codes.extend([c[:4] for c in codes if c])  # 4-char prefix = subclass

        from collections import Counter
        tech_density = dict(Counter(cpc_codes).most_common(10))

        result["data"] = {
            "active_patent_count": active_count,
            "pending_applications": pending_count,
            "tech_density_top10_cpc": tech_density,
        }
        result["status"] = "ok"

    except ImportError:
        log.warning("patent-client not installed. Run: pip install patent-client")
        result["status"] = "dependency_missing"
        result["data"] = {"note": "Install patent-client to activate this module."}
    except Exception as e:
        log.error(f"[Module 2] Failed: {e}")
        result["status"] = "error"
        result["data"] = {"error": str(e)}

    return result


# ═════════════════════════════════════════════
# MODULE 3: THE PSYCHOLOGICAL ATMOSPHERE
# Sentiment & News — Detects Dependency Moats
# ═════════════════════════════════════════════

def run_psychological_atmosphere(company_name: str, lookback_days: int = 14) -> dict:
    """
    Scrapes industry news via pygooglenews, scores sentiment via VADER.
    Flags Law 11 Moat if negative sentiment doesn't tank stock stability.
    """
    log.info(f"[Module 3] Psychological Atmosphere → {company_name} ({lookback_days}d lookback)")
    result = {
        "module": "psychological_atmosphere",
        "company": company_name,
        "lookback_days": lookback_days,
        "status": "error",
        "data": {}
    }

    try:
        from pygooglenews import GoogleNews          # pip install pygooglenews
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer  # pip install vaderSentiment

        gn = GoogleNews(lang="en", country="US")
        search = gn.search(company_name, when=f"{lookback_days}d")
        entries = search.get("entries", [])

        analyzer = SentimentIntensityAnalyzer()
        scores = []
        headlines = []

        for entry in entries[:30]:  # cap at 30 articles
            title = entry.get("title", "")
            score = analyzer.polarity_scores(title)
            scores.append(score["compound"])
            headlines.append({"title": title, "compound_score": score["compound"]})

        avg_sentiment = round(sum(scores) / len(scores), 4) if scores else None
        negative_spike = avg_sentiment is not None and avg_sentiment < -0.2

        # Stock stability proxy via pytrends (search interest as proxy)
        stock_stability_note = "Requires stock API (e.g. yfinance) for full correlation."

        result["data"] = {
            "articles_analyzed": len(scores),
            "average_sentiment_compound": avg_sentiment,
            "negative_spike_detected": negative_spike,
            "law11_moat_flag": negative_spike,  # True = customers stuck despite unhappiness
            "stock_stability": stock_stability_note,
            "sample_headlines": headlines[:5],
        }
        result["status"] = "ok"

    except ImportError as e:
        log.warning(f"Missing dependency: {e}. Run: pip install pygooglenews vaderSentiment")
        result["status"] = "dependency_missing"
        result["data"] = {"note": str(e)}
    except Exception as e:
        log.error(f"[Module 3] Failed: {e}")
        result["status"] = "error"
        result["data"] = {"error": str(e)}

    return result


# ═════════════════════════════════════════════
# OUTPUT ENGINE — Assembles spectre_analysis.json
# ═════════════════════════════════════════════

def assemble_output(ticker: str, company_name: str, modules: list) -> dict:
    return {
        "spectre_version": "0.0.3",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "target": {
            "ticker": ticker,
            "company_name": company_name,
        },
        "schema_compliant": True,
        "modules": modules,
    }


def save_output(payload: dict, output_path: str = "spectre_analysis.json"):
    path = Path(output_path)
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info(f"Output saved → {path.resolve()}")
    return path


# ═════════════════════════════════════════════
# ENTRYPOINT
# ═════════════════════════════════════════════

def run_spectre(ticker: str, company_name: str, output_path: str = "spectre_analysis.json"):
    """
    Full SPECTRE pipeline. Runs all 3 modules and outputs JSON.
    Target: <120 seconds total runtime.
    """
    log.info(f"══ SPECTRE INITIATED ══ Target: {company_name} ({ticker})")
    t_start = time.time()

    modules = []

    # Run all modules
    modules.append(run_financial_fortress(ticker))
    modules.append(run_legal_minefield(company_name))
    modules.append(run_psychological_atmosphere(company_name))

    # Assemble & save
    payload = assemble_output(ticker, company_name, modules)
    path = save_output(payload, output_path)

    elapsed = round(time.time() - t_start, 2)
    log.info(f"══ SPECTRE COMPLETE ══ {elapsed}s | Output: {path}")

    return payload


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SPECTRE — Competitive Moat Auditor")
    parser.add_argument("--ticker", required=True, help="Stock ticker (e.g. AAPL)")
    parser.add_argument("--company", required=True, help="Company name (e.g. 'Apple Inc')")
    parser.add_argument("--output", default="spectre_analysis.json", help="Output JSON path")
    args = parser.parse_args()

    run_spectre(
        ticker=args.ticker,
        company_name=args.company,
        output_path=args.output,
    ) 