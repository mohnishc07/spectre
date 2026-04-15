"""
competitor_intel.py
────────────────────────────────────────────────────────────────
Competitor Intelligence Pipeline — 4 Extraction Functions
Crawls: Pricing | Messaging | Complaints | Investment Signals
No LLM. No API key. Pure Python + Crawl4AI.

Usage:
    pip install crawl4ai
    python competitor_intel.py
"""

import asyncio
import functools
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

# Optional: undetected adapter for hardened anti-bot sites (G2/DataDome, Cloudflare).
# If the import fails, the script still runs — it just falls back to the standard
# crawler for protected sites and you'll get more failures on those.
try:
    from crawl4ai import UndetectedAdapter
    from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
    UNDETECTED_AVAILABLE = True
except ImportError as _undetected_import_err:
    UndetectedAdapter = None
    AsyncPlaywrightCrawlerStrategy = None
    UNDETECTED_AVAILABLE = False
    print(
        f"⚠️  UndetectedAdapter not available ({_undetected_import_err}). "
        "Run `crawl4ai-setup` to install undetected-browser deps. "
        "Protected sites (G2 etc.) will fall back to standard crawler."
    )

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

# Hosts that require the undetected browser path. Routing matches by substring.
# Empty by default — add a host (e.g. "g2.com") if you hit hard anti-bot blocks
# and have already run `crawl4ai-setup` to install undetected-browser deps.
PROTECTED_HOSTS: set[str] = set()

# Master switch: set to False to disable the undetected path entirely (no visible
# browser windows will pop up). Protected sites will then always go stale.
USE_UNDETECTED_FOR_PROTECTED = True

# The undetected adapter is most effective with headless=False, per Crawl4AI docs:
# "Avoid Headless Mode: Detection is easier in headless mode". This means a
# real browser window will appear on your screen during scraping. Set to True
# if you don't care about effectiveness and just want it out of the way.
UNDETECTED_HEADLESS = False


# ═══════════════════════════════════════════════════════════════
# 1. PRICING EXTRACTION
# ═══════════════════════════════════════════════════════════════

def extract_pricing(markdown: str) -> dict:
    """
    Extracts pricing tiers, monthly/annual prices, pricing model, trial offers.

    Handles real-world price formats:
      $15/mo            $15/month         $15/user/mo
      $15 per month     $20 per seat      $890/month
      $3,600 monthly    $100/seat/month   $15/seat/mo

    Tier names are only counted if they appear in a heading or near a price,
    to avoid matching common words ("foundation of your business" etc).
    """
    text_lower = markdown.lower()

    # ── Price patterns ────────────────────────────────────────
    # Build three patterns and union their matches.
    # Group 1 in each pattern is normalized to "$X/mo"-style for storage.
    price_patterns_monthly = [
        # $15/mo, $15/month, $15/user/month, $15/seat/mo, $890/month
        r'\$[\d,]+(?:\.\d{2})?\s*/\s*(?:user/|seat/|member/)?(?:mo|month)\b',
        # $20 per month, $20 per user per month, $100 per seat per month
        r'\$[\d,]+(?:\.\d{2})?\s+per\s+(?:user\s+per\s+|seat\s+per\s+|member\s+per\s+)?(?:mo|month)\b',
        # $3,600 monthly, $890 monthly
        r'\$[\d,]+(?:\.\d{2})?\s+monthly\b',
    ]
    price_patterns_annual = [
        r'\$[\d,]+(?:\.\d{2})?\s*/\s*(?:user/|seat/|member/)?(?:yr|year)\b',
        r'\$[\d,]+(?:\.\d{2})?\s+per\s+(?:user\s+per\s+|seat\s+per\s+)?(?:yr|year|annum)\b',
        r'\$[\d,]+(?:\.\d{2})?\s+(?:annually|yearly|per\s+year)\b',
    ]

    monthly_prices_raw = []
    for pat in price_patterns_monthly:
        monthly_prices_raw += re.findall(pat, markdown, re.IGNORECASE)

    annual_prices_raw = []
    for pat in price_patterns_annual:
        annual_prices_raw += re.findall(pat, markdown, re.IGNORECASE)

    # Normalize: collapse whitespace, lowercase trailing unit
    def normalize_price(p: str) -> str:
        return re.sub(r'\s+', ' ', p.strip())

    monthly_prices = list(dict.fromkeys(normalize_price(p) for p in monthly_prices_raw))
    annual_prices  = list(dict.fromkeys(normalize_price(p) for p in annual_prices_raw))

    # ── Tier names (context-aware) ────────────────────────────
    # Only count a tier word if it appears in a heading line OR within
    # 80 chars of a $ price. This kills false positives like "the foundation
    # of your business" matching the "Foundation" tier word.
    tier_words = [
        "Free", "Starter", "Basic", "Essential",
        "Growth", "Standard", "Plus", "Professional", "Pro",
        "Business", "Scale", "Advanced", "Premium", "Elite",
        "Enterprise", "Ultimate", "Unlimited", "Max", "Team",
    ]
    tier_pattern = r'\b(' + '|'.join(tier_words) + r')\b'

    tiers_found = []
    seen_tiers = set()

    # Pass 1: tier words inside markdown headings
    for line in markdown.split('\n'):
        if line.lstrip().startswith('#'):
            for m in re.findall(tier_pattern, line, re.IGNORECASE):
                t = m.title()
                if t not in seen_tiers:
                    seen_tiers.add(t)
                    tiers_found.append(t)

    # Pass 2: tier words within 40 chars of a $ price that also contains
    # a pricing-context word — tighter window kills false positives on
    # dense marketing copy (e.g. "grow your Business for only $5").
    _pricing_ctx = re.compile(
        r'\b(?:plan|tier|month|mo|annual|year(?:ly)?|seat|user|per)\b', re.IGNORECASE
    )
    for price_match in re.finditer(r'\$\d', markdown):
        start = max(0, price_match.start() - 40)
        end   = min(len(markdown), price_match.end() + 40)
        window = markdown[start:end]
        if not _pricing_ctx.search(window):
            continue
        for m in re.findall(tier_pattern, window, re.IGNORECASE):
            t = m.title()
            if t not in seen_tiers:
                seen_tiers.add(t)
                tiers_found.append(t)

    # ── Pricing model signals ─────────────────────────────────
    pricing_models = {
        "per_seat":      ["per user", "per seat", "per member", "/user", "/seat", "user/mo", "seat/mo"],
        "flat_rate":     ["flat rate", "one price", "single plan", "all-inclusive"],
        "usage_based":   ["pay as you go", "usage-based", "per api call", "per request",
                          "pay per use", "metered", "metered billing",
                          "per credit", "credits per"],
        "freemium":      ["free forever", "free plan", "always free", "free tier",
                          "free crm", "free tools", "forever free"],
        "contact_sales": ["contact sales", "custom pricing", "talk to sales",
                          "request a quote", "request pricing", "get a quote"],
    }
    detected_models = [
        model for model, signals in pricing_models.items()
        if any(s in text_lower for s in signals)
    ]

    # ── Trial / free signals ──────────────────────────────────
    trial_signals = {
        "free_trial":     ["free trial", "try free", "try it free", "14-day trial",
                           "30-day trial", "14 day trial", "30 day trial"],
        "free_plan":      ["free plan", "free forever", "always free", "free tier",
                           "free crm", "free tools", "forever free"],
        "money_back":     ["money-back", "money back", "refund guarantee"],
        "no_credit_card": ["no credit card", "no cc required", "without credit card"],
    }
    trial_offers = [
        signal for signal, keywords in trial_signals.items()
        if any(k in text_lower for k in keywords)
    ]

    # ── Price range from already-extracted prices only ────────
    # Avoids inflating the range with unrelated dollar figures
    # (onboarding fees, case study revenue numbers, etc.).
    def _parse_price_val(p: str) -> int | None:
        m = re.search(r'\$([\d,]+)', p)
        if not m:
            return None
        try:
            return int(m.group(1).replace(',', ''))
        except ValueError:
            return None

    all_price_vals = [
        v for raw in (monthly_prices_raw + annual_prices_raw)
        for v in [_parse_price_val(raw)] if v is not None and 0 < v < 100_000
    ]
    price_range = {}
    if all_price_vals:
        price_range = {
            "lowest":  f"${min(all_price_vals)}",
            "highest": f"${max(all_price_vals)}",
        }

    # ── Free tier detection (multi-signal) ────────────────────
    # True if ANY of:
    #   1. freemium model detected (keywords)
    #   2. free_plan trial signal detected
    #   3. "$0" appears anywhere AND "Free" is in tiers_found
    #   4. The literal phrase "free crm" or "free tools" appears
    has_zero_price = bool(re.search(r'\$0(?:\.00)?\b', markdown))
    has_free_tier = (
        "freemium" in detected_models
        or "free_plan" in trial_offers
        or ("Free" in tiers_found and has_zero_price)
        or "free crm" in text_lower
        or "free tools" in text_lower
    )

    return {
        "tiers_found":         tiers_found,
        "monthly_prices":      monthly_prices,
        "annual_prices":       annual_prices,
        "price_range":         price_range,
        "pricing_model":       detected_models,
        "trial_offers":        trial_offers,
        "has_free_tier":       has_free_tier,
        "requires_sales_call": "contact_sales" in detected_models,
    }


# ═══════════════════════════════════════════════════════════════
# 2. MESSAGING EXTRACTION
# ═══════════════════════════════════════════════════════════════

def extract_messaging(markdown: str) -> dict:
    """
    Extracts headlines, target segments, positioning, tone, CTAs, proof points.
    """
    lines = markdown.split('\n')
    text_lower = markdown.lower()

    # FIX: clean heading prefix with regex instead of fragile lstrip('# ')
    def clean_heading(line: str) -> str:
        return re.sub(r'^#+\s*', '', line.strip())

    h1 = [clean_heading(l) for l in lines if re.match(r'^# [^#]', l)]
    h2 = [clean_heading(l) for l in lines if re.match(r'^## [^#]', l)]
    h3 = [clean_heading(l) for l in lines if re.match(r'^### [^#]', l)]

    # ── Frequency-ranked bucket matching ─────────────────────
    # Keywords in headings count 3× more than body occurrences.
    # A bucket must score ≥ 2 to appear in output — prevents every bucket
    # firing just because a SaaS homepage mentions each concept at least once.
    # Results are ranked by score descending so the strongest signal comes first.
    heading_text = ' '.join(h1 + h2 + h3).lower()

    def _rank_buckets(
        bucket_map: dict[str, list[str]], min_score: int = 2, top_n: int = 3
    ) -> list[str]:
        scored = {
            name: (
                sum(text_lower.count(kw) for kw in kws)
                + sum(heading_text.count(kw) for kw in kws) * 3
            )
            for name, kws in bucket_map.items()
        }
        return [
            name for name, score
            in sorted(scored.items(), key=lambda x: x[1], reverse=True)
            if score >= min_score
        ][:top_n]

    segments = {
        "SMB":         ["small business", "startup", "solopreneur", "freelancer", "small team"],
        "Mid-market":  ["growing team", "mid-size", "scaling", "scale-up", "mid-market"],
        "Enterprise":  ["enterprise", "fortune 500", "large team", "global", "organization"],
        "Developer":   ["api", "developer", "open source", "integrate", "sdk", "webhook"],
        "Agency":      ["agency", "client", "manage multiple", "white label", "reseller"],
        "eCommerce":   ["ecommerce", "shopify", "store", "merchant", "shop"],
    }
    target_segments = _rank_buckets(segments)

    value_props = {
        "cost_leadership": ["affordable", "cheap", "save money", "low cost", "free", "reduce cost"],
        "speed":           ["fast", "instant", "real-time", "quick", "in minutes", "in seconds"],
        "ease":            ["easy", "simple", "no code", "drag and drop", "intuitive", "effortless"],
        "power":           ["powerful", "advanced", "enterprise-grade", "robust", "feature-rich"],
        "outcome_driven":  ["grow", "revenue", "roi", "results", "convert", "pipeline", "leads"],
        "collaboration":   ["team", "together", "collaborate", "shared", "workspace"],
        "ai_native":       ["ai-powered", "ai native", "machine learning", "intelligent", "predict"],
    }
    positioning = _rank_buckets(value_props)

    tones = {
        "fear":        ["risk", "lose", "falling behind", "miss out", "left behind", "threat"],
        "aspiration":  ["grow", "succeed", "achieve", "dominate", "win", "thrive", "transform"],
        "trust":       ["trusted", "reliable", "secure", "proven", "guaranteed", "certified"],
        "urgency":     ["now", "today", "limited", "don't wait", "act fast", "hurry"],
        "empathy":     ["struggle", "frustrate", "pain", "we understand", "we know"],
        "exclusivity": ["only", "unique", "exclusive", "first", "leader", "pioneering"],
    }
    emotional_tone = _rank_buckets(tones)

    cta_map = {
        "low_friction": ["get started", "try free", "start free", "sign up free", "try it free"],
        "demo_request": ["book a demo", "get a demo", "request demo", "schedule demo", "see it in action"],
        "sales_led":    ["contact sales", "talk to sales", "speak to sales", "get a quote"],
        "high_intent":  ["start free trial", "start your trial", "try for free"],
        "generic":      ["sign up", "learn more", "get started", "explore"],
    }
    ctas_by_type = {}
    for cta_type, keywords in cta_map.items():
        found = [k for k in keywords if k in text_lower]
        if found:
            ctas_by_type[cta_type] = found

    all_ctas = [cta for ctas in ctas_by_type.values() for cta in ctas]
    primary_cta = all_ctas[0] if all_ctas else None

    _proof_noun = r'(?:customers|users|companies|businesses|teams|reviews|integrations|countries)'
    proof_numbers = re.findall(
        r'[\d,]+\+?\s+' + _proof_noun,
        markdown, re.IGNORECASE
    ) + re.findall(
        r'(?:millions|thousands|hundreds)\s+of\s+' + _proof_noun,
        markdown, re.IGNORECASE
    )
    uptime = re.findall(r'\d{2,3}(?:\.\d+)?%\s*uptime', markdown, re.IGNORECASE)

    buzzwords = [
        "AI-powered", "all-in-one", "seamless", "robust", "cutting-edge",
        "next-generation", "world-class", "revolutionary", "game-changing",
        "innovative", "transformative", "best-in-class", "holistic", "synergy",
        "frictionless", "end-to-end", "360-degree", "360°",
    ]
    buzzwords_found = [b for b in buzzwords if b.lower() in text_lower]

    return {
        "main_headline":     max(h1, key=len) if h1 else None,
        "subheadlines":      h2[:6],
        "h3_messages":       h3[:4],
        "target_segments":   target_segments,
        "positioning_angle": positioning,
        "emotional_tone":    emotional_tone,
        "ctas_by_type":      ctas_by_type,
        "primary_cta":       primary_cta,
        "proof_points":      proof_numbers[:5],
        "uptime_claims":     uptime,
        "buzzwords":         buzzwords_found,
        "buzzword_count":    len(buzzwords_found),
    }


# ═══════════════════════════════════════════════════════════════
# 3. COMPLAINTS EXTRACTION
# ═══════════════════════════════════════════════════════════════

def extract_complaints(markdown: str, company_name: str = "") -> dict:
    """
    Extracts complaint categories, sample lines, feature gaps, competitor mentions.
    Pass company_name to exclude the analyzed company from competitor_mentions.
    """
    # Filter out navigation/sidebar lines: markdown links, bullet links, and
    # lines that are mostly link syntax — these are Capterra nav elements,
    # not review content. Both category scoring AND sample line selection
    # run against this filtered text so nav keywords can't pollute the ranking.
    _nav_re = re.compile(r'(\]\(https?://|\* \[|^\[.*\]\()')
    lines = [
        l.strip() for l in markdown.split('\n')
        if len(l.strip()) > 25 and not _nav_re.search(l)
    ]
    clean_text = '\n'.join(lines)          # scoring source — nav-free
    text_lower  = clean_text.lower()       # used everywhere below

    complaint_categories = {
        "pricing_value": [
            "expensive", "overpriced", "costly", "pricey",
            "not worth", "too much", "price increase", "raised prices",
            "pricing", "cost", "cheap", "affordable"
        ],
        "ux_complexity": [
            "difficult", "confusing", "complicated", "hard to use",
            "steep learning curve", "clunky", "not intuitive",
            "overwhelming", "complex", "messy", "unintuitive"
        ],
        "performance": [
            "slow", "buggy", "crash", "glitch", "lag", "freeze",
            "error", "downtime", "unreliable", "unstable", "bug"
        ],
        "customer_support": [
            "poor support", "slow response", "unhelpful", "no support",
            "bad customer service", "support team", "ticket", "wait time",
            "unresponsive", "hard to reach"
        ],
        "missing_features": [
            "missing", "lacks", "wish it had", "doesn't have",
            "need more", "limited", "no option", "can't do",
            "feature request", "would like to see", "needs improvement"
        ],
        "integrations": [
            "integration", "doesn't integrate", "sync issue",
            "api problem", "doesn't work with", "compatibility",
            "connect", "native integration"
        ],
        "onboarding": [
            "hard to set up", "setup", "onboarding", "getting started",
            "documentation", "tutorial", "training", "learn"
        ],
        "reporting_analytics": [
            "reporting", "analytics", "dashboard", "metrics",
            "data export", "insights", "visibility", "tracking"
        ],
    }

    category_scores = {}
    category_lines = {}

    for category, keywords in complaint_categories.items():
        hits = sum(1 for kw in keywords for _ in re.finditer(re.escape(kw), text_lower))
        category_scores[category] = hits

        matched_lines = []
        for line in lines:
            if any(kw in line.lower() for kw in keywords):
                matched_lines.append(line)
        category_lines[category] = matched_lines[:5]

    sorted_categories = sorted(
        category_scores.items(), key=lambda x: x[1], reverse=True
    )
    top_complaints = [cat for cat, score in sorted_categories if score > 0]

    feature_gap_patterns = [
        r'\bwish(?:ed)?\b.{0,20}(?:had|have|offered?)\s+([a-z][^\.\n]{8,60})',
        r'\bmissing\s+(?:a\s+|an\s+|the\s+)?([a-z][^\.\n]{8,50})',
        r'\b(?:doesn\'t|does not|can\'t|cannot)\s+([a-z][^\.\n]{5,40})',
        r'\b(?:lack|lacks|lacking)\s+([a-z][^\.\n]{5,50})',
        r'\b(?:need|needs|needed)\s+(?:a\s+|better\s+|more\s+|an\s+)?([a-z][^\.\n]{5,50})',
        r'\bwould\s+(?:love|like)\s+(?:to\s+see\s+|if\s+)?([a-z][^\.\n]{5,60})',
        r'\bno\s+(?:built[- ]in|native|direct)\s+([a-z][^\.\n]{5,40})',
        r'\blimited\s+([a-z][^\.\n]{5,40})',
    ]
    _noise_starts = ('the ', 'a ', 'an ', 'to ', 'and ', 'or ', 'it ', 'is ', 'was ', 'be ', 'by ')
    feature_gaps = []
    for pattern in feature_gap_patterns:
        for m in re.findall(pattern, text_lower):
            m = m.strip().rstrip('.,;:')
            if len(m) > 8 and not any(m.startswith(ns) for ns in _noise_starts):
                feature_gaps.append(m)
    feature_gaps = list(dict.fromkeys(feature_gaps[:10]))

    # ── Competitor mentions ───────────────────────────────────
    # Two-pass approach:
    #   1. Static fallback list for common SaaS tools (broad coverage).
    #   2. Dynamic extraction — scrape names appearing in "vs X", "switched from X",
    #      "alternative to X", "compared to X" patterns on the page itself.
    #      This makes the extractor useful for any vertical, not just CRM.
    # Own company name is excluded from both passes.
    _own = company_name.lower().strip()

    common_competitors = [
        "salesforce", "hubspot", "pipedrive", "zoho", "monday",
        "asana", "notion", "clickup", "airtable", "jira",
        "intercom", "zendesk", "freshdesk", "stripe", "mailchimp",
        "activecampaign", "klaviyo", "marketo", "pardot",
    ]

    # Dynamic: pull names from comparison phrases (1-3 word proper nouns after trigger)
    _comp_trigger = re.compile(
        r'\b(?:vs\.?|versus|compared (?:to|with)|alternative to|switched from|coming from|migrated from|replaced)\s+([A-Z][A-Za-z0-9]+(?: [A-Z][A-Za-z0-9]+){0,2})',
        re.IGNORECASE
    )
    dynamic_competitors = set()
    for m in _comp_trigger.finditer(markdown):
        name = m.group(1).strip().lower()
        # Skip generic words and own name
        if len(name) >= 3 and name != _own and not re.match(r'^(the|a|an|our|us|it|this|that)$', name):
            dynamic_competitors.add(name)

    all_competitors = set(common_competitors) | dynamic_competitors

    competitor_mentions = {}
    for comp in sorted(all_competitors):
        if _own and comp == _own:
            continue
        count = len(re.findall(r'\b' + re.escape(comp) + r'\b', text_lower))
        if count > 0:
            competitor_mentions[comp] = count

    positive_signals = [
        "great", "love", "excellent", "amazing", "fantastic",
        "easy to use", "highly recommend", "best", "wonderful"
    ]
    negative_signals = [
        "hate", "terrible", "awful", "worst", "horrible",
        "useless", "waste", "disappointed", "frustrating", "poor"
    ]
    pos_count = sum(text_lower.count(w) for w in positive_signals)
    neg_count = sum(text_lower.count(w) for w in negative_signals)
    total = pos_count + neg_count
    sentiment = "neutral"
    if total > 0:
        ratio = pos_count / total
        sentiment = "positive" if ratio > 0.6 else "negative" if ratio < 0.4 else "mixed"

    # FIX: also store total_signals so detect_changes can normalize across snapshots
    total_complaint_signals = sum(category_scores.values())

    return {
        "top_complaint_categories": top_complaints[:5],
        "category_scores":          {k: v for k, v in sorted_categories if v > 0},
        "total_complaint_signals":  total_complaint_signals,
        "sample_complaint_lines":   {
            cat: lns for cat, lns in category_lines.items() if lns
        },
        "feature_gaps_mentioned":   feature_gaps,
        "competitor_comparisons":   competitor_mentions,
        "top_competitor_mentioned": max(competitor_mentions, key=competitor_mentions.get)
                                    if competitor_mentions else None,
        "sentiment_lean":           sentiment,
        "positive_signal_count":    pos_count,
        "negative_signal_count":    neg_count,
    }


# ═══════════════════════════════════════════════════════════════
# 4. INVESTMENT SIGNALS EXTRACTION
# ═══════════════════════════════════════════════════════════════

def extract_signals(markdown: str) -> dict:
    """
    Extracts hiring volume, tech investment, geo expansion, seniority distribution.
    """
    text_lower = markdown.lower()

    departments = {
        "engineering":      ["software engineer", "backend", "frontend", "full stack", "sre", "devops", "platform"],
        "product":          ["product manager", "product designer", "ux", "ui designer", "product lead"],
        "data_ai":          ["data scientist", "ml engineer", "ai researcher", "data analyst", "data engineer"],
        "sales":            ["account executive", "sales rep", "sales manager", "bdr", "sdr", "revenue"],
        "marketing":        ["marketing manager", "growth", "content writer", "seo", "demand gen", "brand"],
        "customer_success": ["customer success", "implementation", "onboarding", "account manager", "csm"],
        "security":         ["security engineer", "security analyst", "infosec", "compliance", "soc"],
        "finance_legal":    ["finance", "accounting", "legal", "counsel", "cfo", "controller"],
        "hr_people":        ["recruiter", "people ops", "hr business", "talent acquisition"],
        "leadership":       ["vp of", "director of", "head of", "chief", "cto", "cmo", "cpo"],
    }

    dept_counts = {}
    for dept, keywords in departments.items():
        count = sum(text_lower.count(kw) for kw in keywords)
        if count > 0:
            dept_counts[dept] = count

    sorted_depts = sorted(dept_counts.items(), key=lambda x: x[1], reverse=True)

    tech_areas = {
        "AI_ML":          ["machine learning", "llm", "nlp", "generative ai", "ai engineer", "model training"],
        "mobile":         ["ios", "android", "react native", "mobile engineer", "flutter"],
        "infrastructure": ["kubernetes", "aws", "gcp", "azure", "cloud", "terraform", "microservices"],
        "security":       ["security", "zero trust", "soc 2", "gdpr", "compliance", "pen test"],
        "data_platform":  ["spark", "kafka", "data warehouse", "dbt", "snowflake", "databricks"],
        "api_platform":   ["api", "developer platform", "sdk", "integration", "webhook", "rest", "graphql"],
        "enterprise":     ["enterprise", "salesforce", "sso", "saml", "audit log", "rbac"],
        "payments":       ["payment", "billing", "stripe", "revenue recognition", "monetization"],
    }
    tech_investments = [
        area for area, keywords in tech_areas.items()
        if any(k in text_lower for k in keywords)
    ]

    seniority = {
        "junior":     ["junior", "entry level", "associate", "intern", "graduate"],
        "mid":        ["software engineer", "manager", "specialist", "analyst"],
        "senior":     ["senior", "staff", "lead", "principal", "sr."],
        "leadership": ["director", "vp", "vice president", "head of", "chief"],
    }
    # Classify each line to exactly one seniority level using priority order
    # (highest wins). Prevents "Senior Software Engineer" from incrementing
    # both mid (software engineer) and senior (senior) simultaneously.
    _seniority_priority = ["leadership", "senior", "mid", "junior"]
    seniority_counts: dict[str, int] = {level: 0 for level in seniority}
    for line in text_lower.split('\n'):
        line = line.strip()
        if len(line) < 5:
            continue
        for level in _seniority_priority:
            if any(kw in line for kw in seniority[level]):
                seniority_counts[level] += 1
                break
    dominant_seniority = max(seniority_counts, key=seniority_counts.get) \
        if any(seniority_counts.values()) else None

    regions = {
        "EMEA":          ["london", "berlin", "amsterdam", "paris", "dublin", "emea", "europe"],
        "APAC":          ["singapore", "sydney", "tokyo", "bangalore", "apac", "asia"],
        "LATAM":         ["são paulo", "mexico city", "latam", "latin america"],
        "North America": ["new york", "san francisco", "austin", "toronto", "chicago", "seattle"],
        "Remote":        ["remote", "work from anywhere", "distributed", "fully remote"],
    }
    geo_signals = [
        region for region, keywords in regions.items()
        if any(k in text_lower for k in keywords)
    ]

    remote_stance = "unknown"
    if "fully remote" in text_lower or "remote-first" in text_lower:
        remote_stance = "remote-first"
    elif "remote" in text_lower and "office" in text_lower:
        remote_stance = "hybrid"
    elif "in-office" in text_lower or "on-site" in text_lower:
        remote_stance = "in-office"
    elif "remote" in text_lower:
        remote_stance = "remote-friendly"

    inferences = []
    if dept_counts.get("data_ai", 0) > dept_counts.get("engineering", 0) * 0.3:
        inferences.append("Heavy AI/ML investment — likely building AI features")
    _sales = dept_counts.get("sales", 0)
    _cs    = dept_counts.get("customer_success", 0)
    if _sales > _cs * 1.7:
        inferences.append("Sales-led growth motion — acquiring new customers aggressively")
    elif _cs > _sales:
        inferences.append("Product-led / retention focus — more CS than Sales")
    elif _sales > 0 or _cs > 0:
        # Ratio between 1.0 and 1.7 — neither rule fires; call it explicitly.
        inferences.append("Balanced growth — roughly equal Sales and CS investment")
    if dept_counts.get("security", 0) >= 2:
        inferences.append("Enterprise push — investing in security/compliance")
    if dept_counts.get("leadership", 0) >= 3:
        inferences.append("Leadership build-out — scaling org structure")
    if "AI_ML" in tech_investments and "api_platform" in tech_investments:
        inferences.append("Building developer-facing AI platform")

    total_dept_keyword_hits = sum(dept_counts.values())

    return {
        "total_dept_keyword_hits": total_dept_keyword_hits,
        "hiring_by_department":   dict(sorted_depts),
        "biggest_hiring_push":    sorted_depts[0][0] if sorted_depts else None,
        "top_3_departments":      [d for d, _ in sorted_depts[:3]],
        "tech_investments":       tech_investments,
        "seniority_distribution": seniority_counts,
        "dominant_seniority":     dominant_seniority,
        "geographic_expansion":   geo_signals,
        "remote_stance":          remote_stance,
        "strategic_inferences":   inferences,
    }


# ═══════════════════════════════════════════════════════════════
# CHANGE DETECTION
# ═══════════════════════════════════════════════════════════════

def detect_changes(old: dict, new: dict, feature: str) -> list[str]:
    changes = []

    # FIX: skip diffing if either side is stale (failed scrape fallback)
    if old.get("_stale") or new.get("_stale"):
        return []

    if feature == "pricing":
        old_prices = set(old.get("monthly_prices", []))
        new_prices = set(new.get("monthly_prices", []))
        old_tiers  = set(old.get("tiers_found", []))
        new_tiers  = set(new.get("tiers_found", []))

        if added := new_prices - old_prices:
            changes.append(f"NEW monthly prices: {added}")
        if removed := old_prices - new_prices:
            changes.append(f"REMOVED monthly prices: {removed}")
        if added := new_tiers - old_tiers:
            changes.append(f"NEW tiers added: {added}")
        if removed := old_tiers - new_tiers:
            changes.append(f"REMOVED tiers: {removed}")
        if old.get("has_free_tier") != new.get("has_free_tier"):
            status = "ADDED" if new.get("has_free_tier") else "REMOVED"
            changes.append(f"FREE TIER {status}")
        if old.get("requires_sales_call") != new.get("requires_sales_call"):
            status = "now requires" if new.get("requires_sales_call") else "dropped"
            changes.append(f"Sales call requirement {status}")

    elif feature == "messaging":
        if old.get("main_headline") != new.get("main_headline"):
            changes.append(
                f"HEADLINE CHANGED: '{old.get('main_headline')}' → '{new.get('main_headline')}'"
            )
        old_segs = set(old.get("target_segments", []))
        new_segs = set(new.get("target_segments", []))
        if added := new_segs - old_segs:
            changes.append(f"NOW targeting new segments: {added}")
        if removed := old_segs - new_segs:
            changes.append(f"DROPPED targeting: {removed}")
        if old.get("primary_cta") != new.get("primary_cta"):
            changes.append(
                f"PRIMARY CTA CHANGED: '{old.get('primary_cta')}' → '{new.get('primary_cta')}'"
            )
        old_props = set(old.get("positioning_angle", []))
        new_props = set(new.get("positioning_angle", []))
        if added := new_props - old_props:
            changes.append(f"NEW positioning angle: {added}")

    elif feature == "complaints":
        old_top = old.get("top_complaint_categories", [])
        new_top = new.get("top_complaint_categories", [])
        if old_top and new_top and old_top[0] != new_top[0]:
            changes.append(f"TOP COMPLAINT SHIFTED: '{old_top[0]}' → '{new_top[0]}'")

        # FIX: normalize by total signals so a longer page doesn't fake a spike
        old_total = old.get("total_complaint_signals", 0) or 1
        new_total = new.get("total_complaint_signals", 0) or 1
        old_scores = old.get("category_scores", {})
        new_scores = new.get("category_scores", {})
        for cat, new_score in new_scores.items():
            old_score = old_scores.get(cat, 0)
            if old_score == 0:
                continue
            old_share = old_score / old_total
            new_share = new_score / new_total
            # only flag if relative share grew >50% AND absolute share is meaningful
            if new_share > old_share * 1.5 and new_share > 0.05:
                changes.append(
                    f"COMPLAINT SHARE SPIKE in '{cat}': "
                    f"{old_share:.0%} → {new_share:.0%}"
                )

        old_comps = set(old.get("competitor_comparisons", {}).keys())
        new_comps = set(new.get("competitor_comparisons", {}).keys())
        if added := new_comps - old_comps:
            changes.append(f"NEW competitor comparisons appearing: {added}")

    elif feature == "signals":
        old_dept = old.get("biggest_hiring_push")
        new_dept = new.get("biggest_hiring_push")
        if old_dept != new_dept:
            changes.append(f"HIRING FOCUS SHIFTED: '{old_dept}' → '{new_dept}'")

        old_tech = set(old.get("tech_investments", []))
        new_tech = set(new.get("tech_investments", []))
        if added := new_tech - old_tech:
            changes.append(f"NEW tech investment areas: {added}")

        old_geo = set(old.get("geographic_expansion", []))
        new_geo = set(new.get("geographic_expansion", []))
        if added := new_geo - old_geo:
            changes.append(f"NEW geographic expansion signals: {added}")

        if old.get("remote_stance") != new.get("remote_stance"):
            changes.append(
                f"REMOTE POLICY CHANGED: '{old.get('remote_stance')}' → '{new.get('remote_stance')}'"
            )

    return changes


# ═══════════════════════════════════════════════════════════════
# SNAPSHOT MANAGEMENT
# ═══════════════════════════════════════════════════════════════

SNAPSHOT_DIR = "snapshots"
MAX_SNAPSHOT_ARCHIVES = 10  # per company; oldest deleted when exceeded


def save_snapshot(company_name: str, data: dict) -> None:
    """
    FIX: write to temp file first, then atomically swap. Prevents data loss
    if the script crashes mid-save.
    """
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    filepath = os.path.join(SNAPSHOT_DIR, f"{company_name}_latest.json")
    temppath = filepath + ".tmp"

    # 1. write new data to temp file
    with open(temppath, "w") as f:
        json.dump(data, f, indent=2)

    # 2. archive existing latest if it exists
    if os.path.exists(filepath):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        archive = os.path.join(SNAPSHOT_DIR, f"{company_name}_{ts}.json")
        os.rename(filepath, archive)
        print(f"  [archive] → {archive}")

    # Prune old archives unconditionally — runs even if latest was manually
    # deleted, so the archive count stays bounded regardless of file state.
    _archive_pat = re.compile(rf'^{re.escape(company_name)}_\d{{8}}_\d{{6}}\.json$')
    existing_archives = sorted([
        os.path.join(SNAPSHOT_DIR, f)
        for f in os.listdir(SNAPSHOT_DIR)
        if _archive_pat.match(f)
    ])
    if len(existing_archives) > MAX_SNAPSHOT_ARCHIVES:
        for old_archive in existing_archives[:-MAX_SNAPSHOT_ARCHIVES]:
            os.remove(old_archive)
            print(f"  [pruned]  → {old_archive}")

    # 3. promote temp to latest
    os.rename(temppath, filepath)
    print(f"  [saved]   → {filepath}")


def load_snapshot(company_name: str) -> dict | None:
    filepath = os.path.join(SNAPSHOT_DIR, f"{company_name}_latest.json")
    if os.path.exists(filepath):
        with open(filepath) as f:
            return json.load(f)
    return None


# ═══════════════════════════════════════════════════════════════
# URL DISCOVERY (sitemap-first, path-probe fallback)
# ═══════════════════════════════════════════════════════════════

# Keywords that suggest a URL serves a given feature.
# Order matters within each list — more specific patterns first.
DISCOVERY_KEYWORDS = {
    "pricing": ["pricing", "plans", "/buy", "/subscribe"],
    "signals": ["careers/jobs", "/jobs", "careers", "/join-us", "/work-with-us"],
}

# Common path patterns to try if sitemap doesn't yield a match.
# Kept short (2 per feature) because worst-case probe time scales linearly
# with this list, and beyond the top 2 the hit rate drops off a cliff.
# If a site doesn't use /pricing or /plans, it probably has a custom URL
# that no list of fallbacks will catch — the sitemap is your real hope.
FALLBACK_PATHS = {
    "pricing": ["/pricing", "/plans"],
    "signals": ["/careers", "/jobs"],
}


async def fetch_sitemap_urls(crawler, domain: str, run_cfg) -> tuple[list[str], bool]:
    """
    Fetch sitemap.xml from a domain and return all <loc> URLs found.
    Handles both regular sitemaps and sitemap-index files (sitemap of sitemaps).

    Returns (urls, reachable):
      urls:      list of URLs found in the sitemap (may be empty)
      reachable: True if the domain responded at all (even with no sitemap),
                 False if the request timed out / connection refused.
                 Callers can use this to skip further probing on dead domains.
    """
    sitemap_url = f"https://{domain}/sitemap.xml"
    reachable = False
    try:
        result = await crawler.arun(url=sitemap_url, config=run_cfg)
        # Any response at all — even a 404 — means the domain is reachable.
        if result is not None:
            reachable = bool(getattr(result, "success", False))
            if not reachable:
                # Check error message — "timeout" / "ERR_CONNECTION" means dead,
                # but 404 / 403 means reachable-but-no-sitemap.
                err = str(getattr(result, "error_message", "") or "").lower()
                if "timeout" not in err and "err_connection" not in err:
                    reachable = True

        if not reachable or not getattr(result, "success", False):
            return ([], reachable)

        raw = getattr(result, "html", "") or ""
        if not raw or ("<urlset" not in raw and "<sitemapindex" not in raw):
            return ([], True)  # Reachable but no sitemap content
    except Exception:
        return ([], False)

    urls: list[str] = []
    try:
        # Strip XML namespace declarations to make parsing easier.
        cleaned = re.sub(r'\sxmlns="[^"]+"', '', raw, count=1)
        root = ET.fromstring(cleaned)

        if root.tag.endswith("sitemapindex"):
            child_sitemaps = [
                loc.text for loc in root.findall(".//loc") if loc.text
            ][:3]
            for child_url in child_sitemaps:
                try:
                    child_result = await crawler.arun(url=child_url, config=run_cfg)
                    if child_result and getattr(child_result, "success", False):
                        child_raw = getattr(child_result, "html", "") or ""
                        child_cleaned = re.sub(r'\sxmlns="[^"]+"', '', child_raw, count=1)
                        child_root = ET.fromstring(child_cleaned)
                        for loc in child_root.findall(".//loc"):
                            if loc.text:
                                urls.append(loc.text.strip())
                except Exception:
                    continue
        else:
            for loc in root.findall(".//loc"):
                if loc.text:
                    urls.append(loc.text.strip())
    except ET.ParseError:
        return ([], True)  # Reachable, malformed sitemap

    return (urls, True)


def pick_url_from_sitemap(sitemap_urls: list[str], keywords: list[str]) -> str | None:
    """
    Pick the best URL from a sitemap that matches one of the given keywords.
    Returns None if no match. Prefers URLs with shorter paths (closer to root)
    when multiple match — /pricing beats /resources/blog/pricing-guide.
    """
    matches = []
    for url in sitemap_urls:
        url_lower = url.lower()
        for kw_idx, kw in enumerate(keywords):
            if kw in url_lower:
                # (keyword index, path length) — lower is better on both
                path = url.split("://", 1)[-1].split("/", 1)[-1] if "/" in url else ""
                matches.append((kw_idx, len(path), url))
                break
    if not matches:
        return None
    matches.sort()
    return matches[0][2]


async def probe_paths(crawler, domain: str, paths: list[str], run_cfg) -> str | None:
    """
    Try a list of candidate paths against a domain in parallel using Crawl4AI's
    native arun_many(), which is the documented way to get actual parallelism
    from a single crawler instance. (asyncio.gather on arun() calls gets
    serialized internally by Crawl4AI — don't use that pattern here.)

    Returns the first URL that returns a successful response with substantial
    content. Returns None if none work.
    """
    candidates = [f"https://{domain}{p}" for p in paths]

    try:
        results = await crawler.arun_many(urls=candidates, config=run_cfg)
    except Exception:
        return None

    # Build a url → ok map so we can return the first successful one in
    # priority order (matching `paths` order, not result completion order).
    ok_map: dict[str, bool] = {}
    for result in results or []:
        if result is None:
            continue
        url = getattr(result, "url", None)
        ok = (
            getattr(result, "success", False)
            and getattr(result, "markdown", None)
            and len(result.markdown) >= 500
        )
        if url:
            ok_map[url] = bool(ok)

    for url in candidates:
        if ok_map.get(url):
            return url
    return None


async def discover_urls(
    crawler,
    domain: str,
    run_cfg,
    company_slug: str = "",
) -> dict[str, str | None]:
    """
    Discover the pricing, messaging (homepage), and signals URLs for a domain.

    Strategy:
      1. Fetch sitemap.xml. If found, look for keyword matches per feature.
      2. For any feature still missing, probe common path fallbacks in parallel.
      3. For signals, also try third-party ATS boards (Greenhouse, Lever, Workable,
         Ashby) — most modern SaaS companies host careers there, not on their domain.
      4. Messaging always defaults to https://{domain} (no discovery needed).

    Returns a dict with keys 'pricing', 'messaging', 'signals'.
    Values are URLs or None (None means we couldn't find it).
    Capterra ('complaints') is handled separately — it requires an ID, not discovery.
    """
    print(f"\n  🔍 Discovering URLs for {domain}...")

    # Note: messaging defaults to bare domain. If a site only serves on www.,
    # the server will 301-redirect and Playwright will follow it automatically.
    # No need for an extra probe here — that would double the homepage fetch cost.
    discovered: dict[str, str | None] = {
        "pricing":   None,
        "messaging": f"https://{domain}",
        "signals":   None,
    }

    # ── Step 1: try sitemap ─────────────────────────────────
    sitemap_urls, domain_reachable = await fetch_sitemap_urls(crawler, domain, run_cfg)

    if not domain_reachable:
        # Domain is dead / blocking / timing out. Don't waste time probing
        # more paths — they'll all hit the same wall. Return with everything
        # set to None (messaging already points to bare domain; the main
        # crawl will attempt it and fail cleanly there if needed).
        print(f"    ✗ {domain} is unreachable (timeout / connection refused)")
        print("    ↳ skipping path probing — domain looks dead")
        return discovered

    if sitemap_urls:
        print(f"    ✓ sitemap.xml found ({len(sitemap_urls)} URLs)")
        for feature, keywords in DISCOVERY_KEYWORDS.items():
            picked = pick_url_from_sitemap(sitemap_urls, keywords)
            if picked:
                discovered[feature] = picked
                print(f"    ✓ {feature:9} (sitemap): {picked}")
    else:
        print("    ⓘ no sitemap found, will probe paths")

    # ── Step 2: path probing for whatever's still missing ───
    missing_features = [
        f for f in ("pricing", "signals") if discovered[f] is None
    ]
    if missing_features:
        for feature in missing_features:
            picked = await probe_paths(crawler, domain, FALLBACK_PATHS[feature], run_cfg)
            if picked:
                discovered[feature] = picked
                print(f"    ✓ {feature:9} (probed):  {picked}")
            else:
                print(f"    ✗ {feature:9} not found")

    # ── Step 3: ATS platform probing for signals (careers) ────────────────
    # ~60% of modern SaaS companies host jobs on Greenhouse, Lever, Workable,
    # or Ashby rather than a self-hosted /careers page. Try all four in parallel.
    # Uses a longer timeout (12s) — Greenhouse/Workable can take 5-7s cold.
    if discovered["signals"] is None and company_slug:
        slug = re.sub(r'[^a-z0-9\-]', '', company_slug.lower().replace('_', '-').replace(' ', '-'))
        if not slug:
            print("    ✗ signals    (ATS):     company_slug produced empty slug, skipping")
        else:
            ats_run_cfg = CrawlerRunConfig(
                page_timeout=12000,
                wait_until="domcontentloaded",
                delay_before_return_html=1.0,
                magic=False,
                simulate_user=False,
            )
            ats_candidates = [
                f"https://boards.greenhouse.io/{slug}",
                f"https://jobs.lever.co/{slug}",
                f"https://{slug}.workable.com",
                f"https://{slug}.ashbyhq.com",
            ]
            try:
                ats_results = await crawler.arun_many(urls=ats_candidates, config=ats_run_cfg)
            except Exception:
                ats_results = []
            ok_ats: dict[str, bool] = {}
            for result in ats_results or []:
                if result is None:
                    continue
                url = getattr(result, "url", None)
                ok = (
                    getattr(result, "success", False)
                    and getattr(result, "markdown", None)
                    and len(result.markdown) >= 200
                )
                if url:
                    ok_ats[url] = bool(ok)
            for url in ats_candidates:
                if ok_ats.get(url):
                    discovered["signals"] = url
                    print(f"    ✓ signals    (ATS):     {url}")
                    break
            else:
                print(f"    ✗ signals    (ATS):     no ATS boards found for slug '{slug}'")

    print(f"    ✓ messaging          : {discovered['messaging']}")
    return discovered


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

async def analyze_competitor(
    company_name: str,
    domain: str,
    capterra_id: str | None = None,
    capterra_slug: str | None = None,
) -> dict:
    """
    Full pipeline: crawl → extract → diff → save.

    Args:
        company_name:  lowercase slug, e.g. "hubspot"
        domain:        bare domain, e.g. "hubspot.com"
        capterra_id:   Capterra's internal numeric ID, e.g. "152373" for HubSpot CRM.
                       Look it up manually at capterra.com — search the product, then
                       grab the number from the URL path /p/{ID}/{Slug}/.
                       If None, complaints scraping is skipped entirely.
        capterra_slug: The trailing slug from the same URL, e.g. "HubSpot-CRM".
                       Required if capterra_id is provided.
    """
    print(f"\n{'═'*60}")
    print(f"  Analyzing: {company_name.upper()}")
    print(f"{'═'*60}")

    previous = load_snapshot(company_name)
    all_changes = {}

    # ── Standard crawler config (headless + stealth) ─────────
    # FIX: enable_stealth (real param), removed use_stealth_js (does not exist)
    standard_browser_cfg = BrowserConfig(
        headless=True,
        enable_stealth=True,
        user_agent_mode="random",
        ignore_https_errors=True,
    )

    # FIX: wait_until="load" — domcontentloaded fires before anti-bot sensors finish
    standard_run_cfg = CrawlerRunConfig(
        page_timeout=45000,
        wait_until="load",
        delay_before_return_html=8.0,
        magic=True,
        simulate_user=True,
        scroll_delay=0.5,
        scan_full_page=True,
    )

    # ── Undetected crawler config (visible browser, undetected adapter) ──
    # Used only for hosts in PROTECTED_HOSTS (e.g. g2.com / DataDome).
    # headless=False is recommended by docs for actual evasion effectiveness.
    undetected_browser_cfg = BrowserConfig(
        headless=UNDETECTED_HEADLESS,
        enable_stealth=True,
        ignore_https_errors=True,
        verbose=False,
    )
    # Longer timeout — captcha challenges can take 10–20s to clear.
    undetected_run_cfg = CrawlerRunConfig(
        page_timeout=60000,
        wait_until="load",
        delay_before_return_html=12.0,
        magic=True,
        simulate_user=True,
        scroll_delay=0.8,
        scan_full_page=True,
    )

    # Lightweight config for discovery probes — short timeouts since we're
    # just checking if a path exists, not extracting content from it.
    # 8s is enough for a healthy server; longer timeouts just waste time
    # on dead/blocking hosts.
    discover_run_cfg = CrawlerRunConfig(
        page_timeout=8000,
        wait_until="domcontentloaded",  # OK here, we're not waiting for anti-bot
        delay_before_return_html=0.5,
        magic=False,
        simulate_user=False,
    )

    # Build the URLs dict. If the caller passed pre-built URLs (batch mode with
    # known paths), use those. Otherwise discover via sitemap+probing inside the
    # crawler block below.
    extractors = {
        "pricing":    extract_pricing,
        "messaging":  extract_messaging,
        "complaints": functools.partial(extract_complaints, company_name=company_name),
        "signals":    extract_signals,
    }

    def is_protected(url: str) -> bool:
        return any(host in url for host in PROTECTED_HOSTS)

    new_data = {
        "company":   company_name,
        "domain":    domain,
        "timestamp": datetime.now().isoformat(),
        "analysis":  {},
    }

    def store_stale(feature: str, reason: str) -> None:
        """Mark a feature as stale on the new snapshot, preserving previous data if any."""
        if previous and feature in previous.get("analysis", {}):
            stale = dict(previous["analysis"][feature])
            stale["_stale"] = True
            stale["_stale_since"] = previous.get("timestamp")
            stale["_stale_reason"] = reason
            new_data["analysis"][feature] = stale
            print(f"    ↳ kept stale data from {previous.get('timestamp')}")
        else:
            new_data["analysis"][feature] = {
                "_stale": True,
                "_stale_since": None,
                "_stale_reason": reason,
            }
            print(f"    ↳ [{feature}] no previous data — empty stale marker")

    async def crawl_one(crawler, feature: str, url: str, run_cfg) -> None:
        """Run one crawl, extract on success, mark stale on failure."""
        try:
            result = await crawler.arun(url=url, config=run_cfg)
            ok = (
                result is not None
                and getattr(result, "success", False)
                and result.markdown
                and len(result.markdown) >= 500
            )
            if not ok:
                err_msg = getattr(result, "error_message", "no markdown / too short")
                err_short = str(err_msg).split("\n")[0][:200]
                print(f"    ⚠️  FAILED: {err_short}")
                store_stale(feature, err_short)
                return

            new_data["analysis"][feature] = extractors[feature](result.markdown)
            print(f"    ✓ extracted {len(result.markdown):,} chars")

        except Exception as e:
            err_short = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
            print(f"    ✗ error: {err_short}")
            store_stale(feature, err_short)

    # ── Standard crawler pass (also handles discovery) ───────
    async with AsyncWebCrawler(config=standard_browser_cfg) as crawler:

        # Discover URLs inside the crawler context so we can reuse the same
        # browser instance. Discovery is async and runs probes in parallel.
        discovered = await discover_urls(crawler, domain, discover_run_cfg, company_slug=company_name)

        urls: dict[str, str] = {}
        for feature, url in discovered.items():
            if url:
                urls[feature] = url
            else:
                # Discovery failed for this feature — mark stale up front so
                # the diff loop knows it was attempted but unfindable.
                store_stale(feature, "URL discovery failed (no sitemap match, no probe success)")

        # Capterra is a separate flow — requires user-provided ID, no discovery.
        if capterra_id and capterra_slug:
            urls["complaints"] = (
                f"https://www.capterra.com/p/{capterra_id}/{capterra_slug}/reviews/"
            )
        else:
            print("\n  ⓘ No Capterra ID provided — skipping complaints feature")

        # Split URLs by protection level so we use the right crawler for each.
        standard_urls  = {f: u for f, u in urls.items() if not is_protected(u)}
        protected_urls = {f: u for f, u in urls.items() if is_protected(u)}

        # Crawl the standard (unprotected) URLs.
        for feature, url in standard_urls.items():
            print(f"\n  ► {feature.upper()}: {url}")
            await crawl_one(crawler, feature, url, standard_run_cfg)

    # ── Undetected crawler pass (only if needed and available) ──
    if protected_urls:
        if not USE_UNDETECTED_FOR_PROTECTED or not UNDETECTED_AVAILABLE:
            reason = ("undetected disabled by config"
                      if not USE_UNDETECTED_FOR_PROTECTED
                      else "UndetectedAdapter not installed")
            print(f"\n  ⓘ Skipping protected sites — {reason}")
            for feature, url in protected_urls.items():
                print(f"\n  ► {feature.upper()}: {url}")
                print(f"    ⚠️  SKIPPED ({reason})")
                store_stale(feature, reason)
        else:
            print(f"\n  ⓘ Switching to UNDETECTED browser for protected sites "
                  f"(headless={UNDETECTED_HEADLESS})")
            adapter = UndetectedAdapter()
            strategy = AsyncPlaywrightCrawlerStrategy(
                browser_config=undetected_browser_cfg,
                browser_adapter=adapter,
            )
            try:
                async with AsyncWebCrawler(
                    crawler_strategy=strategy,
                    config=undetected_browser_cfg,
                ) as crawler:
                    for feature, url in protected_urls.items():
                        print(f"\n  ► {feature.upper()}: {url}")
                        await crawl_one(crawler, feature, url, undetected_run_cfg)
            except Exception as e:
                # If the undetected crawler itself fails to start, mark all protected
                # features stale rather than crashing the whole pipeline.
                err_short = f"undetected crawler init failed: {type(e).__name__}: {str(e).splitlines()[0][:200]}"
                print(f"\n  ✗ {err_short}")
                for feature in protected_urls:
                    if feature not in new_data["analysis"]:
                        store_stale(feature, err_short)

    # ── Change detection ─────────────────────────────────────
    # Iterate over features we actually attempted (urls), not all known extractors.
    # This avoids confusing diffs/stale warnings for features that were skipped
    # because the user didn't supply credentials/IDs (e.g. capterra_id).
    attempted_features = list(urls.keys())

    if previous:
        print(f"\n  Comparing with snapshot from: {previous['timestamp']}")
        for feature in attempted_features:
            old_f = previous["analysis"].get(feature, {})
            new_f = new_data["analysis"].get(feature, {})
            diffs = detect_changes(old_f, new_f, feature)
            if diffs:
                all_changes[feature] = diffs

        # FIX: warn loudly about features that couldn't be diffed
        stale_features = [
            f for f in attempted_features
            if new_data["analysis"].get(f, {}).get("_stale")
        ]
        if stale_features:
            print(f"\n  ⚠️  STALE (not diffed): {stale_features}")

        if all_changes:
            print("\n  🚨 CHANGES DETECTED:")
            for feature, diffs in all_changes.items():
                print(f"\n    [{feature.upper()}]")
                for d in diffs:
                    print(f"      → {d}")

            change_report = {
                "company":        company_name,
                "detected_at":    datetime.now().isoformat(),
                "compared_with":  previous["timestamp"],
                "changes":        all_changes,
                "stale_features": stale_features,
            }
            report_path = os.path.join(SNAPSHOT_DIR, f"{company_name}_changes.json")
            with open(report_path, "w") as f:
                json.dump(change_report, f, indent=2)
            print(f"\n  [report]  → {report_path}")
        else:
            print("\n  ✓ No changes since last snapshot")
    else:
        print("\n  ℹ  No previous snapshot — saving as baseline")

    save_snapshot(company_name, new_data)
    return new_data


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT — edit this list to add competitors
# ═══════════════════════════════════════════════════════════════

COMPETITORS = [
    # (company_slug, domain, capterra_id, capterra_slug)
    # To find Capterra ID: search the product on capterra.com, then grab the
    # number from the URL: /p/{ID}/{Slug}/  (e.g. /p/152373/HubSpot-CRM/)
    # Set capterra_id=None to skip the complaints feature for that company.
    ("hubspot", "hubspot.com", "152373", "HubSpot-CRM"),
    # ("pipedrive",  "pipedrive.com",  "146091", "Pipedrive"),
    # ("salesforce", "salesforce.com", "17680",  "Salesforce"),
    # ("monday",     "monday.com",     "168875", "monday-com"),
]


SAVED_COMPETITORS_FILE = "competitors_saved.json"


_SAVED_COMPETITORS_VERSION = 1


def load_saved_competitors() -> list:
    """
    Load competitors saved from previous interactive sessions.
    File format: {"version": 1, "entries": [[company, domain, capterra_id, capterra_slug], ...]}
    Falls back gracefully if the file is the old bare-list format (pre-versioning).
    """
    if not os.path.exists(SAVED_COMPETITORS_FILE):
        return []
    with open(SAVED_COMPETITORS_FILE) as f:
        data = json.load(f)
    # Migrate bare-list format written before schema versioning was added
    if isinstance(data, list):
        print(f"  ⓘ Migrating {SAVED_COMPETITORS_FILE} to versioned format")
        _write_saved_competitors(data)
        return data
    if data.get("version") != _SAVED_COMPETITORS_VERSION:
        abs_path = os.path.abspath(SAVED_COMPETITORS_FILE)
        print(
            f"\n  ⛔  {abs_path} has schema version {data.get('version')!r} "
            f"but this script expects version {_SAVED_COMPETITORS_VERSION}.\n"
            f"     Saved competitors will NOT be loaded. Rename or delete the file\n"
            f"     to start fresh, or manually edit 'version' to {_SAVED_COMPETITORS_VERSION}.\n"
        )
        return []
    return data.get("entries", [])


def _write_saved_competitors(entries: list) -> None:
    with open(SAVED_COMPETITORS_FILE, "w") as f:
        json.dump({"version": _SAVED_COMPETITORS_VERSION, "entries": entries}, f, indent=2)


def add_saved_competitor(
    company: str, domain: str, capterra_id: str | None, capterra_slug: str | None
) -> None:
    """Persist a competitor entry so it shows up in future batch runs."""
    entries = load_saved_competitors()
    if any(e[0] == company and e[1] == domain for e in entries):
        print(f"  ⓘ {company} ({domain}) is already in the saved list")
        return
    entries.append([company, domain, capterra_id, capterra_slug])
    _write_saved_competitors(entries)
    print(f"  ✓ Saved to {SAVED_COMPETITORS_FILE} — will appear in future batch runs")


def prompt_interactive() -> tuple[str, str, str | None, str | None]:
    """
    Ask the user for company details. Returns (company, domain, capterra_id, capterra_slug).
    Capterra fields are None if the user just hits Enter.
    """
    print("\n" + "═" * 60)
    print("  Interactive mode")
    print("═" * 60)

    def _normalise_company(raw: str) -> str:
        return re.sub(r'\s+', '-', raw.strip().lower())

    company = _normalise_company(input("Company name (e.g. hubspot): "))
    while not company:
        company = _normalise_company(input("Company name (required): "))

    domain = input(f"Primary domain (e.g. hubspot.com): ").strip().lower()
    while not domain:
        domain = input("Domain (required, e.g. hubspot.com): ").strip().lower()
    # Strip protocol/www if user pasted a full URL
    domain = re.sub(r'^https?://', '', domain)
    domain = re.sub(r'^www\.', '', domain)
    domain = domain.rstrip('/')

    # Basic format validation — must look like something.tld
    _domain_re = re.compile(
        r'^(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$'
    )
    while not _domain_re.match(domain):
        print(f"  ⚠  '{domain}' doesn't look like a valid domain (expected: thing.tld)")
        domain = input("Domain (required, e.g. hubspot.com): ").strip().lower()
        domain = re.sub(r'^https?://', '', domain)
        domain = re.sub(r'^www\.', '', domain)
        domain = domain.rstrip('/')

    print("\nOptional: Capterra ID + slug for complaints data.")
    print("  Find at capterra.com — search the product, grab the number")
    print("  from the URL path /p/{ID}/{Slug}/.")
    print("  Press Enter twice to skip.")
    capterra_id = input("Capterra ID (optional, digits only): ").strip() or None
    if capterra_id and not re.match(r'^\d+$', capterra_id):
        print(f"  ⚠  '{capterra_id}' is not a numeric ID — did you paste the slug? Skipping complaints.")
        capterra_id = None
    capterra_slug = None
    if capterra_id:
        capterra_slug = input("Capterra slug (optional): ").strip() or None
        if not capterra_slug:
            print("  ⓘ Slug missing — complaints will be skipped")
            capterra_id = None

    return company, domain, capterra_id, capterra_slug


async def main():
    # Two modes: batch (use COMPETITORS list) or interactive (prompt user).
    # If COMPETITORS is non-empty, ask which mode to use.
    if COMPETITORS:
        print("\nFound COMPETITORS list with", len(COMPETITORS), "entries.")
        choice = input("Run [b]atch from list, [i]nteractive, or [q]uit? ").strip().lower()
    else:
        choice = "i"

    if choice == "q":
        print("Bye.")
        return

    if choice == "b":
        # Merge hardcoded COMPETITORS with any saved from interactive sessions
        saved = load_saved_competitors()
        saved_tuples = [tuple(e) for e in saved]
        all_competitors = list(COMPETITORS) + [e for e in saved_tuples if e not in COMPETITORS]
        if saved:
            print(f"  (+ {len(saved)} saved from previous interactive sessions)")
        for i, entry in enumerate(all_competitors):
            if len(entry) == 2:
                company, domain = entry
                capterra_id, capterra_slug = None, None
            elif len(entry) == 4:
                company, domain, capterra_id, capterra_slug = entry
            else:
                print(f"⚠️  Skipping malformed COMPETITORS entry: {entry}")
                continue
            await analyze_competitor(company, domain, capterra_id, capterra_slug)
            if i < len(all_competitors) - 1:
                print("\n  ⏳ Pausing 5s before next competitor...")
                await asyncio.sleep(5)
    else:
        # Interactive — allow multiple companies in one session
        while True:
            company, domain, capterra_id, capterra_slug = prompt_interactive()
            await analyze_competitor(company, domain, capterra_id, capterra_slug)
            save_it = input("\nSave to reusable list for future batch runs? [y/N]: ").strip().lower()
            if save_it == "y":
                add_saved_competitor(company, domain, capterra_id, capterra_slug)
            again = input("\nAnalyze another company? [y/N]: ").strip().lower()
            if again != "y":
                break

    print(f"\n{'═'*60}")
    print("  Done. Check the snapshots/ directory.")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    asyncio.run(main())