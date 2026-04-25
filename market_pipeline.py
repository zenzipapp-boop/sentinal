#!/usr/bin/env python3
"""
Weekly Satellite Portfolio Auditor
──────────────────────────────────
Headless Linux-only pipeline for weekly satellite review.

Workflow:
    1. Load satellites.json and watchlist.json.
    2. Fetch OHLCV/volume history with yfinance.
    3. Extract authenticated Screener.in tables through browser-use MCP.
    4. Run three local Ollama stages sequentially:
         a. Qwen 3 30B — quality screen
         b. GPT-OSS 20B — 3-paragraph thesis
         c. Gemma 4 26B — skeptical audit and invalidation triggers
    5. Size the weekly SIP using conviction buckets.
    6. Atomically update satellites.json with thesis_history and audit_log.
    7. Write SATELLITE_REPORT_YYYY-MM-DD.md for Claude synthesis.

CLI:
    python market_pipeline.py
    python market_pipeline.py --watchlist my_stocks.json
    python market_pipeline.py --add    "DIXON.NS:840:30"
    python market_pipeline.py --exit    "DIXON.NS"
    python market_pipeline.py --screener "DIXON.NS"

Notes:
    - Browser Use MCP is the primary Screener.in extraction path.
    - Direct HTTP parsing is only a fallback if MCP/auth is unavailable.
    - Models are run one at a time with explicit teardown to respect 16 GB VRAM.
"""

import argparse
import asyncio
import copy
import importlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, cast


REQUIRED_THIRD_PARTY_IMPORTS: list[tuple[str, str]] = [
    ("yfinance", "yfinance"),
    ("pandas", "pandas"),
    ("numpy", "numpy"),
    ("requests", "requests"),
    ("bs4", "beautifulsoup4"),
    ("feedparser", "feedparser"),
    ("dotenv", "python-dotenv"),
    ("pypdf", "pypdf"),
    ("langchain_ollama", "langchain-ollama"),
]


def _dependency_guard() -> None:
    missing: list[tuple[str, str]] = []
    for module_name, package_name in REQUIRED_THIRD_PARTY_IMPORTS:
        try:
            importlib.import_module(module_name)
        except Exception:
            missing.append((module_name, package_name))

    if not missing:
        return

    missing_modules = ", ".join(module for module, _ in missing)
    install_targets = " ".join(dict.fromkeys(package for _, package in missing))
    message = (
        "Missing required Python modules: "
        f"{missing_modules}\n"
        "Install them with:\n"
        f"  pip install {install_targets}"
    )
    print(message, file=sys.stderr)
    raise SystemExit(1)


_dependency_guard()

import feedparser
from dotenv import load_dotenv
import numpy as np  # noqa: F401 - startup dependency guard requires this module
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from browser_use import Agent
from browser_use.browser import BrowserProfile
from browser_use.llm import ChatOpenAI

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("satellite")

# ─── Constants ────────────────────────────────────────────────────────────────
TODAY       = datetime.now().strftime("%Y-%m-%d")
WEEK_NO     = datetime.now().isocalendar()[1]

OLLAMA_BASE     = "http://localhost:11434"
OLLAMA_CHAT_URL = f"{OLLAMA_BASE}/api/chat"
OLLAMA_GEN_URL  = f"{OLLAMA_BASE}/api/generate"

MODEL_SCREENER  = "qwen3.6:35b"      # Quality screener — thorough reasoner
MODEL_THESIS    = "gpt-oss:20b"    # Thesis writer — narrative builder
MODEL_AUDITOR   = "gemma4:26b"     # Thesis auditor — devil's advocate
OLLAMA_KEEP_ALIVE = "0s"
STAGE_MODELS = (MODEL_SCREENER, MODEL_THESIS, MODEL_AUDITOR)

BROWSER_USE_API_KEY = ""
BROWSER_USE_SHOW_LIVE = False
SCREENER_COOKIE = os.environ.get("SCREENER_SESSION_ID", "").strip()

OLLAMA_BASE = "http://localhost:11434"
OLLAMA_MODEL = "qwen3.6:35b"
CHROME_USER_DATA_DIR = Path.home() / ".config/google-chrome-pipeline"
CHROME_PROFILE_DIR = "PipelineBot"

_NSE_SESSION = None

SIP_MONTHLY     = 5000            # ₹ per month
SIP_WEEKLY      = SIP_MONTHLY / 4  # ₹2,500 deployed per week
MAX_SATELLITES  = 5
MIN_SATELLITES  = 3
MAX_ALLOC_PCT   = 40               # no single satellite > 40% of satellite book
OHLCV_DAYS      = 365              # 1 year for trend + valuation context

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.resolve()

SATELLITES_FILE  = BASE_DIR / "satellites.json"
WATCHLIST_FILE   = BASE_DIR / "watchlist.json"
PROMPTS_FILE     = BASE_DIR / "prompts.json"
SCREENER_DIR     = BASE_DIR / "screener_data"
ANNUAL_REPORTS_DIR = BASE_DIR / "annual_reports"
ANNUAL_REPORTS_SUMMARY_DIR = ANNUAL_REPORTS_DIR / "processed"
BUDGET_DIR       = BASE_DIR / "govt_budgets"
BUDGET_SUMMARY_DIR = BUDGET_DIR / "processed"
REPORTS_DIR      = BASE_DIR / "reports"
OUTPUT_MD        = REPORTS_DIR / f"SATELLITE_REPORT_{TODAY}.md"
DETAILED_OUTPUT_MD = REPORTS_DIR / f"detailed_satellite_report_{TODAY}.md"
SCREENER_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# ─── Portfolio data paths ──────────────────────────────────────────────────────
PORTFOLIO_DATA_DIR            = BASE_DIR / "portfolio_data"
PORTFOLIO_CURRENT_CORES_DIR   = PORTFOLIO_DATA_DIR / "current" / "cores"
PORTFOLIO_CURRENT_SATS_DIR    = PORTFOLIO_DATA_DIR / "current" / "satellites"
PORTFOLIO_WATCHLIST_CORES_DIR = PORTFOLIO_DATA_DIR / "watchlist" / "cores"
PORTFOLIO_WATCHLIST_SATS_DIR  = PORTFOLIO_DATA_DIR / "watchlist" / "satellites"
PORTFOLIO_EXITED_DIR          = PORTFOLIO_DATA_DIR / "exited"

# ─── Reports paths ────────────────────────────────────────────────────────────
REPORTS_CURRENT_CORES_DIR     = REPORTS_DIR / "current" / "cores"
REPORTS_CURRENT_SATS_DIR      = REPORTS_DIR / "current" / "satellites"
REPORTS_WATCHLIST_CORES_DIR   = REPORTS_DIR / "watchlist" / "cores"
REPORTS_WATCHLIST_SATS_DIR    = REPORTS_DIR / "watchlist" / "satellites"
REPORTS_EXITED_DIR            = REPORTS_DIR / "exited"

ANNUAL_REPORT_CONTEXT_WARNINGS: dict[str, str] = {}
STAGE_RETRY_DELAY_SECONDS = 5
STAGE_MAX_RETRIES = 2
SPARKLINE_BLOCKS = "▁▂▃▄▅▆▇█"

# ─── Prompts config ───────────────────────────────────────────────────────────
def _load_prompts() -> dict:
    if PROMPTS_FILE.exists():
        try:
            return json.loads(PROMPTS_FILE.read_text(encoding="utf-8"))
        except Exception:
            log.warning("Could not load %s — using built-in prompts", PROMPTS_FILE)
    return {}

PROMPTS: dict = _load_prompts()


_STAGE_MODEL_FALLBACKS = {1: MODEL_SCREENER, 2: MODEL_THESIS, 3: MODEL_AUDITOR, 4: MODEL_SCREENER}


def _stage_model(category: str, stage: int) -> str:
    key = "cores" if category.lower().startswith("core") else "satellites"
    return PROMPTS.get(key, {}).get("models", {}).get(f"stage{stage}", _STAGE_MODEL_FALLBACKS.get(stage, MODEL_SCREENER))


def _stage_prompt_template(category: str, stage: int) -> str:
    key = "cores" if category.lower().startswith("core") else "satellites"
    raw = PROMPTS.get(key, {}).get("prompts", {}).get(f"stage{stage}", "")
    if not raw:
        return ""
    # prompts.json stores literal braces in JSON schema examples; escape them so
    # .format() works, then restore the {var_name} placeholders.
    var_names = re.findall(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', raw)
    escaped = raw.replace("{", "{{").replace("}", "}}")
    for var in set(var_names):
        escaped = escaped.replace("{{" + var + "}}", "{" + var + "}")
    return escaped


class BrowserUseUnavailable(RuntimeError):
    """Raised when browser-use MCP cannot be used and a fallback is required."""


def current_week_key() -> str:
    return f"{datetime.now().year}-W{WEEK_NO:02d}"


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if value == "":
                return default
        number = float(value)
        if number != number:
            return default
        return number
    except Exception:
        return default


def _safe_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if value == "":
                return default
        return int(float(value))
    except Exception:
        return default


def _ensure_dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _ensure_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        delete=False,
        encoding=encoding,
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    ) as handle:
        handle.write(content)
        temp_name = handle.name
    Path(temp_name).replace(path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, ensure_ascii=False))


def backup_satellites_file(max_backups: int = 4) -> None:
    if not SATELLITES_FILE.exists():
        return

    backup_path = SATELLITES_FILE.with_name(f"satellites_backup_{TODAY}.json")
    try:
        atomic_write_text(backup_path, SATELLITES_FILE.read_text(encoding="utf-8"))
        log.info("Created satellites backup → %s", backup_path.name)
    except Exception as exc:
        log.warning("Failed to create satellites backup: %s", exc)
        return

    backups = sorted(SATELLITES_FILE.parent.glob("satellites_backup_*.json"), key=lambda path: path.name)
    excess = len(backups) - max_backups
    if excess <= 0:
        return

    for stale_path in backups[:excess]:
        try:
            stale_path.unlink()
            log.info("Removed old satellites backup → %s", stale_path.name)
        except Exception as exc:
            log.warning("Failed to remove old backup %s: %s", stale_path.name, exc)


def _load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("[%s] JSON read failed: %s", path.name, exc)
        return default


def _append_data_warning(payload: dict[str, Any], warning_text: str) -> None:
    existing = payload.get("data_warnings")
    if isinstance(existing, list):
        existing.append(warning_text)
    else:
        payload["data_warnings"] = [warning_text]


def _cache_staleness_warning(path: Path, ticker: str, *, max_age_days: int = 7) -> str | None:
    try:
        modified_ts = path.stat().st_mtime
    except OSError:
        return None
    age_seconds = time.time() - modified_ts
    if age_seconds <= max_age_days * 86400:
        return None

    age_days = age_seconds / 86400
    modified_date = datetime.fromtimestamp(modified_ts).strftime("%Y-%m-%d")
    return (
        f"[{ticker}] Screener cache is stale ({path.name}: {age_days:.1f} days old; "
        f"last modified {modified_date}). Refresh recommended."
    )


def _table_to_records(table: Any) -> dict[str, Any]:
    try:
        frame = pd.read_html(StringIO(str(table)), header=0)[0]
        frame = frame.fillna("")
        frame.columns = [str(column).strip() for column in frame.columns]
        rows: list[dict[str, Any]] = []
        for row in frame.to_dict(orient="records"):
            cleaned_row = {}
            for key, value in row.items():
                if hasattr(value, "item"):
                    try:
                        value = value.item()
                    except Exception:
                        value = str(value)
                cleaned_row[str(key).strip()] = value if value == value else None
            rows.append(cleaned_row)
        return {
            "columns": [str(column).strip() for column in frame.columns],
            "rows": rows,
        }
    except Exception:
        soup = BeautifulSoup(str(table), "html.parser")
        raw_rows: list[list[str]] = []
        for tr in soup.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
            if cells:
                raw_rows.append(cells)
        if not raw_rows:
            return {"columns": [], "rows": []}
        columns = raw_rows[0]
        records: list[dict[str, Any]] = []
        for row in raw_rows[1:]:
            record = {columns[idx] if idx < len(columns) else f"col_{idx}": value for idx, value in enumerate(row)}
            records.append(record)
        return {"columns": columns, "rows": records}


def extract_screener_sections(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    result = {}

    def _recent_first_limit(table_data: dict[str, Any]) -> dict[str, Any]:
        columns = list(table_data.get("columns", []))
        rows = list(table_data.get("rows", []))
        if len(columns) <= 1 or not rows:
            return {"columns": columns, "rows": rows}

        first_column_values = [row.get(columns[0]) for row in rows if isinstance(row, dict) and row.get(columns[0]) not in (None, "")]
        if not first_column_values:
            return {"columns": columns, "rows": rows}

        if not all(isinstance(value, str) for value in first_column_values):
            return {"columns": columns, "rows": rows}

        first_column_text_values = [value for value in first_column_values if isinstance(value, str)]
        if not any(re.search(r"[A-Za-z]", value) for value in first_column_text_values):
            return {"columns": columns, "rows": rows}

        data_columns = list(reversed(columns[1:]))[:5]
        columns = [columns[0], *data_columns]

        reordered_rows = []
        for row in rows:
            if not isinstance(row, dict):
                reordered_rows.append(row)
                continue

            rebuilt_row = {columns[0]: row.get(columns[0])}
            for column in data_columns:
                rebuilt_row[column] = row.get(column)
            reordered_rows.append(rebuilt_row)

        return {"columns": columns, "rows": reordered_rows}

    # key_ratios — two sources on the page, merge both
    top_ratios_rows = []
    top = soup.find(id="top-ratios")
    if top:
        for li in top.find_all("li"):
            name = li.find("span", class_="name")
            value = li.find("span", class_="value") or li.find("span", class_="number")
            if name and value:
                top_ratios_rows.append({
                    "name": name.get_text(" ", strip=True),
                    "value": value.get_text(" ", strip=True),
                })

    ratios_section = soup.find(id="ratios")
    ratios_table = {"columns": [], "rows": []}
    if ratios_section:
        table = ratios_section.find("table")
        if table:
            ratios_table = _recent_first_limit(_table_to_records(table))

    result["key_ratios"] = {
        "top_ratios": top_ratios_rows,
        "columns": ratios_table.get("columns", []),
        "rows": ratios_table.get("rows", []),
    }

    # quarterly_results
    quarterly = soup.find(id="quarterly-results")
    if not quarterly:
        quarterly = soup.find(id="quarters")
    if quarterly:
        table = quarterly.find("table")
        result["quarterly_results"] = _recent_first_limit(_table_to_records(table)) if table else {"columns": [], "rows": []}
    else:
        result["quarterly_results"] = {"columns": [], "rows": []}

    # profit-loss
    pl = soup.find(id="profit-loss")
    if pl:
        table = pl.find("table")
        result["profit_loss"] = _recent_first_limit(_table_to_records(table)) if table else {"columns": [], "rows": []}

    # balance sheet
    bs = soup.find(id="balance-sheet")
    if bs:
        table = bs.find("table")
        result["balance_sheet"] = _recent_first_limit(_table_to_records(table)) if table else {"columns": [], "rows": []}

    # cash flows
    cf = soup.find(id="cash-flow")
    if cf:
        table = cf.find("table")
        result["cash_flows"] = _recent_first_limit(_table_to_records(table)) if table else {"columns": [], "rows": []}

    # shareholding
    sh = soup.find(id="shareholding")
    if sh:
        table = sh.find("table")
        result["shareholding_pattern"] = _recent_first_limit(_table_to_records(table)) if table else {"columns": [], "rows": []}
    else:
        result["shareholding_pattern"] = {"columns": [], "rows": []}

    return result


def summarize_table(section: dict[str, Any] | None, max_rows: int = 2) -> str:
    if not isinstance(section, dict):
        return "Unavailable"

    rows = section.get("rows", [])
    if not rows:
        return "No rows extracted"

    lines = []
    for row in rows[:max_rows]:
        if isinstance(row, dict):
            items = [(k, v) for k, v in list(row.items())[:5] if not str(k).startswith("Unnamed")]
            if items:
                cols = " | ".join(f"{k}={v}" for k, v in items)
            else:
                cols = " | ".join(str(v) for v in list(row.values())[:5])
            lines.append(cols)
        elif isinstance(row, list):
            cols = " | ".join(str(v) for v in row[:5])
            lines.append(cols)
        else:
            lines.append(str(row))
    return " | ".join(lines) if lines else "No data"


def _compact_json(value: Any, limit: int = 1200) -> str:
    if value is None:
        return "{}"
    text = json.dumps(value, indent=2, ensure_ascii=False)
    if len(text) <= limit:
        return text
    return text[: limit - 20].rstrip() + "\n..."


def _normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def _find_matching_subdir(root: Path, ticker: str) -> Path | None:
    if not root.exists():
        return None

    wanted = {
        _normalize_label(ticker),
        _normalize_label(screener_company_slug(ticker)),
    }

    for child in root.iterdir():
        if not child.is_dir():
            continue
        label = _normalize_label(child.name)
        if label in wanted or any(label in target or target in label for target in wanted):
            return child
    return None


def load_annual_report_context(ticker: str, limit: int = 2200, *, report_root: Path | None = None, annual_report_years: int = 3) -> str:
    if report_root is None:
        report_root = _find_matching_subdir(ANNUAL_REPORTS_SUMMARY_DIR, ticker)
    if report_root is None:
        ANNUAL_REPORT_CONTEXT_WARNINGS.pop(ticker, None)
        return "No annual report summary available."

    def _is_placeholder_summary(value: Any) -> bool:
        if not isinstance(value, dict):
            return False
        text = str(value.get("portfolio_summary", "") or value.get("one_line_summary", "") or "").strip().lower()
        if not text:
            return False
        return "skipped" in text or "unavailable" in text

    def _is_raw_text_placeholder(value: Any) -> bool:
        if value is None:
            return False
        return "raw text extracted" in str(value).lower()

    def _warn_placeholder_only() -> None:
        warning_text = (
            f"[{ticker}] Annual report context is placeholder-only ('raw text extracted'); "
            "it will not add meaningful signal to LLM stages."
        )
        if ANNUAL_REPORT_CONTEXT_WARNINGS.get(ticker) == warning_text:
            return
        ANNUAL_REPORT_CONTEXT_WARNINGS[ticker] = warning_text
        log.warning(warning_text)

    index_path = report_root / "index.json"
    if index_path.exists():
        try:
            payload = json.loads(index_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                reports = payload.get("reports")
                years = payload.get("years", [])

                if isinstance(reports, list) and reports:
                    placeholders = [_is_raw_text_placeholder(report) for report in reports]
                    if placeholders and all(placeholders):
                        _warn_placeholder_only()
                    else:
                        ANNUAL_REPORT_CONTEXT_WARNINGS.pop(ticker, None)

                    recent_reports = reports[:annual_report_years] if years else reports[:annual_report_years]
                else:
                    ANNUAL_REPORT_CONTEXT_WARNINGS.pop(ticker, None)
                    recent_reports = []

                for key in ("combined_summary", "summary", "annual_report_summary"):
                    value = payload.get(key)
                    if value and not _is_placeholder_summary(value):
                        return _compact_json(value, limit=limit) if isinstance(value, (dict, list)) else str(value)[:limit]

                if recent_reports:
                    return _compact_json({"reports": recent_reports}, limit=limit)
        except Exception as exc:
            log.warning("[%s] Failed to read annual report index: %s", ticker, exc)
            ANNUAL_REPORT_CONTEXT_WARNINGS.pop(ticker, None)

    summary_files = sorted((report_root / "summaries").glob("*.json"))
    collected: list[dict[str, Any]] = []
    for path in summary_files[:annual_report_years]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            collected.append(
                {
                    "file": path.name,
                    "report_title": payload.get("report_title", path.stem),
                    "one_line_summary": payload.get("one_line_summary", payload.get("summary", "")),
                    "main_points": payload.get("main_points", []),
                    "risks": payload.get("risks", payload.get("risk_flags", [])),
                    "thesis_implications": payload.get("thesis_implications", []),
                }
            )

    if collected:
        ANNUAL_REPORT_CONTEXT_WARNINGS.pop(ticker, None)
        return _compact_json({"reports": collected}, limit=limit)
    ANNUAL_REPORT_CONTEXT_WARNINGS.pop(ticker, None)
    return "No annual report summary available."


def load_budget_context(years: int = 2, limit: int = 1500) -> str:
    """Load and format government budget summaries for LLM prompts. Returns explicit fallback if not available."""
    if not BUDGET_SUMMARY_DIR.exists():
        return "No budget data available. Proceed without macro budget context."

    index_path = BUDGET_SUMMARY_DIR / "index.json"
    if not index_path.exists():
        return "No budget data available. Proceed without macro budget context."

    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return "No budget data available. Proceed without macro budget context."

        year_list = payload.get("years", [])
        summaries = payload.get("summaries", {})

        if not year_list or not summaries:
            return "No budget data available. Proceed without macro budget context."

        recent_years = year_list[:years]
        formatted_parts: list[str] = []

        for year in recent_years:
            budget = summaries.get(year)
            if not budget or not isinstance(budget, dict):
                continue

            fiscal_stance = budget.get("fiscal_stance", "unknown")
            fiscal_deficit = budget.get("fiscal_deficit_gdp", "N/A")
            capex = budget.get("capex_outlay", "N/A")

            formatted_parts.append(f"BUDGET {year}: {fiscal_stance} | Deficit: {fiscal_deficit} | Capex: {capex}")

            pli_schemes = budget.get("pli_schemes", [])
            if pli_schemes:
                pli_lines = []
                for scheme in pli_schemes[:5]:
                    if isinstance(scheme, dict):
                        name = scheme.get("scheme_name", "Unknown")
                        sector = scheme.get("sector", "")
                        allocation = scheme.get("allocation", "")
                        pli_lines.append(f"  • {name} ({sector}): {allocation}")
                if pli_lines:
                    formatted_parts.append("\nPLI SCHEMES:\n" + "\n".join(pli_lines))

            sector_tailwinds = budget.get("sector_tailwinds", [])
            if sector_tailwinds:
                tw_lines = []
                for tw in sector_tailwinds[:5]:
                    if isinstance(tw, dict):
                        sector = tw.get("sector", "")
                        equity_impact = tw.get("equity_impact", "")
                        if sector and equity_impact:
                            tw_lines.append(f"  • {sector}: {equity_impact}")
                if tw_lines:
                    formatted_parts.append("\nSECTOR TAILWINDS:\n" + "\n".join(tw_lines))

            sector_headwinds = budget.get("sector_headwinds", [])
            if sector_headwinds:
                hw_lines = []
                for hw in sector_headwinds[:5]:
                    if isinstance(hw, dict):
                        sector = hw.get("sector", "")
                        equity_impact = hw.get("equity_impact", "")
                        if sector and equity_impact:
                            hw_lines.append(f"  • {sector}: {equity_impact}")
                if hw_lines:
                    formatted_parts.append("\nSECTOR HEADWINDS:\n" + "\n".join(hw_lines))

            import_duties = budget.get("import_duty_changes", [])
            if import_duties:
                duty_lines = []
                for duty in import_duties[:5]:
                    if isinstance(duty, dict):
                        item = duty.get("item", "")
                        change = duty.get("change", "")
                        new_rate = duty.get("new_rate", "")
                        equity_impact = duty.get("equity_impact", "")
                        if item and equity_impact:
                            duty_lines.append(f"  • {item} ({change} to {new_rate}): {equity_impact}")
                if duty_lines:
                    formatted_parts.append("\nIMPORT DUTY CHANGES:\n" + "\n".join(duty_lines))

            tax_changes = budget.get("tax_changes", [])
            if tax_changes:
                tax_lines = []
                for tax in tax_changes[:5]:
                    if isinstance(tax, dict):
                        tax_type = tax.get("type", "")
                        description = tax.get("description", "")
                        equity_impact = tax.get("equity_impact", "")
                        if tax_type and equity_impact:
                            tax_lines.append(f"  • {tax_type}: {equity_impact}")
                if tax_lines:
                    formatted_parts.append("\nTAX CHANGES:\n" + "\n".join(tax_lines))

            formatted_parts.append("")

        if formatted_parts:
            formatted_text = "\n".join(formatted_parts)
            if len(formatted_text) > limit:
                formatted_text = formatted_text[:limit] + "..."
            return formatted_text

    except Exception as exc:
        log.warning("Failed to load budget context: %s", exc)

    return "No budget data available. Proceed without macro budget context."


def _escape_json_string_controls(text: str) -> str:
    repaired: list[str] = []
    in_string = False
    escaped = False

    for char in text:
        if in_string:
            if escaped:
                repaired.append(char)
                escaped = False
            elif char == "\\":
                repaired.append(char)
                escaped = True
            elif char == '"':
                repaired.append(char)
                in_string = False
            elif char == "\n":
                repaired.append("\\n")
            elif char == "\r":
                repaired.append("\\r")
            elif char == "\t":
                repaired.append("\\t")
            else:
                repaired.append(char)
        else:
            repaired.append(char)
            if char == '"':
                in_string = True

    return "".join(repaired)


def _normalize_json_candidate(candidate: str) -> str:
    candidate = re.sub(r",(\s*[}\]])", r"\1", candidate)
    return candidate


def get_nse_session() -> requests.Session:
    global _NSE_SESSION
    if _NSE_SESSION:
        return _NSE_SESSION

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com/",
    })
    session.get("https://www.nseindia.com", timeout=10)
    session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
    time.sleep(1)
    _NSE_SESSION = session
    return session


def _sort_items_by_date_desc(items: list[dict[str, Any]], date_key: str) -> list[dict[str, Any]]:
    def _parse_date(value: Any) -> datetime:
        text = str(value or "").strip()
        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%d-%b-%Y",
            "%d-%b-%Y %H:%M:%S",
            "%d/%m/%Y",
            "%d/%m/%Y %H:%M:%S",
            "%Y%m%d",
        ):
            try:
                return datetime.strptime(text, fmt)
            except Exception:
                continue
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return datetime.min

    return sorted(items, key=lambda item: _parse_date(item.get(date_key)), reverse=True)


def fetch_nse_announcements(symbol: str) -> list[dict]:
    symbol = symbol.split(".")[0].upper()
    try:
        session = get_nse_session()
        url = f"https://www.nseindia.com/api/corp-info?symbol={symbol}&corpType=announcements"
        response = session.get(url, timeout=10)
        response.raise_for_status()
        payload = response.json()
        items = _ensure_dict_list(payload if isinstance(payload, list) else payload.get("data", payload.get("rows", [])) if isinstance(payload, dict) else [])
        announcements = []
        for item in items:
            title = str(item.get("subject", "")).strip()
            snippet_source = item.get("details", item.get("subject", ""))
            announcements.append({
                "title": title,
                "snippet": str(snippet_source or "")[:400],
                "source": "nse_announcement",
                "date": str(item.get("date", "")),
            })
        return _sort_items_by_date_desc(announcements, "date")[:10]
    except Exception as exc:
        log.warning("[%s] NSE announcements fetch failed: %s", symbol, exc)
        return []


def _extract_bse_scrip_code(payload: Any) -> str | None:
    candidates: list[dict[str, Any]] = []
    if isinstance(payload, list):
        candidates = _ensure_dict_list(payload)
    elif isinstance(payload, dict):
        for key in ("Table", "Table1", "data", "Data", "result", "Result"):
            candidates = _ensure_dict_list(payload.get(key))
            if candidates:
                break
        if not candidates:
            candidates = [payload]

    for item in candidates:
        for key in ("scripCode", "SCRIP_CD", "scrip_cd", "Security Code", "SECURITYCODE", "securityCode"):
            value = str(item.get(key, "")).strip()
            if value.isdigit() and len(value) == 6:
                return value
            if value:
                digits = re.sub(r"\D", "", value)
                if len(digits) == 6:
                    return digits
    return None


def fetch_bse_announcements(ticker: str) -> list[dict]:
    symbol = ticker.split(".")[0].upper()
    try:
        headers = {
            "Referer": "https://www.bseindia.com/",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
        }
        search_url = (
            "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
            f"?Group=&Scripcode=&industry=&segment=Equity&status=Active&scrip={symbol}"
        )
        search_response = requests.get(search_url, headers=headers, timeout=10)
        search_response.raise_for_status()
        scrip_code = _extract_bse_scrip_code(search_response.json())
        if not scrip_code:
            return []

        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        today = datetime.now().strftime("%Y%m%d")
        url = (
            "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
            f"?strCat=-1&strPrevDate={thirty_days_ago}&strScrip={scrip_code}"
            f"&strSearch=P&strToDate={today}&strType=C&subcategory=-1"
        )
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        payload = response.json()
        items = _ensure_dict_list(payload if isinstance(payload, list) else payload.get("Table", payload.get("data", [])) if isinstance(payload, dict) else [])
        announcements = []
        for item in items:
            headline = str(item.get("HEADLINE", "")).strip()
            announcements.append({
                "title": headline,
                "snippet": headline[:400],
                "source": "bse_announcement",
                "date": str(item.get("NEWS_DT", "")),
            })
        return _sort_items_by_date_desc(announcements, "date")[:10]
    except Exception as exc:
        log.warning("[%s] BSE announcements fetch failed: %s", symbol, exc)
        return []


def _extract_moneycontrol_article_text(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
        "Referer": "https://www.moneycontrol.com/",
    }
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for selector in ("div.content_wrapper", "div.article-desc", "div#contentdata", "div.arti-flow"):
        node = soup.select_one(selector)
        if node:
            text = node.get_text(" ", strip=True)
            if text:
                return text
    return ""


def fetch_moneycontrol_news(ticker: str) -> list[dict]:
    try:
        company_lower = ticker.split(".")[0].lower()
        feed = feedparser.parse(f"https://www.moneycontrol.com/rss/{company_lower}.rss")
        entries = list(getattr(feed, "entries", [])[:5])
        items: list[dict] = []
        for entry in entries:
            title = str(getattr(entry, "title", "")).strip()
            link = str(getattr(entry, "link", "")).strip()
            summary = str(getattr(entry, "summary", "")).strip()
            snippet = summary[:400]
            full_text = ""
            if link:
                try:
                    full_text = _extract_moneycontrol_article_text(link)
                except Exception:
                    full_text = ""
            items.append({
                "title": title,
                "snippet": (full_text[:600] if full_text else snippet),
                "source": "moneycontrol_full",
                "url": link,
            })
        return items
    except Exception as exc:
        log.warning("[%s] Moneycontrol news fetch failed: %s", ticker, exc)
        return []


def fetch_yfinance_news(ticker: str) -> list[dict]:
    try:
        entries = _ensure_dict_list(getattr(yf.Ticker(ticker), "news", []))[:5]
        items: list[dict] = []
        for entry in entries:
            title = str(entry.get("title", "")).strip()
            summary = str(entry.get("summary", entry.get("content", ""))).strip()
            provider = str(entry.get("publisher", "yfinance_news")).strip() or "yfinance_news"
            items.append({
                "title": title,
                "snippet": summary[:400],
                "source": provider,
                "url": str(entry.get("link", entry.get("url", ""))).strip(),
            })
        return items
    except Exception as exc:
        log.warning("[%s] yfinance news fetch failed: %s", ticker, exc)
        return []


def _rss_matches_company(entry: Any, company_tokens: list[str]) -> bool:
    haystack_parts = [
        str(getattr(entry, "title", "")),
        str(getattr(entry, "summary", "")),
        str(getattr(entry, "description", "")),
        str(getattr(entry, "link", "")),
    ]
    haystack = " ".join(haystack_parts).lower()
    return any(token in haystack for token in company_tokens if token)


def _rss_entries_to_items(entries: list[Any], source: str, company_tokens: list[str]) -> list[dict]:
    items: list[dict] = []
    for entry in entries:
        if company_tokens and not _rss_matches_company(entry, company_tokens):
            continue
        title = str(getattr(entry, "title", "")).strip()
        summary = str(getattr(entry, "summary", getattr(entry, "description", ""))).strip()
        link = str(getattr(entry, "link", "")).strip()
        items.append({
            "title": title,
            "snippet": summary[:400],
            "source": source,
            "url": link,
        })
    return items


def fetch_et_rss(ticker: str) -> list[dict]:
    try:
        company = ticker.split(".")[0].lower()
        company_tokens = [company, company.replace("&", "and")]
        feed_urls = [
            "https://economictimes.indiatimes.com/markets/stocks/news/rssfeeds/2146842.cms",
            "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        ]
        items: list[dict] = []
        for feed_url in feed_urls:
            feed = feedparser.parse(feed_url)
            items.extend(_rss_entries_to_items(list(getattr(feed, "entries", [])[:10]), "et_rss", company_tokens))
        return items[:5]
    except Exception as exc:
        log.warning("[%s] ET RSS fetch failed: %s", ticker, exc)
        return []


def fetch_bs_rss(ticker: str) -> list[dict]:
    try:
        company = ticker.split(".")[0].lower()
        company_tokens = [company, company.replace("&", "and")]
        feed_urls = [
            "https://www.business-standard.com/rss/markets-106.rss",
            "https://www.business-standard.com/rss/companies-101.rss",
        ]
        items: list[dict] = []
        for feed_url in feed_urls:
            feed = feedparser.parse(feed_url)
            items.extend(_rss_entries_to_items(list(getattr(feed, "entries", [])[:10]), "bs_rss", company_tokens))
        return items[:5]
    except Exception as exc:
        log.warning("[%s] BS RSS fetch failed: %s", ticker, exc)
        return []


def fetch_searxng(ticker: str, searxng_url: str) -> list[dict]:
    company = ticker.split(".")[0]
    all_items = []
    queries = [
        f"{company} India stock fundamentals results",
        f"{company} management guidance outlook sector",
    ]
    for query in queries:
        try:
            resp = requests.get(
                f"{searxng_url.rstrip('/')}/search",
                params={"q": query, "format": "json", "time_range": "month", "categories": "news"},
                timeout=20,
            )
            resp.raise_for_status()
            for r in resp.json().get("results", [])[:5]:
                all_items.append({
                    "title": r.get("title", ""),
                    "snippet": r.get("content", "")[:400],
                    "source": "searxng",
                    "url": r.get("url", ""),
                })
        except Exception as exc:
            log.warning(f"[{ticker}] News fetch failed: {exc}")

    return all_items


SCREENER_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "score": {"type": "number"},
        "quality_verdict": {"type": "string", "enum": ["QUALIFY", "WATCHLIST", "REJECT"]},
        "business_quality": {"type": "string"},
        "growth_runway": {"type": "string"},
        "valuation": {"type": "string"},
        "key_risks": {"type": "array", "items": {"type": "string"}},
        "thesis_assumptions": {"type": "array", "items": {"type": "string"}},
        "trend_read": {"type": "string"},
        "rationale": {"type": "string"},
        "input_quality_score": {"type": "number"},
        "confidence_score": {"type": "number"},
        "prompt_assessment": {"type": "string"},
    },
    "required": [
        "score",
        "quality_verdict",
        "business_quality",
        "growth_runway",
        "valuation",
        "key_risks",
        "thesis_assumptions",
        "trend_read",
        "rationale",
    ],
    "additionalProperties": True,
}


THESIS_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "thesis": {"type": "string"},
        "assumptions": {"type": "array", "items": {"type": "string"}},
        "bull_case": {"type": "string"},
        "bear_case": {"type": "string"},
        "time_horizon": {"type": "string"},
        "exit_triggers": {"type": "array", "items": {"type": "string"}},
        "changed_from_prior": {"type": "string"},
        "narrative_change": {"type": "string"},
        "input_quality_score": {"type": "number"},
        "confidence_score": {"type": "number"},
        "prompt_assessment": {"type": "string"},
    },
    "required": [
        "thesis",
        "assumptions",
        "bull_case",
        "bear_case",
        "time_horizon",
        "exit_triggers",
        "changed_from_prior",
        "narrative_change",
    ],
    "additionalProperties": True,
}


AUDITOR_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "score": {"type": "number"},
        "assumptions_status": {
            "type": "object",
            "additionalProperties": {"type": "boolean"},
        },
        "broken_assumptions": {"type": "array", "items": {"type": "string"}},
        "new_risks": {"type": "array", "items": {"type": "string"}},
        "invalidation_triggers": {"type": "array", "items": {"type": "string"}},
        "thesis_intact": {"type": "boolean"},
        "decision": {"type": "string", "enum": ["ADD", "HOLD", "TRIM", "EXIT"]},
        "decision_rationale": {"type": "string"},
        "add_recommended": {"type": "boolean"},
        "add_rationale": {"type": "string"},
        "red_flags": {"type": "array", "items": {"type": "string"}},
        "devils_advocate": {"type": "string"},
        "input_quality_score": {"type": "number"},
        "confidence_score": {"type": "number"},
        "prompt_assessment": {"type": "string"},
    },
    "required": [
        "score",
        "assumptions_status",
        "broken_assumptions",
        "new_risks",
        "invalidation_triggers",
        "thesis_intact",
        "decision",
        "decision_rationale",
        "add_recommended",
        "add_rationale",
        "red_flags",
        "devils_advocate",
    ],
    "additionalProperties": True,
}


async def run_blocking(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


async def run_browser_use_task(task: str) -> dict[str, Any]:
    """Run a task using browser_use Agent directly (no MCP server).

    Creates a visible Chrome window with Ollama LLM backing, runs the task,
    and returns the agent's output.
    """
    log.info("[BrowserUse] Starting Agent with task...")

    # Ensure Chrome user data directory exists
    CHROME_USER_DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # Create Ollama LLM via OpenAI-compatible endpoint
        llm = ChatOpenAI(
            model=OLLAMA_MODEL,
            base_url=f"{OLLAMA_BASE}/v1",
            api_key="ollama",
            temperature=0.0,
        )

        # Create browser profile: visible, with persistent Chrome profile path
        browser_profile = BrowserProfile(
            headless=False,  # Visible browser window
            user_data_dir=str(CHROME_USER_DATA_DIR),
            profile_directory=CHROME_PROFILE_DIR,
        )

        # Create and run agent
        agent = Agent(
            task=task,
            llm=llm,
            browser_profile=browser_profile,
        )

        log.info("[BrowserUse] Running Agent...")
        result = await agent.run(max_steps=50)

        # Extract the output from agent history
        if result and len(result) > 0:
            output = result.final_result()
            if output:
                log.info("[BrowserUse] Agent completed successfully")
                return {"output": output}

        log.warning("[BrowserUse] Agent returned empty result")
        return {}

    except Exception as exc:
        log.error(f"[BrowserUse] Agent failed: {exc}", exc_info=True)
        raise


async def call_browser_use_run_session(task: str) -> Any:
    """Run a browser automation task using browser_use Agent directly."""
    return await run_browser_use_task(task)


def _normalize_screener_payload(ticker: str, payload: dict[str, Any], source: str) -> dict[str, Any]:
    normalized = dict(payload or {})
    normalized["ticker"] = ticker
    normalized["updated"] = TODAY
    normalized["source"] = source
    normalized["key_ratios"] = normalized.get("key_ratios") or {"columns": [], "rows": []}
    normalized["shareholding_pattern"] = normalized.get("shareholding_pattern") or {"columns": [], "rows": []}
    normalized["quarterly_results"] = normalized.get("quarterly_results") or {"columns": [], "rows": []}
    return normalized


def get_screener_urls(slug: str) -> list[str]:
    return [
        f"https://www.screener.in/api/company/{slug}/consolidated/",
        f"https://www.screener.in/api/company/{slug}/",
    ]


def get_screener_page_urls(slug: str) -> list[str]:
    return [
        f"https://www.screener.in/company/{slug}/consolidated/",
        f"https://www.screener.in/company/{slug}/",
    ]


async def fetch_screener_via_browser_use(ticker: str, *, show_live: bool = False) -> dict[str, Any]:
    company_slug = screener_company_slug(ticker)
    task = textwrap.dedent(
        f"""
                Navigate to https://www.screener.in/company/{company_slug}/consolidated/ and extract
                key_ratios, shareholding_pattern, and quarterly_results tables.
                Return JSON only with this exact structure:
                {{
                    'ticker': '{ticker}',
                    'company_url': 'https://www.screener.in/company/{company_slug}/consolidated/',
                    'key_ratios': {{'columns': [...], 'rows': [...]}},
                    'shareholding_pattern': {{'columns': [...], 'rows': [...]}},
                    'quarterly_results': {{'columns': [...], 'rows': [...]}}
                }}
                Preserve column labels exactly as shown on the page.
                Return empty columns/rows for any table that cannot be located.
        """
    ).strip()

    result = await call_browser_use_run_session(task)
    if not isinstance(result, dict):
        raise BrowserUseUnavailable("browser-use did not return structured result")

    # Extract output from Agent result
    output = result.get("output")
    if isinstance(output, str):
        # Try to parse JSON string
        try:
            payload = json.loads(output)
        except (json.JSONDecodeError, ValueError):
            raise BrowserUseUnavailable("browser-use output is not valid JSON")
    elif isinstance(output, dict):
        payload = output
    else:
        raise BrowserUseUnavailable("browser-use output is not a dict or JSON string")

    if not all(key in payload for key in ("key_ratios", "shareholding_pattern", "quarterly_results")):
        raise BrowserUseUnavailable("browser-use did not return screener tables")

    payload.setdefault("company_url", f"https://www.screener.in/company/{company_slug}/consolidated/")
    return _normalize_screener_payload(ticker, payload, source="browser-use")


def fetch_screener_via_http(ticker: str) -> dict[str, Any]:
    company_slug = screener_company_slug(ticker)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    response = None
    url = None
    for candidate_url in get_screener_page_urls(company_slug):
        candidate_response = requests.get(candidate_url, timeout=30, headers=headers)
        if candidate_response.status_code == 404:
            continue
        candidate_response.raise_for_status()
        response = candidate_response
        url = candidate_url
        break
    if response is None or url is None:
        raise RuntimeError("Screener page not found")
    sections = extract_screener_sections(response.text)
    return _normalize_screener_payload(
        ticker,
        {
            "company_url": url,
            **sections,
            "profit_loss": sections.get("profit_loss", {"columns": [], "rows": []}),
            "balance_sheet": sections.get("balance_sheet", {"columns": [], "rows": []}),
            "cash_flows": sections.get("cash_flows", {"columns": [], "rows": []}),
        },
        source="fallback-http",
    )


def fetch_screener_json_api(ticker: str) -> dict[str, Any]:
    if not SCREENER_COOKIE:
        raise RuntimeError("SCREENER_SESSION_ID not set")

    company_slug = screener_company_slug(ticker)
    response = None
    url = None
    for candidate_url in get_screener_urls(company_slug):
        candidate_response = requests.get(
            candidate_url,
            timeout=20,
            headers={
                "Referer": f"https://www.screener.in/company/{company_slug}/consolidated/",
                "X-Requested-With": "XMLHttpRequest",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
            },
            cookies={"sessionid": SCREENER_COOKIE},
        )
        if candidate_response.status_code == 404:
            continue
        if candidate_response.status_code in {401, 403}:
            raise RuntimeError("Screener session expired")
        candidate_response.raise_for_status()
        response = candidate_response
        url = candidate_url
        break
    if response is None or url is None:
        raise RuntimeError("Screener JSON API returned 404 for all known URLs")

    raw_payload = response.json()
    if not isinstance(raw_payload, dict):
        raise RuntimeError("Unexpected Screener JSON API payload")

    payload = {
        "company_url": f"https://www.screener.in/company/{company_slug}/consolidated/",
        "key_ratios": raw_payload.get("ratios"),
        "quarterly_results": raw_payload.get("quarters"),
        "shareholding_pattern": raw_payload.get("shareholding"),
        "balance_sheet": raw_payload.get("balance_sheet"),
        "cash_flows": raw_payload.get("cash_flows"),
        "peers": raw_payload.get("peers"),
    }
    return _normalize_screener_payload(ticker, payload, source="screener-json-api")


async def fetch_screener_snapshot(ticker: str, *, show_live: bool = False) -> dict[str, Any]:
    cache_path = SCREENER_DIR / f"{ticker}.json"
    company_slug = screener_company_slug(ticker)

    # Step 1 — JSON API
    if SCREENER_COOKIE:
        try:
            payload = await run_blocking(fetch_screener_json_api, ticker)
            atomic_write_json(cache_path, payload)
            log.info("[%s] Screener snapshot captured via Screener JSON API", ticker)
            return payload
        except Exception as exc:
            log.warning("[%s] Screener JSON API failed: %s", ticker, exc)
    else:
        log.info("[%s] SCREENER_SESSION_ID not set, skipping JSON API", ticker)

    # Step 2 — Browser Use MCP
    log.info("[%s] Attempting Browser Use MCP to fetch screener data...", ticker)
    try:
        payload = await fetch_screener_via_browser_use(ticker, show_live=show_live)
        atomic_write_json(cache_path, payload)
        log.info("[%s] Screener snapshot captured via browser-use MCP", ticker)
        return payload
    except Exception as exc:
        log.error("[%s] Browser Use MCP failed (will fall through to next method): %s", ticker, exc)

    # Step 3 — local cache
    cached = _load_json_file(cache_path, {})
    if isinstance(cached, dict) and cached:
        log.info("[%s] Loaded cached screener snapshot", ticker)
        cached.setdefault("source", "cache")
        cached.setdefault("ticker", ticker)
        staleness_warning = _cache_staleness_warning(cache_path, ticker)
        if staleness_warning:
            log.warning(staleness_warning)
            _append_data_warning(cached, staleness_warning)
        return cached

    # Step 4 — fallback HTTP
    try:
        payload = await run_blocking(fetch_screener_via_http, ticker)
        atomic_write_json(cache_path, payload)
        log.info("[%s] Screener via fallback HTTP", ticker)
        return payload
    except Exception as exc:
        log.warning("[%s] Fallback HTTP failed: %s", ticker, exc)

    # Step 5 — empty scaffold
    slug = screener_company_slug(ticker)
    return {
        "ticker": ticker,
        "updated": TODAY,
        "source": "unavailable",
        "company_url": f"https://www.screener.in/company/{slug}/consolidated/",
        "key_ratios": {"columns": [], "rows": [], "top_ratios": []},
        "quarterly_results": {"columns": [], "rows": []},
        "shareholding_pattern": {"columns": [], "rows": []},
        "balance_sheet": {},
        "cash_flows": {},
        "peers": [],
        "error": "all sources failed",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SATELLITES.JSON — persistent store
# ═══════════════════════════════════════════════════════════════════════════════


def normalize_satellite_record(ticker: str, raw_satellite: Any) -> dict[str, Any]:
    raw = raw_satellite if isinstance(raw_satellite, dict) else {}
    normalized = dict(raw)
    status = str(normalized.get("status", "watchlist")).strip().lower()
    if status not in {"active", "watchlist", "exited"}:
        status = "watchlist"
    category = str(normalized.get("category", "satellite")).strip().lower()
    if category not in {"core", "satellite"}:
        category = "satellite"

    normalized["ticker"] = ticker
    normalized["status"] = status
    normalized["category"] = category
    normalized["paper"] = bool(normalized.get("paper", False))
    normalized["entry_date"] = normalized.get("entry_date") or TODAY
    normalized["avg_price"] = _safe_float(normalized.get("avg_price"), None)
    normalized["total_qty"] = _safe_int(normalized.get("total_qty"), 0) or 0
    normalized["allocation_pct"] = _safe_int(normalized.get("allocation_pct"), 0) or 0
    normalized["total_invested"] = _safe_float(normalized.get("total_invested"), 0.0) or 0.0
    normalized["original_thesis"] = str(normalized.get("original_thesis", "") or "")
    normalized["thesis_assumptions"] = _ensure_text_list(normalized.get("thesis_assumptions"))
    normalized["thesis_history"] = _ensure_dict_list(normalized.get("thesis_history"))
    normalized["audit_log"] = _ensure_dict_list(normalized.get("audit_log"))
    normalized["transactions"] = _ensure_dict_list(normalized.get("transactions"))
    normalized["fundamentals"] = normalized.get("fundamentals") if isinstance(normalized.get("fundamentals"), dict) else {}
    normalized["screener_cache_path"] = str(normalized.get("screener_cache_path") or "")
    normalized["annual_report_path"] = str(normalized.get("annual_report_path") or "")
    normalized["exit_date"] = normalized.get("exit_date")
    normalized["exit_reason"] = normalized.get("exit_reason")
    return normalized

def _ticker_folder(status: str, category: str) -> Path:
    s = status.lower().strip()
    c = category.lower().strip()
    if s == "exited":
        return PORTFOLIO_EXITED_DIR
    if s == "active":
        return PORTFOLIO_CURRENT_CORES_DIR if c == "core" else PORTFOLIO_CURRENT_SATS_DIR
    return PORTFOLIO_WATCHLIST_CORES_DIR if c == "core" else PORTFOLIO_WATCHLIST_SATS_DIR


def _ticker_report_folder(status: str, category: str) -> Path:
    s = status.lower().strip()
    c = category.lower().strip()
    if s == "exited":
        return REPORTS_EXITED_DIR
    if s == "active":
        return REPORTS_CURRENT_CORES_DIR if c == "core" else REPORTS_CURRENT_SATS_DIR
    return REPORTS_WATCHLIST_CORES_DIR if c == "core" else REPORTS_WATCHLIST_SATS_DIR


def move_ticker_reports(ticker: str, from_status: str, to_status: str, category: str) -> None:
    """Move all per-ticker report files from source folder to destination folder."""
    src = _ticker_report_folder(from_status, category)
    dst = _ticker_report_folder(to_status, category)
    dst.mkdir(parents=True, exist_ok=True)
    for f in sorted(src.glob(f"{ticker}_*.md")):
        dst_path = dst / f.name
        shutil.move(str(f), str(dst_path))
        print(f"  Moved report: {f.name}  →  {dst_path}")


def _all_portfolio_dirs() -> list[Path]:
    return [
        PORTFOLIO_CURRENT_CORES_DIR,
        PORTFOLIO_CURRENT_SATS_DIR,
        PORTFOLIO_WATCHLIST_CORES_DIR,
        PORTFOLIO_WATCHLIST_SATS_DIR,
        PORTFOLIO_EXITED_DIR,
    ]


def _portfolio_dirs_empty() -> bool:
    for folder in _all_portfolio_dirs():
        if folder.exists() and any(folder.glob("*.json")):
            return False
    return True


def save_single_ticker(record: dict[str, Any]) -> None:
    """Atomically write a ticker record to the correct portfolio_data/ subfolder.
    Removes stale copies from other subfolders so the file lives in exactly one place."""
    ticker = str(record.get("ticker", "")).upper()
    status = str(record.get("status", "watchlist")).lower()
    category = str(record.get("category", "satellite")).lower()
    target_folder = _ticker_folder(status, category)
    target_folder.mkdir(parents=True, exist_ok=True)
    target_path = target_folder / f"{ticker}.json"
    for folder in _all_portfolio_dirs():
        if folder == target_folder:
            continue
        stale = folder / f"{ticker}.json"
        if stale.exists():
            try:
                stale.unlink()
            except Exception as exc:
                log.warning("Could not remove stale ticker file %s: %s", stale, exc)
    atomic_write_json(target_path, record)


def load_all_tickers(folders: list[Path] | None = None) -> dict[str, Any]:
    """Load all per-ticker JSONs from specified folders (default: all portfolio_data/ folders)."""
    if folders is None:
        folders = _all_portfolio_dirs()
    result: dict[str, Any] = {}
    for folder in folders:
        if not folder.exists():
            continue
        for path in sorted(folder.glob("*.json")):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                log.warning("Failed to read ticker file %s: %s", path, exc)
                continue
            if not isinstance(raw, dict):
                continue
            ticker = str(raw.get("ticker", path.stem)).upper()
            result[ticker] = normalize_satellite_record(ticker, raw)
    return result


def migrate_legacy_data() -> None:
    """One-time migration from watchlist.json / satellites.json to per-ticker files."""
    migrated: list[str] = []

    if SATELLITES_FILE.exists():
        payload = _load_json_file(SATELLITES_FILE, {})
        if isinstance(payload, dict):
            for ticker, raw in payload.items():
                ticker = str(ticker).upper()
                record = normalize_satellite_record(ticker, raw)
                record.setdefault("category", "satellite")
                if not record.get("screener_cache_path"):
                    record["screener_cache_path"] = str(SCREENER_DIR / f"{ticker}.json")
                if not record.get("annual_report_path"):
                    report_root = _find_matching_subdir(ANNUAL_REPORTS_SUMMARY_DIR, ticker)
                    record["annual_report_path"] = str(report_root) if report_root else ""
                save_single_ticker(record)
                migrated.append(f"{ticker} (satellites.json → {record['status']}/{record['category']})")

    if WATCHLIST_FILE.exists():
        wl = _load_json_file(WATCHLIST_FILE, [])
        if isinstance(wl, list):
            existing = load_all_tickers()
            for item in wl:
                ticker = str(item).upper().strip()
                if not ticker or ticker in existing:
                    continue
                record = normalize_satellite_record(ticker, {})
                record["ticker"] = ticker
                record["status"] = "watchlist"
                record["category"] = "satellite"
                record["avg_price"] = None
                record["screener_cache_path"] = str(SCREENER_DIR / f"{ticker}.json")
                report_root = _find_matching_subdir(ANNUAL_REPORTS_SUMMARY_DIR, ticker)
                record["annual_report_path"] = str(report_root) if report_root else ""
                save_single_ticker(record)
                migrated.append(f"{ticker} (watchlist.json → watchlist/satellite)")

    if migrated:
        print("migrate_legacy_data: migrated the following tickers:")
        for entry in migrated:
            print(f"  {entry}")
        print("Old files kept as backup: satellites.json, watchlist.json")
    else:
        print("migrate_legacy_data: no legacy data to migrate.")


def load_satellites() -> dict[str, Any]:
    """Load all per-ticker records from portfolio_data/ subfolders."""
    return load_all_tickers()


def save_satellites(sats: dict[str, Any]) -> None:
    """Write each ticker to its per-ticker file in portfolio_data/."""
    for ticker, record in sats.items():
        if not isinstance(record, dict):
            continue
        rec = dict(record)
        rec["ticker"] = str(ticker).upper()
        save_single_ticker(rec)


def cmd_add(spec: str) -> None:
    """--add TICKER:AVG_PRICE:ALLOC_PCT"""
    parts = spec.strip().split(":")
    if len(parts) != 3:
        log.error("--add format: TICKER:AVG_PRICE:ALLOC_PCT  e.g. DIXON.NS:840:30")
        sys.exit(1)
    ticker, price, alloc = parts[0].upper(), float(parts[1]), int(parts[2])
    all_tickers = load_all_tickers()
    if ticker in all_tickers and all_tickers[ticker].get("status") == "active":
        log.error(f"{ticker} is already an active holding.")
        sys.exit(1)
    try:
        cat_raw = input("  Core or satellite? [c/s, default s]: ").strip().lower()
    except EOFError:
        cat_raw = "s"
    category = "core" if cat_raw == "c" else "satellite"
    try:
        loc_raw = input("  Current holding or watchlist? [c/w, default c]: ").strip().lower()
    except EOFError:
        loc_raw = "c"
    status = "active" if loc_raw != "w" else "watchlist"
    report_root = _find_matching_subdir(ANNUAL_REPORTS_SUMMARY_DIR, ticker)
    record = normalize_satellite_record(ticker, {
        "status": status,
        "category": category,
        "paper": False,
        "entry_date": TODAY,
        "avg_price": price,
        "allocation_pct": alloc,
        "total_qty": 0,
        "total_invested": 0,
        "screener_cache_path": str(SCREENER_DIR / f"{ticker}.json"),
        "annual_report_path": str(report_root) if report_root else "",
    })
    save_single_ticker(record)
    folder_label = _ticker_source_label(record)
    log.info(f"[SATELLITE] Added {ticker} @ ₹{price} | {alloc}% allocation → {folder_label}")


def cmd_paper(spec: str) -> None:
    """--paper TICKER:AVG_PRICE:ALLOC_PCT"""
    parts = spec.strip().split(":")
    if len(parts) != 3:
        log.error("--paper format: TICKER:AVG_PRICE:ALLOC_PCT  e.g. DIXON.NS:840:30")
        sys.exit(1)
    ticker, price, alloc = parts[0].upper(), float(parts[1]), int(parts[2])
    sats = load_satellites()
    if ticker in sats and sats[ticker]["status"] == "active":
        log.error(f"{ticker} already an active satellite.")
        sys.exit(1)
    sats[ticker] = normalize_satellite_record(ticker, {
        "status": "active",
        "category": "satellite",
        "paper": True,
        "entry_date": TODAY,
        "avg_price": price,
        "allocation_pct": alloc,
        "total_qty": 0,
        "total_invested": 0,
        "screener_cache_path": str(SCREENER_DIR / f"{ticker}.json"),
        "annual_report_path": "",
    })
    save_satellites(sats)
    log.info(f"[SATELLITE] Added PAPER {ticker} @ ₹{price} | {alloc}% allocation")


def cmd_exit(ticker: str) -> None:
    all_tickers = load_all_tickers()
    ticker = ticker.upper()
    if ticker not in all_tickers:
        log.error(f"{ticker} not found in portfolio_data/")
        sys.exit(1)
    rec = dict(all_tickers[ticker])
    if rec.get("status") != "active":
        log.warning(f"[{ticker}] Skipping exit — ticker is not an active holding (status={rec.get('status')}). Only active tickers can be exited.")
        return
    try:
        current = float(yf.Ticker(ticker).fast_info["last_price"])
        avg = _safe_float(rec.get("avg_price"), None)
        if avg:
            gain = (current - avg) / avg * 100
            log.info(f"[SATELLITE] Exiting {ticker} @ ₹{current:.2f} ({gain:+.1f}% from avg)")
        else:
            log.info(f"[SATELLITE] Exiting {ticker} @ ₹{current:.2f}")
    except Exception:
        pass
    try:
        exit_reason = input("  Exit reason: ").strip()
    except EOFError:
        exit_reason = ""
    _old_category = rec.get("category", "satellite")
    rec["status"] = "exited"
    rec["exit_date"] = TODAY
    rec["exit_reason"] = exit_reason
    save_single_ticker(rec)
    move_ticker_reports(ticker, "active", "exited", _old_category)
    log.info(f"[SATELLITE] {ticker} marked exited → portfolio_data/exited/")


async def cmd_screener(ticker: str, *, show_live: bool = False) -> None:
    """Fetch screener.in data via browser-use MCP and cache it locally."""
    ticker = ticker.upper()
    path = SCREENER_DIR / f"{ticker}.json"
    data = await fetch_screener_snapshot(ticker, show_live=show_live)
    atomic_write_json(path, data)
    log.info(f"Saved screener snapshot → {path}")


async def load_screener(ticker: str, *, show_live: bool = False) -> dict[str, Any]:
    path = SCREENER_DIR / f"{ticker}.json"
    if path.exists():
        cached = _load_json_file(path, {})
        if isinstance(cached, dict) and cached:
            log.info(f"[{ticker}] Loaded screener fundamentals (updated {cached.get('updated', '?')})")
            staleness_warning = _cache_staleness_warning(path, ticker)
            if staleness_warning:
                log.warning(staleness_warning)
                _append_data_warning(cached, staleness_warning)
            return cached

    data = await fetch_screener_snapshot(ticker, show_live=show_live)
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET DATA  (reused from market_pipeline.py, trimmed for 1yr view)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_ohlcv(ticker: str) -> dict:
    log.info(f"[{ticker}] Fetching {OHLCV_DAYS}-day OHLCV …")
    end   = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=OHLCV_DAYS + 60)

    downloaded = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
    if downloaded is None:
        raise ValueError(f"No data for {ticker}")
    if downloaded.empty:
        raise ValueError(f"No data for {ticker}")

    df = downloaded.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.tail(OHLCV_DAYS).copy()
    raw_close = cast(pd.Series, df["Close"].squeeze())
    adj_close = cast(pd.Series, df["Adj Close"].squeeze()) if "Adj Close" in df.columns else raw_close
    close = adj_close
    volume = cast(pd.Series, df["Volume"].squeeze())
    data_warnings: list[str] = []

    recent_window = min(30, len(df))
    if recent_window > 0:
        raw_recent = raw_close.tail(recent_window).replace(0, pd.NA).astype(float)
        adj_recent = adj_close.tail(recent_window).astype(float)
        ratio = (adj_recent - raw_recent).abs() / raw_recent.abs()
        ratio = ratio.dropna()
        if not ratio.empty:
            max_diff = float(ratio.max())
            if max_diff > 0.05:
                warning_text = (
                    f"[{ticker}] Corporate action guard: adjusted vs raw close diverged by "
                    f"{max_diff * 100:.2f}% within last 30 days. Split/bonus/dividend may have occurred; "
                    f"review satellites.json avg_price manually."
                )
                log.warning(warning_text)
                data_warnings.append(warning_text)

    # Indicators
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_g = gain.ewm(com=13, adjust=False).mean()
    avg_l = loss.ewm(com=13, adjust=False).mean()
    rs    = avg_g / avg_l.replace(0, float("nan"))
    rsi   = (100 - 100 / (1 + rs)).round(2)

    ma50  = close.rolling(50).mean().round(4)
    ma200 = close.rolling(200).mean().round(4)
    vol_ma20 = volume.rolling(20).mean()

    def _scalar(series, date):
        try:
            val = series.loc[date]
            if hasattr(val, "__len__"): val = val.iloc[0]
            return None if val != val else round(float(val), 4)
        except Exception:
            return None

    latest_date = df.index[-1]
    c      = float(close.iloc[-1])
    ma50v  = _scalar(ma50, latest_date)
    ma200v = _scalar(ma200, latest_date)
    rsiv   = _scalar(rsi, latest_date)
    volv   = float(volume.iloc[-1])
    vol20v = _scalar(vol_ma20, latest_date)

    # 52-week stats
    all_closes = close.tolist()
    w52_high = round(max(all_closes), 2)
    w52_low  = round(min(all_closes), 2)
    start_price = float(close.iloc[0]) if len(close) else c
    one_year_return = round((c / start_price - 1) * 100, 2) if start_price else None
    three_month_start = float(close.iloc[-63]) if len(close) >= 63 else start_price
    three_month_return = round((c / three_month_start - 1) * 100, 2) if three_month_start else None
    max_drawdown = None
    if len(close):
        running_peak = close.cummax()
        drawdowns = (close / running_peak - 1) * 100
        max_drawdown = round(float(drawdowns.min()), 2)

    # Monthly returns for trend context (last 3 months)
    monthly = close.resample("ME").last().pct_change().tail(6).round(4).tolist()

    return {
        "ticker":           ticker,
        "latest_close":     round(c, 2),
        "one_year_return_pct": one_year_return,
        "three_month_return_pct": three_month_return,
        "max_drawdown_pct": max_drawdown,
        "rsi14":            rsiv,
        "ma50":             ma50v,
        "ma200":            ma200v,
        "price_vs_ma50":    "above" if ma50v and c > ma50v else "below",
        "price_vs_ma200":   "above" if ma200v and c > ma200v else "below",
        "week52_high":      w52_high,
        "week52_low":       w52_low,
        "pct_from_52w_high": round((c - w52_high) / w52_high * 100, 2),
        "pct_from_52w_low":  round((c - w52_low) / w52_low * 100, 2),
        "volume_ratio":     round(volv / vol20v, 2) if vol20v else None,
        "monthly_returns":  monthly,
        "data_warnings": data_warnings,
    }


def fetch_news(ticker: str, searxng_url: str) -> list[dict]:
    results: list[dict] = []
    results += fetch_nse_announcements(ticker)
    results += fetch_bse_announcements(ticker)
    results += fetch_moneycontrol_news(ticker)
    results += fetch_yfinance_news(ticker)
    results += fetch_et_rss(ticker)
    results += fetch_bs_rss(ticker)
    results += fetch_searxng(ticker, searxng_url)

    unique: list[dict] = []
    token_sets: list[set[str]] = []
    for item in results:
        title = str(item.get("title", "")).strip()
        if not title:
            continue

        normalized = re.sub(r"[^\w\s]+", " ", title.lower())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        tokens = {token for token in normalized.split(" ") if token}
        if not tokens:
            continue

        is_duplicate = False
        for seen_tokens in token_sets:
            min_tokens = min(len(tokens), len(seen_tokens))
            if min_tokens == 0:
                continue
            overlap_ratio = len(tokens & seen_tokens) / float(min_tokens)
            if overlap_ratio > 0.60:
                is_duplicate = True
                break

        if is_duplicate:
            continue

        unique.append(item)
        token_sets.append(tokens)

    return unique[:20] or [{"title": "No news", "snippet": ""}]


# ═══════════════════════════════════════════════════════════════════════════════
# OLLAMA  (streaming, with fallback — same pattern as market_pipeline.py)
# ═══════════════════════════════════════════════════════════════════════════════

def release_ollama_model(model: str) -> None:
    try:
        subprocess.run(
            ["ollama", "stop", model],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except Exception:
        pass


def screener_company_slug(ticker: str) -> str:
    return str(ticker).upper().split(".")[0].strip()


def verify_ollama_models_available() -> None:
    tags_url = f"{OLLAMA_BASE}/api/tags"
    try:
        response = requests.get(tags_url, timeout=10)
        response.raise_for_status()
    except Exception as exc:
        log.error("Ollama health check failed at %s: %s", tags_url, exc)
        raise SystemExit(1)

    try:
        payload = response.json()
    except Exception as exc:
        log.error("Ollama health check returned invalid JSON: %s", exc)
        raise SystemExit(1)

    available_models: set[str] = set()
    for item in payload.get("models", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        for key in ("name", "model"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                available_models.add(value.strip())

    missing_models = [model for model in STAGE_MODELS if model not in available_models]
    if missing_models:
        for model in missing_models:
            log.error("Ollama startup check failed: required model missing: %s", model)
        raise SystemExit(1)

    log.info("Ollama health check passed. Models ready: %s", ", ".join(STAGE_MODELS))


def log_prompt_context_length(ticker: str, stage_no: int, prompt: str) -> None:
    prompt_length = len(prompt)
    log.info("[%s] Stage %d prompt length: %d chars", ticker, stage_no, prompt_length)
    if prompt_length > 12000:
        log.warning(
            "[%s] Stage %d prompt length is %d chars; context may be truncated depending on model context window.",
            ticker,
            stage_no,
            prompt_length,
        )


def ollama_generate(
    model: str,
    prompt: str,
    num_predict: int = 1200,
    format_schema: dict[str, Any] | str | None = None,
    num_ctx: int = 8192,
    temperature: float = 0.0,
) -> str:
    options = {
        "temperature": temperature,
        "num_ctx": num_ctx,
        "num_predict": num_predict,
        "keep_alive": OLLAMA_KEEP_ALIVE,
    }

    try:
        # Try /api/chat streaming first.
        resp = requests.post(
            OLLAMA_CHAT_URL,
            json={
                "model": model,
                "stream": True,
                "think": False,
                **({"format": format_schema} if format_schema is not None else {}),
                "options": options,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=(30, 600),
            stream=True,
        )
        resp.raise_for_status()
        chunks = []
        for line in resp.iter_lines():
            if not line:
                continue
            data = json.loads(line)
            chunks.append(data.get("message", {}).get("content", ""))
            if data.get("done"):
                break
        result = "".join(chunks).strip()
        if result:
            return result
    except Exception as e:
        log.warning(f"  /api/chat failed: {e}")

    try:
        # Fallback /api/generate.
        resp = requests.post(
            OLLAMA_GEN_URL,
            json={
                "model": model,
                "prompt": prompt,
                "stream": True,
                "think": False,
                **({"format": format_schema} if format_schema is not None else {}),
                "options": options,
            },
            timeout=(30, 600),
            stream=True,
        )
        resp.raise_for_status()
        chunks = []
        for line in resp.iter_lines():
            if not line:
                continue
            data = json.loads(line)
            chunks.append(data.get("response", ""))
            if data.get("done"):
                break
        result = "".join(chunks).strip()
        if result:
            return result
    except Exception as e:
        log.warning(f"  /api/generate failed: {e}")
    finally:
        release_ollama_model(model)

    raise RuntimeError(f"Ollama unreachable for model {model}")


def extract_json(raw: str) -> dict:
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL)
    raw = re.sub(r"```json|```", "", raw)
    start = raw.find("{")
    end   = raw.rfind("}") + 1
    if start == -1 or end == 0:
        score = re.search(r'"score"\s*:\s*(\d+)', raw)
        return {"score": int(score.group(1))} if score else {}
    candidate = _normalize_json_candidate(raw[start:end].strip())
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        try:
            return json.loads(_normalize_json_candidate(_escape_json_string_controls(candidate)))
        except json.JSONDecodeError:
            pass
        score = re.search(r'"score"\s*:\s*(\d+)', raw)
        return {"score": int(score.group(1))} if score else {}


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1 — QUALITY SCREENER  (Qwen 3 30B)
# Decides if a stock deserves a satellite slot. Long-term view only.
# ═══════════════════════════════════════════════════════════════════════════════

SCREENER_PROMPT = """You are a long-term equity analyst focused on Indian mid/small caps.
Evaluate {ticker} for a satellite position in a personal portfolio.
Hold period is 1-5 years (flexible). This is a weekly satellite auditor, not a swing trade screen.

PRICE DATA:
{price_data}

FUNDAMENTALS (from Screener.in — may be empty if not provided):
{fundamentals}

ANNUAL REPORT CONTEXT:
{annual_reports}

GOVERNMENT BUDGET CONTEXT:
{budget_context}

RECENT NEWS:
{news}

Compare the fundamentals against the 1-year price trend. Explain whether the market is confirming, ignoring, or contradicting the fundamentals.
Also include a brief prompt/data assessment: rate the input quality from 1-10 and add a one-sentence prompt_assessment describing whether the provided data is sufficient or limited.
Do not provide hidden chain-of-thought; keep reasoning concise and use the rationale field for a brief explanation.
Respond ONLY with valid JSON, score field FIRST:
{{
  "score": <1-10, where 8+ = strong satellite candidate>,
  "quality_verdict": "QUALIFY" | "WATCHLIST" | "REJECT",
  "business_quality": "brief assessment of moat and competitive position",
  "growth_runway": "TAM, growth drivers for next 3-5 years",
  "valuation": "cheap/fair/expensive relative to growth — one line",
  "key_risks": ["risk1", "risk2", "risk3"],
  "thesis_assumptions": [
    "assumption 1 that must hold for thesis to work",
    "assumption 2",
    "assumption 3"
  ],
  "trend_read": "2-3 sentences on how the 1-year trend lines up with the fundamentals",
    "rationale": "2-3 sentences on why this score",
    "input_quality_score": <1-10>,
    "confidence_score": <1-10>,
    "prompt_assessment": "one sentence on prompt/data quality"
}}"""


def run_screener(ticker: str, ohlcv: dict, news: list, fundamentals: dict, *,
                 annual_report_path: Path | None = None, category: str = "satellite") -> dict:
    model = _stage_model(category, 1)
    template = _stage_prompt_template(category, 1) or SCREENER_PROMPT
    log.info(f"[{ticker}] Stage 1 — Quality Screener ({model}) …")
    news_text = "\n".join(f"• {n['title']}: {n['snippet']}" for n in news[:8])
    fund_text = _compact_json(fundamentals, limit=1800) if fundamentals else "No screener data provided."
    price_text = _compact_json({k: v for k, v in ohlcv.items() if k != "ohlcv"}, limit=1600)
    annual_report_text = load_annual_report_context(ticker, limit=1800, report_root=annual_report_path)
    budget_text = load_budget_context(years=2, limit=1200)

    prompt = template.format(
        ticker=ticker,
        price_data=price_text,
        fundamentals=fund_text,
        annual_reports=annual_report_text,
        budget_context=budget_text,
        news=news_text,
    )
    log_prompt_context_length(ticker, 1, prompt)
    raw = ollama_generate(model, prompt, num_predict=1000, format_schema=SCREENER_OUTPUT_SCHEMA)
    result = extract_json(raw)
    result["raw"] = raw
    result["prompt"] = prompt
    result["model"] = model
    result["inputs"] = {
        "ticker": ticker,
        "price_data": price_text,
        "fundamentals": fund_text,
        "annual_reports": annual_report_text,
        "news": news_text,
    }
    result["fundamentals_snapshot"] = fundamentals
    log.info(f"[{ticker}] Screener score: {result.get('score', '?')} | {result.get('quality_verdict', '?')}")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2 — THESIS WRITER  (GPT-OSS 20B)
# Writes or updates the investment thesis. Knows prior thesis if held.
# ═══════════════════════════════════════════════════════════════════════════════

THESIS_PROMPT = """You are an investment analyst writing a long-term thesis for a satellite position.

Stock: {ticker}
Current price: ₹{price}
Screener verdict: {screener_verdict}
1-year price trend: {one_year_trend}%
3-month price trend: {three_month_trend}%

PRIOR THESIS (empty if new position):
{prior_thesis}

PRIOR ASSUMPTIONS:
{prior_assumptions}

PREVIOUS AUDIT (empty if new position):
{prior_audit}

ANNUAL REPORT CONTEXT:
{annual_reports}

GOVERNMENT BUDGET CONTEXT:
{budget_context}

RECENT NEWS:
{news}

FUNDAMENTALS:
{fundamentals}

Write exactly 3 paragraphs in the thesis. The first paragraph should explain the business and moat, the second should connect fundamentals to the price trend, and the third should define exit triggers and what changed versus the previous audit. Respond ONLY with valid JSON:
Also include a brief prompt/data assessment: rate the input quality from 1-10 and add a one-sentence prompt_assessment describing whether the provided data is sufficient or limited.
Do not provide hidden chain-of-thought; keep reasoning concise and use the narrative fields for brief explanation.
{{
  "thesis": "3 paragraphs separated by blank lines",
  "assumptions": [
    "assumption 1 — specific and checkable (e.g. 'Revenue growth > 20% YoY for next 2 years')",
    "assumption 2",
    "assumption 3",
    "assumption 4"
  ],
  "bull_case": "what needs to happen for 2-3x return",
  "bear_case": "what kills the thesis",
  "time_horizon": "estimated hold period e.g. '18-36 months'",
  "exit_triggers": ["trigger1 — e.g. assumption 2 breaks", "trigger2", "trigger3"],
  "changed_from_prior": "what changed since last audit (empty if new position)",
    "narrative_change": "how the story strengthened, weakened, or shifted versus the previous audit",
    "input_quality_score": <1-10>,
    "confidence_score": <1-10>,
    "prompt_assessment": "one sentence on prompt/data quality"
}}"""


def run_thesis(ticker: str, ohlcv: dict, screener: dict,
               news: list, fundamentals: dict, prior: dict, *,
               annual_report_path: Path | None = None, category: str = "satellite") -> dict:
    model = _stage_model(category, 2)
    template = _stage_prompt_template(category, 2) or THESIS_PROMPT
    log.info(f"[{ticker}] Stage 2 — Thesis Writer ({model}) …")
    news_text = "\n".join(f"• {n['title']}: {n['snippet']}" for n in news[:6])
    fund_text = _compact_json(fundamentals, limit=2200) if fundamentals else "None."
    annual_report_text = load_annual_report_context(ticker, limit=2200, report_root=annual_report_path)
    prior_history = prior.get("thesis_history", []) if isinstance(prior, dict) else []
    if prior_history:
        prior_thesis = prior_history[-1].get("thesis", "") or prior.get("original_thesis", "")
    else:
        prior_thesis = prior.get("original_thesis", "") if isinstance(prior, dict) else ""
    prior_audit = prior.get("audit_log", [])[-1] if isinstance(prior, dict) and prior.get("audit_log") else {}

    prompt = template.format(
        ticker=ticker,
        price=ohlcv["latest_close"],
        one_year_trend=ohlcv.get("one_year_return_pct", "?") or "?",
        three_month_trend=ohlcv.get("three_month_return_pct", "?") or "?",
        screener_verdict=screener.get("quality_verdict", "N/A"),
        prior_thesis=prior_thesis or "None — new position.",
        prior_assumptions=json.dumps(prior.get("thesis_assumptions", []), indent=2),
        prior_audit=_compact_json(prior_audit, limit=1000) if prior_audit else "None — new position.",
        annual_reports=annual_report_text,
        news=news_text,
        fundamentals=fund_text,
    )
    log_prompt_context_length(ticker, 2, prompt)
    raw = ollama_generate(model, prompt, num_predict=1500, format_schema=THESIS_OUTPUT_SCHEMA)
    result = extract_json(raw)
    result["raw"] = raw
    result["prompt"] = prompt
    result["model"] = model
    result["inputs"] = {
        "ticker": ticker,
        "price": ohlcv["latest_close"],
        "one_year_trend": ohlcv.get("one_year_return_pct", "?") or "?",
        "three_month_trend": ohlcv.get("three_month_return_pct", "?") or "?",
        "screener_verdict": screener.get("quality_verdict", "N/A"),
        "prior_thesis": prior_thesis or "None — new position.",
        "prior_assumptions": prior.get("thesis_assumptions", []),
        "prior_audit": prior_audit,
        "annual_reports": annual_report_text,
        "news": news_text,
        "fundamentals": fund_text,
    }
    result["fundamentals_snapshot"] = fundamentals
    log.info(f"[{ticker}] Thesis written. Assumptions: {len(result.get('assumptions', []))}")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 3 — THESIS AUDITOR  (Gemma 4 26B)
# Argues against the thesis. Checks each assumption. Recommends ADD/HOLD/TRIM/EXIT.
# ═══════════════════════════════════════════════════════════════════════════════

AUDITOR_PROMPT = """You are a skeptical portfolio risk manager auditing a satellite position.
Your job is to find flaws, check assumptions, and force an honest decision.

Stock: {ticker}
Current price: ₹{price}
Entry price: ₹{entry_price}
Return so far: {return_pct}%
Held since: {entry_date}
1-year price trend: {one_year_trend}%
3-month price trend: {three_month_trend}%
Screener verdict: {screener_verdict}
Screener score: {screener_score}

INVESTMENT THESIS:
{thesis}

ASSUMPTIONS TO CHECK (mark each true/false with evidence):
{assumptions}

SCREENER FUNDAMENTALS:
{fundamentals}

PRIOR AUDIT DECISIONS:
{prior_audits}

ANNUAL REPORT CONTEXT:
{annual_reports}

GOVERNMENT BUDGET CONTEXT:
{budget_context}

RECENT NEWS:
{news}

MACRO CONTEXT: Indian equity market, current week {week}.

Be harsh. Explicitly search for thesis invalidation triggers such as declining margins, FII selling, promoter pledging, deteriorating cash flow, leverage spikes, or shareholding deterioration. Respond ONLY with valid JSON, score field FIRST:
Also include a brief prompt/data assessment: rate the input quality from 1-10 and add a one-sentence prompt_assessment describing whether the provided data is sufficient or limited.
Do not provide hidden chain-of-thought; keep reasoning concise and use the decision_rationale/devils_advocate fields for brief explanation.
{{
  "score": <1-10 conviction in thesis STILL holding>,
  "assumptions_status": {{
    "assumption text": true/false
  }},
  "broken_assumptions": ["list any assumptions now false"],
  "new_risks": ["risks not in original thesis"],
  "invalidation_triggers": ["triggers that would invalidate the thesis"],
  "thesis_intact": true/false,
  "decision": "ADD" | "HOLD" | "TRIM" | "EXIT",
  "decision_rationale": "2-3 sentences justifying the decision",
  "add_recommended": true/false,
  "add_rationale": "why add more now — or why not",
  "red_flags": ["key red flags seen in the audit"],
    "devils_advocate": "the strongest bear case right now in 2 sentences",
    "input_quality_score": <1-10>,
    "confidence_score": <1-10>,
    "prompt_assessment": "one sentence on prompt/data quality"
}}"""


def run_auditor(ticker: str, ohlcv: dict, sat: dict,
                thesis: dict, news: list, fundamentals: dict, screener: dict, *,
                annual_report_path: Path | None = None, category: str = "satellite") -> dict:
    model = _stage_model(category, 3)
    template = _stage_prompt_template(category, 3) or AUDITOR_PROMPT
    log.info(f"[{ticker}] Stage 3 — Thesis Auditor ({model}) …")
    price = ohlcv["latest_close"]
    entry = sat.get("avg_price", price)
    ret_pct = round((price - entry) / entry * 100, 2) if entry else 0
    assumptions = thesis.get("assumptions", sat.get("thesis_assumptions", []))

    shareholding_summary = summarize_table(
        fundamentals.get("shareholding_pattern") if isinstance(fundamentals, dict) else None
    )
    quarterly_summary = summarize_table(
        fundamentals.get("quarterly_results") if isinstance(fundamentals, dict) else None
    )
    key_ratio_summary = summarize_table(
        fundamentals.get("key_ratios") if isinstance(fundamentals, dict) else None
    )
    annual_report_text = load_annual_report_context(ticker, limit=1800, report_root=annual_report_path)
    budget_text = load_budget_context(years=2, limit=1200)

    prior_audits = sat.get("audit_log", [])[-3:]
    prior_text = json.dumps(
        [
            {"week": a.get("week"), "decision": a.get("decision"), "verdict": a.get("auditor_verdict", "")[:200]}
            for a in prior_audits
        ],
        indent=2,
    ) if prior_audits else "No prior audits — first run."

    news_text = "\n".join(f"• {n['title']}: {n['snippet']}" for n in news[:6])

    prompt = template.format(
        ticker=ticker,
        price=price,
        entry_price=entry,
        return_pct=ret_pct,
        entry_date=sat.get("entry_date", TODAY),
        one_year_trend=ohlcv.get("one_year_return_pct", "?") or "?",
        three_month_trend=ohlcv.get("three_month_return_pct", "?") or "?",
        screener_verdict=screener.get("quality_verdict", "N/A"),
        screener_score=screener.get("score", "N/A"),
        thesis=thesis.get("thesis", "No thesis written."),
        assumptions=json.dumps(assumptions, indent=2),
        fundamentals=_compact_json(
            {
                "screener_source": screener.get("source") if isinstance(screener, dict) else None,
                "key_ratios": key_ratio_summary,
                "shareholding_pattern": shareholding_summary,
                "quarterly_results": quarterly_summary,
            },
            limit=2200,
        ),
        prior_audits=prior_text,
        annual_reports=annual_report_text,
        budget_context=budget_text,
        news=news_text,
        week=f"{TODAY} (Week {WEEK_NO})",
    )
    log_prompt_context_length(ticker, 3, prompt)
    raw = ollama_generate(model, prompt, num_predict=1400, format_schema=AUDITOR_OUTPUT_SCHEMA)
    result = extract_json(raw)
    result["raw"] = raw
    result["prompt"] = prompt
    result["model"] = model
    result["inputs"] = {
        "ticker": ticker,
        "price": price,
        "entry_price": entry,
        "return_pct": ret_pct,
        "entry_date": sat.get("entry_date", TODAY),
        "one_year_trend": ohlcv.get("one_year_return_pct", "?") or "?",
        "three_month_trend": ohlcv.get("three_month_return_pct", "?") or "?",
        "screener_verdict": screener.get("quality_verdict", "N/A"),
        "screener_score": screener.get("score", "N/A"),
        "thesis": thesis.get("thesis", "No thesis written."),
        "assumptions": assumptions,
        "fundamentals": {
            "screener_source": screener.get("source") if isinstance(screener, dict) else None,
            "key_ratios": key_ratio_summary,
            "shareholding_pattern": shareholding_summary,
            "quarterly_results": quarterly_summary,
        },
        "prior_audits": prior_audits,
        "annual_reports": annual_report_text,
        "news": news_text,
        "week": f"{TODAY} (Week {WEEK_NO})",
    }
    result["fundamentals_snapshot"] = fundamentals
    log.info(
        f"[{ticker}] Auditor: conviction={result.get('score', '?')}/10 | "
        f"thesis_intact={result.get('thesis_intact', '?')} | "
        f"decision={result.get('decision', '?')}"
    )
    return result


STAGE4_MEMO_PROMPT = """You are the portfolio synthesis writer for a weekly satellite investing process.

Write exactly 3-4 concise paragraphs in plain markdown text (no bullets, no JSON).
The memo must cover:
1) what changed this week across the book,
2) where conviction is building or fading,
3) one key cross-portfolio risk to watch.

WEEK: {week}

PER-TICKER STAGE 2 + STAGE 3 SNAPSHOT:
{weekly_snapshot}

SIP ALLOCATIONS:
{allocations}
"""


def run_weekly_portfolio_memo(results: list[dict[str, Any]], allocations: dict[str, Any], *, category: str = "satellite") -> str:
    if not results:
        return ""

    ticker_snapshot: list[dict[str, Any]] = []
    for result in results:
        ticker = str(result.get("ticker", "")).strip()
        if not ticker:
            continue
        thesis = result.get("thesis", {}) if isinstance(result.get("thesis"), dict) else {}
        auditor = result.get("auditor", {}) if isinstance(result.get("auditor"), dict) else {}
        ticker_snapshot.append(
            {
                "ticker": ticker,
                "held": bool(result.get("is_held")),
                "stage2_thesis": thesis.get("thesis", ""),
                "narrative_change": thesis.get("narrative_change", "") or thesis.get("changed_from_prior", ""),
                "stage3_conviction": auditor.get("score"),
                "stage3_decision": auditor.get("decision"),
                "stage3_rationale": auditor.get("decision_rationale", ""),
                "stage3_red_flags": auditor.get("red_flags", []),
                "stage3_new_risks": auditor.get("new_risks", []),
            }
        )

    allocation_snapshot = {
        ticker: value
        for ticker, value in allocations.items()
        if ticker != "_portfolio"
    }
    if "_portfolio" in allocations:
        allocation_snapshot["_portfolio"] = allocations.get("_portfolio", {})

    model = _stage_model(category, 4)
    template = _stage_prompt_template(category, 4) or STAGE4_MEMO_PROMPT
    prompt = template.format(
        week=f"{TODAY} (Week {WEEK_NO})",
        weekly_snapshot=_compact_json(ticker_snapshot, limit=12000),
        allocations=_compact_json(allocation_snapshot, limit=8000),
    )
    log_prompt_context_length("PORTFOLIO", 4, prompt)
    memo = ollama_generate(model, prompt, num_predict=1200)
    return memo.strip()


async def run_stage_with_retries(
    stage_label: str,
    ticker: str,
    runner: Any,
    *,
    max_retries: int = STAGE_MAX_RETRIES,
    retry_delay: int = STAGE_RETRY_DELAY_SECONDS,
) -> tuple[bool, Any, str]:
    last_error = f"{stage_label} failed - model did not respond"
    for attempt in range(max_retries + 1):
        attempt_no = attempt + 1
        try:
            result = await runner()
            if result is None:
                raise RuntimeError("returned None")
            if isinstance(result, str) and not result.strip():
                raise RuntimeError("returned empty string")
            if isinstance(result, dict) and not result:
                raise RuntimeError("returned empty payload")
            return True, result, ""
        except Exception as exc:
            last_error = str(exc) or f"{stage_label} failed - model did not respond"
            if attempt < max_retries:
                log.warning(
                    "[%s] %s attempt %d/%d failed: %s. Retrying in %ds.",
                    ticker,
                    stage_label,
                    attempt_no,
                    max_retries + 1,
                    last_error,
                    retry_delay,
                )
                await asyncio.sleep(retry_delay)
            else:
                log.error(
                    "[%s] %s failed after %d attempts: %s",
                    ticker,
                    stage_label,
                    max_retries + 1,
                    last_error,
                )
    return False, None, last_error


def build_stage_failure_payload(stage_no: int, model: str, error_text: str) -> dict[str, Any]:
    failure_message = f"Stage {stage_no} failed - model did not respond"
    return {
        "stage_failed": True,
        "stage_no": stage_no,
        "failure_message": failure_message,
        "failure_error": error_text,
        "model": model,
        "raw": "",
        "prompt": "",
        "inputs": {},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SIZING ENGINE
# Conviction-based SIP recommendation for active satellites only.
# ═══════════════════════════════════════════════════════════════════════════════

def _historical_convictions(sat: dict[str, Any], limit: int = 3) -> list[float]:
    convictions: list[float] = []
    audits = _ensure_dict_list(sat.get("audit_log", []))
    for entry in audits[-limit:]:
        value = _safe_float(entry.get("conviction"), None)
        if value is None:
            value = _safe_float(entry.get("conviction_score"), None)
        if value is None:
            value = _safe_float(entry.get("auditor_score"), None)
        if value is None:
            value = _safe_float(entry.get("screener_score"), None)
        if value is not None:
            convictions.append(value)
    return convictions


def _smoothed_conviction(current: float, historical_values: list[float]) -> tuple[float, float | None]:
    if not historical_values:
        return current, None
    historical_avg = sum(historical_values) / len(historical_values)
    smoothed = 0.60 * current + 0.40 * historical_avg
    return smoothed, historical_avg


def compute_sip_sizing(results: list[dict], satellites: dict) -> dict[str, Any]:
    allocations: dict[str, Any] = {}
    gross_recommendation = 0.0
    active_count = 0
    hold_cash_tickers: list[str] = []

    for result in results:
        ticker = result.get("ticker")
        if not ticker:
            continue

        sat = satellites.get(ticker, {})
        if sat.get("status") != "active":
            continue

        active_count += 1
        current_conviction = _safe_float(result.get("auditor", {}).get("score"), None)
        if current_conviction is None:
            current_conviction = _safe_float(result.get("screener", {}).get("score"), 5.0) or 5.0
        historical_values = _historical_convictions(sat, limit=3)
        conviction, historical_avg = _smoothed_conviction(current_conviction, historical_values)

        if conviction > 8:
            multiplier = 1.5
            action = "OVERWEIGHT"
        elif conviction < 5:
            multiplier = 0.0
            action = "HOLD_CASH"
            hold_cash_tickers.append(ticker)
        else:
            multiplier = 1.0
            action = "NORMAL"

        amount = round(SIP_WEEKLY * multiplier, 0)
        gross_recommendation += amount
        allocations[ticker] = {
            "amount": amount,
            "multiplier": multiplier,
            "action": action,
            "conviction": conviction,
            "current_conviction": current_conviction,
            "historical_avg_conviction": historical_avg,
            "historical_samples": len(historical_values),
            "weekly_sip": SIP_WEEKLY,
        }

    allocations["_portfolio"] = {
        "weekly_sip": SIP_WEEKLY,
        "gross_recommendation": gross_recommendation,
        "active_positions": active_count,
        "hold_cash_tickers": hold_cash_tickers,
    }
    return allocations


def compute_sizing(results: list[dict], satellites: dict) -> dict[str, Any]:
    return compute_sip_sizing(results, satellites)


# ═══════════════════════════════════════════════════════════════════════════════
# UPDATE SATELLITES.JSON
# ═══════════════════════════════════════════════════════════════════════════════

def update_satellite(satellites: dict, ticker: str, ohlcv: dict,
                     screener: dict, thesis: dict, auditor: dict,
                     allocation: dict, stage_failures: list[str] | None = None) -> None:
    sat = satellites.get(ticker, {})
    if auditor.get("decision") == "EXIT" and sat.get("status") != "active":
        log.warning(f"[{ticker}] Skipping exit — ticker is not an active holding (status={sat.get('status')}). Only active tickers can be exited.")
        return
    week_key = current_week_key()
    sat = normalize_satellite_record(ticker, satellites.get(ticker, {}))

    thesis_text = thesis.get("thesis", "")
    if thesis_text:
        if not sat.get("original_thesis"):
            sat["original_thesis"] = thesis_text
        sat["thesis_assumptions"] = _ensure_text_list(thesis.get("assumptions", sat.get("thesis_assumptions", [])))

        thesis_entry = {
            "week": week_key,
            "date": TODAY,
            "score": screener.get("score"),
            "thesis": thesis_text,
            "changed_from_prior": thesis.get("changed_from_prior", ""),
            "narrative_change": thesis.get("narrative_change", ""),
            "assumptions": thesis.get("assumptions", []),
            "bull_case": thesis.get("bull_case", ""),
            "bear_case": thesis.get("bear_case", ""),
            "time_horizon": thesis.get("time_horizon", ""),
            "exit_triggers": thesis.get("exit_triggers", []),
            "price": ohlcv.get("latest_close"),
            "one_year_return_pct": ohlcv.get("one_year_return_pct"),
            "three_month_return_pct": ohlcv.get("three_month_return_pct"),
        }
        sat.setdefault("thesis_history", []).append(thesis_entry)

    conviction = _safe_float(auditor.get("score"), None)
    if conviction is None:
        log.warning(
            "[%s] Stage 3 conviction parse warning: model output missing/invalid numeric score; "
            "writing conviction=null in audit_log.",
            ticker,
        )
    recommendation_amount = allocation.get("amount", 0) if allocation else 0

    audit_entry = {
        "week": week_key,
        "date": TODAY,
        "price": ohlcv.get("latest_close"),
        "one_year_return_pct": ohlcv.get("one_year_return_pct"),
        "three_month_return_pct": ohlcv.get("three_month_return_pct"),
        "screener_score": screener.get("score"),
        "screener_verdict": screener.get("quality_verdict"),
        "auditor_score": auditor.get("score"),
        "conviction": conviction,
        "smoothed_conviction": allocation.get("conviction") if allocation else None,
        "sip_multiplier": allocation.get("multiplier", 0.0) if allocation else 0.0,
        "sip_amount": recommendation_amount,
        "action": allocation.get("action", "HOLD_CASH") if allocation else "HOLD_CASH",
        "assumptions_status": auditor.get("assumptions_status", {}),
        "broken_assumptions": auditor.get("broken_assumptions", []),
        "new_risks": auditor.get("new_risks", []),
        "invalidation_triggers": auditor.get("invalidation_triggers", []),
        "red_flags": auditor.get("red_flags", []),
        "thesis_intact": auditor.get("thesis_intact", True),
        "decision": auditor.get("decision", "HOLD"),
        "decision_rationale": auditor.get("decision_rationale", ""),
        "add_recommended": auditor.get("add_recommended", False),
        "add_rationale": auditor.get("add_rationale", ""),
        "devils_advocate": auditor.get("devils_advocate", ""),
        "narrative_change": thesis.get("narrative_change", ""),
        "changed_from_prior": thesis.get("changed_from_prior", ""),
        "stage_failed": bool(stage_failures),
        "failed_stages": list(stage_failures or []),
    }

    sat.setdefault("audit_log", []).append(audit_entry)

    if recommendation_amount and recommendation_amount > 0:
        sat["total_invested"] = float(sat.get("total_invested", 0.0) or 0.0) + float(recommendation_amount)

    if audit_entry["decision"] == "EXIT":
        sat["status"] = "exited"
        sat["exit_date"] = TODAY
        sat["exit_reason"] = audit_entry["decision_rationale"]

    satellites[ticker] = sat
    save_single_ticker(sat)


# ═══════════════════════════════════════════════════════════════════════════════
# MARKDOWN REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def _previous_week_audit(snapshot: dict[str, Any], ticker: str) -> dict[str, Any]:
    sat = snapshot.get(ticker, {}) if isinstance(snapshot, dict) else {}
    audits = _ensure_dict_list(sat.get("audit_log", [])) if isinstance(sat, dict) else []
    if not audits:
        return {}
    return audits[-1]


def _audit_conviction(entry: dict[str, Any]) -> float | None:
    for key in ("smoothed_conviction", "conviction", "conviction_score", "auditor_score", "screener_score"):
        value = _safe_float(entry.get(key), None)
        if value is not None:
            return value
    return None


def _conviction_sparkline(sat: dict[str, Any]) -> str:
    audits = _ensure_dict_list(sat.get("audit_log", []))
    if len(audits) < 3:
        return ""

    values: list[float] = []
    for entry in audits:
        value = _safe_float(entry.get("smoothed_conviction"), None)
        if value is None:
            value = _safe_float(entry.get("conviction"), None)
        if value is not None:
            values.append(max(0.0, min(10.0, value)))
    if len(values) < 3:
        return ""

    recent = values[-5:]
    chars: list[str] = []
    max_index = len(SPARKLINE_BLOCKS) - 1
    for value in recent:
        block_index = int(round((value / 10.0) * max_index))
        block_index = max(0, min(max_index, block_index))
        chars.append(SPARKLINE_BLOCKS[block_index])
    return "".join(chars)


def _build_weekly_change_lines(
    results: list[dict[str, Any]],
    allocations: dict[str, Any],
    previous_satellites: dict[str, Any],
) -> list[str]:
    lines = ["## What Changed This Week", ""]
    significant_changes: list[str] = []

    for result in sorted(results, key=lambda item: str(item.get("ticker", ""))):
        ticker = str(result.get("ticker", "")).strip()
        if not ticker:
            continue
        screener = result.get("screener", {}) if isinstance(result.get("screener"), dict) else {}
        auditor = result.get("auditor", {}) if isinstance(result.get("auditor"), dict) else {}
        decision_now = str(auditor.get("decision") or screener.get("quality_verdict") or "N/A").upper()

        allocation = allocations.get(ticker, {}) if isinstance(allocations.get(ticker), dict) else {}
        conviction_now = _safe_float(allocation.get("conviction"), None)

        previous_audit = _previous_week_audit(previous_satellites, ticker)
        decision_prev = str(
            previous_audit.get("decision")
            or previous_audit.get("screener_verdict")
            or "N/A"
        ).upper()
        conviction_prev = _audit_conviction(previous_audit)

        if not previous_audit:
            significant_changes.append(
                f"- **{ticker}:** No prior audit snapshot found; baseline starts this week "
                f"(decision `{decision_now}`, smoothed conviction "
                f"{f'{conviction_now:.2f}' if conviction_now is not None else 'n/a'})."
            )
            continue

        verdict_changed = decision_prev != decision_now
        conviction_delta = None
        if conviction_now is not None and conviction_prev is not None:
            conviction_delta = conviction_now - conviction_prev
        conviction_changed = conviction_delta is not None and abs(conviction_delta) > 1.5

        if verdict_changed or conviction_changed:
            details: list[str] = []
            if verdict_changed:
                details.append(f"verdict `{decision_prev}` -> `{decision_now}`")
            if conviction_changed and conviction_delta is not None and conviction_prev is not None and conviction_now is not None:
                details.append(
                    f"smoothed conviction {conviction_prev:.2f} -> {conviction_now:.2f} "
                    f"({conviction_delta:+.2f})"
                )
            significant_changes.append(f"- **{ticker}:** " + "; ".join(details) + ".")

    if significant_changes:
        lines.extend(significant_changes)
    else:
        lines.append(
            "- No significant ticker-level changes this week: no verdict shifts and no smoothed-conviction move above 1.5 points."
        )

    lines.extend(["", "---", ""])
    return lines


def _ticker_source_label(sat: dict[str, Any]) -> str:
    status = str(sat.get("status", "watchlist")).lower()
    category = str(sat.get("category", "satellite")).lower()
    cat_label = "cores" if category == "core" else "satellites"
    if status == "exited":
        return "exited"
    if status == "active":
        return f"current/{cat_label}"
    return f"watchlist/{cat_label}"


def _ticker_data_completeness(ticker: str, sat: dict[str, Any]) -> str:
    missing: list[str] = []
    screener_path_str = sat.get("screener_cache_path", "")
    screener_path = Path(screener_path_str) if screener_path_str else SCREENER_DIR / f"{ticker}.json"
    if not screener_path.exists():
        missing.append("screener cache")
    else:
        try:
            age_days = (datetime.now() - datetime.fromtimestamp(screener_path.stat().st_mtime)).days
            if age_days > _SCREENER_MAX_AGE_DAYS:
                missing.append(f"screener stale ({age_days}d old)")
        except OSError:
            missing.append("screener cache unreadable")
    ar_path_str = sat.get("annual_report_path", "")
    if not ar_path_str or not Path(ar_path_str).exists():
        missing.append("annual report folder")
    elif not (Path(ar_path_str) / "index.json").exists():
        missing.append("annual report index.json")
    return "Complete" if not missing else "Missing: " + ", ".join(missing)


def build_ticker_report(
    ticker: str,
    rec: dict[str, Any],
    screener: dict[str, Any],
    thesis: dict[str, Any],
    auditor: dict[str, Any],
    ohlcv: dict[str, Any],
) -> str:
    """Build a standalone per-ticker markdown report with stage 1/2/3 sections."""
    lines = [
        f"# {ticker}",
        f"**{TODAY}** | Status: {rec.get('status', 'watchlist')} | Category: {rec.get('category', 'satellite')}",
        "",
    ]

    source_label = _ticker_source_label(rec)
    completeness = _ticker_data_completeness(ticker, rec)
    lines += [
        f"**Source:** `{source_label}` | **Data:** {completeness}",
        "",
    ]

    fundamentals = rec.get("fundamentals") or screener.get("fundamentals_snapshot") or thesis.get("fundamentals_snapshot") or auditor.get("fundamentals_snapshot") or {}
    key_ratios = summarize_table(fundamentals.get("key_ratios") if isinstance(fundamentals, dict) else None)
    shareholding = summarize_table(fundamentals.get("shareholding_pattern") if isinstance(fundamentals, dict) else None)
    quarterly = summarize_table(fundamentals.get("quarterly_results") if isinstance(fundamentals, dict) else None)

    lines += [
        "**Extracted fundamentals:**",
        f"- Key ratios: {key_ratios}",
        f"- Shareholding pattern: {shareholding}",
        f"- Quarterly results: {quarterly}",
        "",
    ]

    stage1_failed = bool(screener.get("stage_failed")) if isinstance(screener, dict) else False
    stage2_failed = bool(thesis.get("stage_failed")) if isinstance(thesis, dict) else False
    stage3_failed = bool(auditor.get("stage_failed")) if isinstance(auditor, dict) else False

    lines += [
        f"**Stage 1 (Screening):**",
        f"- Score: {screener.get('score', '?')}/10" if not stage1_failed else "- Stage 1 failed - model did not respond",
        f"- Quality verdict: {screener.get('quality_verdict', 'N/A')}",
        f"- Trend: {screener.get('trend_read', 'n/a')}",
        "",
    ]

    lines += [
        f"**Stage 2 (Thesis):**",
        f"{thesis.get('thesis', 'No thesis written.').strip()}" if not stage2_failed else "Stage 2 failed - model did not respond",
        "",
    ]

    if thesis.get('narrative_change') or thesis.get('changed_from_prior'):
        lines += [
            f"**Changes since last audit:** {thesis.get('narrative_change') or thesis.get('changed_from_prior')}",
            "",
        ]

    lines += [
        f"**Stage 3 (Audit):**",
        f"- Conviction: {auditor.get('score', '?')}/10" if not stage3_failed else "- Stage 3 failed - model did not respond",
        f"- Decision: {auditor.get('decision', 'HOLD')}",
        f"- Thesis intact: {'Yes' if auditor.get('thesis_intact', True) else 'No'}",
        f"- Decision rationale: {auditor.get('decision_rationale', '')}",
        f"- Devil's advocate: {auditor.get('devils_advocate', '')}",
        "",
    ]

    red_flags = []
    for key in ("broken_assumptions", "new_risks", "red_flags", "invalidation_triggers"):
        value = auditor.get(key, [])
        if isinstance(value, list):
            red_flags.extend(str(item) for item in value if str(item).strip())
    if red_flags:
        lines.append("**Key red flags and thesis invalidation triggers:**")
        for flag in dict.fromkeys(red_flags):
            lines.append(f"- {flag}")
    else:
        lines.append("**Key red flags and thesis invalidation triggers:** None surfaced.")

    recent_tx_lines: list[str] = []
    if rec.get("status") == "active":
        txns = _ensure_dict_list(rec.get("transactions", []))
        for tx in txns[-3:]:
            tx_date = tx.get("date", "?")
            tx_type = str(tx.get("type", "?")).upper()
            tx_qty = tx.get("qty", "?")
            tx_price = tx.get("price", "?")
            tx_notes = tx.get("notes", "")
            tx_str = f"- {tx_date} {tx_type} {tx_qty} × ₹{tx_price}"
            if tx_notes:
                tx_str += f" — {tx_notes}"
            recent_tx_lines.append(tx_str)

    if recent_tx_lines:
        lines += ["", "**Recent transactions (last 3):**"]
        lines.extend(recent_tx_lines)

    return "\n".join(lines)


def write_ticker_report(ticker: str, status: str, category: str, content: str) -> Path:
    """Write per-ticker report to the correct reports/ subfolder."""
    folder = _ticker_report_folder(status, category)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{ticker}_{TODAY}.md"
    atomic_write_text(path, content)
    return path


def build_report(
    results: list[dict],
    satellites: dict,
    allocations: dict,
    data_warnings: list[str] | None = None,
    previous_satellites: dict[str, Any] | None = None,
) -> str:
    portfolio = allocations.get("_portfolio", {}) if isinstance(allocations, dict) else {}
    active_count = sum(1 for s in satellites.values() if isinstance(s, dict) and s.get("status") == "active" and str(s.get("category", "satellite")).lower() == "satellite")

    lines = [
        "# Weekly Satellite Portfolio Auditor",
        f"**Week {WEEK_NO} | {TODAY}**",
        "",
    ]
    lines.extend(
        _build_weekly_change_lines(
            results,
            allocations,
            previous_satellites if isinstance(previous_satellites, dict) else {},
        )
    )
    lines += [
        f"**Weekly SIP:** ₹{SIP_WEEKLY:,.0f}",
        f"**Gross SIP recommendation:** ₹{float(portfolio.get('gross_recommendation', 0.0) or 0.0):,.0f}",
        f"**Active satellites:** {active_count} / {MAX_SATELLITES}",
        f"",
        "## Screening Summary",
        "",
        "| Ticker | Source | Held | Stage 1 | Stage 3 | Decision | SIP |",
        "|---|---|---:|---:|---:|---|---:|",
    ]

    for result in results:
        ticker = result["ticker"]
        sat = satellites.get(ticker, {})
        ticker_label = f"{ticker} [PAPER]" if sat.get("paper") else ticker
        screener = result.get("screener", {})
        auditor = result.get("auditor", {})
        alloc = allocations.get(ticker, {})
        held = "Yes" if result.get("is_held") else "No"
        source = _ticker_source_label(sat)
        stage1 = screener.get("score", "-")
        stage3 = auditor.get("score", "-")
        decision = auditor.get("decision", screener.get("quality_verdict", "-"))
        sip_text = "-"
        if isinstance(alloc, dict) and alloc:
            sip_text = f"₹{float(alloc.get('amount', 0.0) or 0.0):,.0f}"
        lines.append(f"| {ticker_label} | {source} | {held} | {stage1} | {stage3} | {decision} | {sip_text} |")

    combined_warnings: list[str] = []
    if data_warnings:
        combined_warnings.extend(str(item).strip() for item in data_warnings if str(item).strip())
    for result in results:
        ohlcv = result.get("ohlcv", {}) if isinstance(result.get("ohlcv"), dict) else {}
        for warning in ohlcv.get("data_warnings", []):
            text = str(warning).strip()
            if text:
                combined_warnings.append(text)

    if combined_warnings:
        lines += ["", "## Data Warnings", ""]
        for warning in dict.fromkeys(combined_warnings):
            lines.append(f"- {warning}")

    lines += ["", "---", "", "## Detailed Audit", ""]

    ordered_results = sorted(results, key=lambda item: (not item.get("is_held", False), item["ticker"]))
    for result in ordered_results:
        ticker = result["ticker"]
        screener = result.get("screener", {})
        thesis = result.get("thesis", {})
        auditor = result.get("auditor", {})
        fundamentals = (
            result.get("fundamentals")
            or screener.get("fundamentals_snapshot")
            or thesis.get("fundamentals_snapshot")
            or auditor.get("fundamentals_snapshot")
            or {}
        )
        sat = satellites.get(ticker, {})
        ticker_label = f"{ticker} [PAPER]" if sat.get("paper") else ticker
        price = result.get("ohlcv", {}).get("latest_close", 0.0)
        entry = sat.get("avg_price", price) or price
        ret_pct = round((price - entry) / entry * 100, 2) if entry else 0.0
        alloc = allocations.get(ticker, {}) if isinstance(allocations, dict) else {}

        key_ratios = summarize_table(fundamentals.get("key_ratios") if isinstance(fundamentals, dict) else None)
        shareholding = summarize_table(fundamentals.get("shareholding_pattern") if isinstance(fundamentals, dict) else None)
        quarterly = summarize_table(fundamentals.get("quarterly_results") if isinstance(fundamentals, dict) else None)
        sparkline = _conviction_sparkline(sat)
        heading = f"### {ticker_label}"
        if sparkline:
            heading += f" | Conviction trend: {sparkline}"

        stage1_failed = bool(screener.get("stage_failed")) if isinstance(screener, dict) else False
        stage2_failed = bool(thesis.get("stage_failed")) if isinstance(thesis, dict) else False
        stage3_failed = bool(auditor.get("stage_failed")) if isinstance(auditor, dict) else False

        source_label = _ticker_source_label(sat)
        completeness = _ticker_data_completeness(ticker, sat)

        recent_tx_lines: list[str] = []
        if result.get("is_held"):
            txns = _ensure_dict_list(sat.get("transactions", []))
            for tx in txns[-3:]:
                tx_date = tx.get("date", "?")
                tx_type = str(tx.get("type", "?")).upper()
                tx_qty = tx.get("qty", "?")
                tx_price = tx.get("price", "?")
                tx_notes = tx.get("notes", "")
                tx_str = f"- {tx_date} {tx_type} {tx_qty} × ₹{tx_price}"
                if tx_notes:
                    tx_str += f" — {tx_notes}"
                recent_tx_lines.append(tx_str)

        lines += [
            heading,
            f"**Source:** `{source_label}` | **Data:** {completeness}",
            f"**Screening summary:** {screener.get('quality_verdict', 'N/A')} | score {screener.get('score', '?')}/10 | trend: {screener.get('trend_read', 'n/a')}",
            f"",
            f"**Extracted fundamentals:**",
            f"- Key ratios: {key_ratios}",
            f"- Shareholding pattern: {shareholding}",
            f"- Quarterly results: {quarterly}",
            f"",
            f"**Stage 1 score:** {screener.get('score', '?')}/10" if not stage1_failed else "**Stage 1:** Stage 1 failed - model did not respond",
            f"**Stage 2 thesis:**",
            f"{thesis.get('thesis', 'No thesis written.').strip()}" if not stage2_failed else "Stage 2 failed - model did not respond",
            f"",
            f"**Changes since last audit:** {thesis.get('narrative_change') or thesis.get('changed_from_prior') or 'First review / no prior audit.'}",
            f"",
            f"**Stage 3 audit:**",
            f"- Conviction: {auditor.get('score', '?')}/10" if not stage3_failed else "- Stage 3 failed - model did not respond",
            f"- Decision: {auditor.get('decision', 'HOLD')}",
            f"- Thesis intact: {'Yes' if auditor.get('thesis_intact', True) else 'No'}",
            f"- Decision rationale: {auditor.get('decision_rationale', '')}",
            f"- Devil's advocate: {auditor.get('devils_advocate', '')}",
            f"",
        ]

        red_flags = []
        for key in ("broken_assumptions", "new_risks", "red_flags", "invalidation_triggers"):
            value = auditor.get(key, [])
            if isinstance(value, list):
                red_flags.extend(str(item) for item in value if str(item).strip())
        if red_flags:
            lines.append("**Key red flags and thesis invalidation triggers:**")
            for flag in dict.fromkeys(red_flags):
                lines.append(f"- {flag}")
        else:
            lines.append("**Key red flags and thesis invalidation triggers:** None surfaced.")

        if recent_tx_lines:
            lines.append("")
            lines.append("**Recent transactions (last 3):**")
            lines.extend(recent_tx_lines)

        if result.get("is_held") and isinstance(alloc, dict) and alloc:
            lines.append("")
            lines.append(
                f"**SIP recommendation:** ₹{float(alloc.get('amount', 0.0) or 0.0):,.0f} "
                f"({float(alloc.get('multiplier', 0.0) or 0.0):.1f}x weekly SIP, {alloc.get('action', 'HOLD_CASH')})"
            )
        elif result.get("is_held"):
            lines.append("")
            lines.append("**SIP recommendation:** Not available.")
        else:
            lines.append("")
            lines.append("**SIP recommendation:** Not applicable for watchlist candidates.")

        lines += ["", "---", ""]

    closed_positions = sorted(
        (
            (ticker, sat)
            for ticker, sat in satellites.items()
            if isinstance(sat, dict) and sat.get("status") == "exited"
        ),
        key=lambda item: str(item[0]),
    )
    lines += ["## Closed Positions", ""]
    if closed_positions:
        lines += [
            "| Ticker | Exit Date | Exit Reason | Last Conviction |",
            "|---|---|---|---:|",
        ]
        for ticker, sat in closed_positions:
            audits = _ensure_dict_list(sat.get("audit_log", []))
            last_audit = audits[-1] if audits else {}
            last_conviction = _audit_conviction(last_audit)
            conviction_text = f"{last_conviction:.2f}" if last_conviction is not None else "-"
            exit_date = str(sat.get("exit_date") or "-")
            exit_reason = str(sat.get("exit_reason") or "-").replace("\n", " ").replace("|", "/").strip() or "-"
            lines.append(f"| {ticker} | {exit_date} | {exit_reason} | {conviction_text} |")
    else:
        lines.append("No closed positions yet.")

    return "\n".join(lines)


def _report_json(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, default=str)


def build_detailed_report(results: list[dict], satellites: dict, allocations: dict) -> str:
    portfolio = allocations.get("_portfolio", {}) if isinstance(allocations, dict) else {}
    lines = [
        "# Detailed Satellite Report LLM Version",
        f"**Week {WEEK_NO} | {TODAY}**",
        "",
        "This report includes the rendered prompts, the input bundles given to each model, and the structured/raw outputs.",
        "It does not expose hidden chain-of-thought; instead it records concise rationale and self-rated input quality.",
        "",
        f"**Weekly SIP:** ₹{SIP_WEEKLY:,.0f}",
        f"**Gross SIP recommendation:** ₹{float(portfolio.get('gross_recommendation', 0.0) or 0.0):,.0f}",
        f"**Active satellites:** {sum(1 for s in satellites.values() if s.get('status') == 'active')} / {MAX_SATELLITES}",
        f"**Output file:** {DETAILED_OUTPUT_MD}",
        "",
    ]

    ordered_results = sorted(results, key=lambda item: (not item.get("is_held", False), item["ticker"]))
    for result in ordered_results:
        ticker = result["ticker"]
        screener = result.get("screener", {})
        thesis = result.get("thesis", {})
        auditor = result.get("auditor", {})
        alloc = allocations.get(ticker, {}) if isinstance(allocations, dict) else {}
        sat = satellites.get(ticker, {})
        ticker_label = f"{ticker} [PAPER]" if sat.get("paper") else ticker
        lines += [
            f"## {ticker_label}",
            f"**Held:** {'Yes' if result.get('is_held') else 'No'} | **Decision:** {auditor.get('decision', screener.get('quality_verdict', '-'))} | **SIP:** {alloc.get('amount', '-')}",
            "",
        ]

        for stage_name, stage_data in (
            ("Stage 1 — Screener", screener),
            ("Stage 2 — Thesis", thesis),
            ("Stage 3 — Auditor", auditor),
        ):
            if not isinstance(stage_data, dict):
                continue
            lines += [
                f"### {stage_name}",
                f"**Model:** {stage_data.get('model', 'unknown')}",
                f"**Input quality score:** {stage_data.get('input_quality_score', 'n/a')}",
                f"**Confidence score:** {stage_data.get('confidence_score', 'n/a')}",
                f"**Prompt assessment:** {stage_data.get('prompt_assessment', 'n/a')}",
                "",
                "**Prompt**",
                "```text",
                str(stage_data.get("prompt", "")),
                "```",
                "",
                "**Inputs given to the model**",
                "```json",
                _report_json(stage_data.get("inputs", {})),
                "```",
                "",
                "**Structured output**",
                "```json",
                _report_json({k: v for k, v in stage_data.items() if k not in {"raw", "prompt", "inputs"}}),
                "```",
                "",
                "**Raw model output**",
                "```text",
                str(stage_data.get("raw", "")),
                "```",
                "",
            ]

        lines += [
            "### Snapshot Context",
            "**Current satellite record**",
            "```json",
            _report_json(sat),
            "```",
            "",
            "---",
            "",
        ]

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# PRE-RUN WATCHLIST MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def _wl_add(category: str) -> None:
    try:
        ticker = input(f"  Ticker to add to watchlist {category}s: ").strip().upper()
    except EOFError:
        return
    if not ticker:
        return
    all_tickers = load_all_tickers()
    if ticker in all_tickers:
        existing = all_tickers[ticker]
        print(f"  {ticker} already exists as {existing['status']}/{existing['category']}. Skipping.")
        return
    record = normalize_satellite_record(ticker, {})
    record["ticker"] = ticker
    record["status"] = "watchlist"
    record["category"] = category
    record["avg_price"] = None
    record["screener_cache_path"] = str(SCREENER_DIR / f"{ticker}.json")
    report_root = _find_matching_subdir(ANNUAL_REPORTS_SUMMARY_DIR, ticker)
    record["annual_report_path"] = str(report_root) if report_root else ""
    save_single_ticker(record)
    print(f"  \u2713 Added {ticker} to watchlist/{category}s")


def _wl_remove(wl_cores: dict[str, Any], wl_sats: dict[str, Any]) -> None:
    all_wl = {**wl_cores, **wl_sats}
    if not all_wl:
        print("  Watchlist is empty.")
        return
    try:
        ticker = input("  Ticker to remove: ").strip().upper()
    except EOFError:
        return
    if ticker not in all_wl:
        print(f"  {ticker} not found in watchlist.")
        return
    rec = all_wl[ticker]
    path = _ticker_folder(rec["status"], rec["category"]) / f"{ticker}.json"
    try:
        path.unlink()
        print(f"  \u2713 Removed {ticker} from watchlist")
    except Exception as exc:
        print(f"  Failed to remove {ticker}: {exc}")


def _find_in_watchlist(ticker: str) -> tuple[dict[str, Any] | None, str]:
    """Search watchlist for ticker. Returns (record, category) or (None, '')."""
    ticker = ticker.upper()
    for category, folder in [("core", PORTFOLIO_WATCHLIST_CORES_DIR), ("satellite", PORTFOLIO_WATCHLIST_SATS_DIR)]:
        path = folder / f"{ticker}.json"
        if path.exists():
            try:
                rec = normalize_satellite_record(ticker, json.loads(path.read_text(encoding="utf-8")))
                return rec, category
            except Exception:
                pass
    return None, ""


def _do_promote_from_watchlist(ticker: str, rec: dict[str, Any], category: str) -> dict[str, Any] | None:
    """Promote watchlist ticker to active. Returns updated record or None if thesis_history is empty."""
    ticker_upper = ticker.upper()
    if not _ensure_dict_list(rec.get("thesis_history", [])):
        print(f"  {ticker_upper} has no analysis history yet. Run the pipeline on it first before promoting to active.")
        return None

    try:
        confirm = input(f"  {ticker_upper} is on your watchlist. Promote to active position? (y/n): ").strip().lower()
    except EOFError:
        return None
    if confirm != "y":
        return None

    try:
        avg_price = float(input(f"  Avg price (₹): ").strip())
    except (ValueError, EOFError):
        print("  Invalid price.")
        return None

    try:
        qty_str = input(f"  Qty [0]: ").strip() or "0"
        qty = int(qty_str)
    except (ValueError, EOFError):
        qty = 0

    try:
        entry_date = input(f"  Entry date [YYYY-MM-DD, default {TODAY}]: ").strip() or TODAY
    except EOFError:
        entry_date = TODAY

    rec["status"] = "active"
    rec["category"] = category
    rec["paper"] = False
    rec["avg_price"] = avg_price
    rec["total_qty"] = qty
    rec["entry_date"] = entry_date
    rec["total_invested"] = round(avg_price * qty, 2)
    _tx_record(rec, {
        "date": TODAY,
        "type": "buy",
        "qty": qty,
        "price": avg_price,
        "notes": "Promoted from watchlist"
    })
    print(f"  ✓ Promoted {ticker_upper} → current/{category}s @ ₹{avg_price} | qty {qty}")
    save_single_ticker(rec)
    move_ticker_reports(ticker_upper, "watchlist", "active", category)
    return rec


def _wl_promote(wl_cores: dict[str, Any], wl_sats: dict[str, Any]) -> None:
    all_wl = {**wl_cores, **wl_sats}
    if not all_wl:
        print("  Watchlist is empty.")
        return
    try:
        ticker = input("  Ticker to promote to current (active): ").strip().upper()
    except EOFError:
        return
    if ticker not in all_wl:
        print(f"  {ticker} not found in watchlist.")
        return
    category = "core" if ticker in wl_cores else "satellite"
    rec = all_wl[ticker]
    _do_promote_from_watchlist(ticker, rec, category)


def _view_portfolio() -> None:
    """Print a concise table of the current portfolio and watchlist."""
    def _print_section(title: str, records: dict, watchlist: bool = False) -> None:
        pad = "\u2500" * max(1, 54 - len(title))
        print(f"\n  \u2500\u2500 {title} {pad}")
        print(f"  {'TICKER':<16} {'AVG PRICE':>10} {'QTY':>6} {'TOTAL INV':>12} {'CONVICTION':<12}")
        print("  " + "\u2500" * 60)
        if not records:
            print("  (none)")
        for ticker, rec in sorted(records.items()):
            if watchlist:
                print(f"  {ticker:<16} {'N/A':>10} {'N/A':>6} {'N/A':>12} {'N/A':<12}")
            else:
                avg   = rec.get("avg_price") or rec.get("entry_price")
                qty   = rec.get("total_qty") or rec.get("holding_quantity")
                inv   = rec.get("total_invested")
                conv  = str(rec.get("conviction", "\u2014"))
                avg_s = f"\u20b9{avg:,.2f}" if isinstance(avg, (int, float)) else "\u2014"
                qty_s = str(int(qty)) if isinstance(qty, (int, float)) else "\u2014"
                inv_s = f"\u20b9{inv:,.0f}" if isinstance(inv, (int, float)) else "\u2014"
                print(f"  {ticker:<16} {avg_s:>10} {qty_s:>6} {inv_s:>12} {conv:<12}")
        print("  " + "\u2500" * 60)

    _print_section("CURRENT CORES",        load_all_tickers([PORTFOLIO_CURRENT_CORES_DIR]))
    _print_section("CURRENT SATELLITES",   load_all_tickers([PORTFOLIO_CURRENT_SATS_DIR]))
    _print_section("WATCHLIST CORES",      load_all_tickers([PORTFOLIO_WATCHLIST_CORES_DIR]), watchlist=True)
    _print_section("WATCHLIST SATELLITES", load_all_tickers([PORTFOLIO_WATCHLIST_SATS_DIR]),  watchlist=True)


def watchlist_management_loop() -> None:
    """Interactive loop to manage the watchlist before running the pipeline."""
    while True:
        wl_cores = load_all_tickers([PORTFOLIO_WATCHLIST_CORES_DIR])
        wl_sats  = load_all_tickers([PORTFOLIO_WATCHLIST_SATS_DIR])

        print("\n  \u2500\u2500 WATCHLIST \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
        print(f"  Cores      ({len(wl_cores)}): {', '.join(sorted(wl_cores)) or 'none'}")
        print(f"  Satellites ({len(wl_sats)}): {', '.join(sorted(wl_sats)) or 'none'}")
        print("  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
        print("  a) Add to watchlist cores")
        print("  b) Add to watchlist satellites")
        print("  c) Remove from watchlist")
        print("  d) Promote to current (active)")
        print("  e) Done \u2014 proceed")
        print("  v) View portfolio")

        try:
            choice = input("\n  Choice [a-e, v]: ").strip().lower()
        except EOFError:
            break

        if choice == "e":
            break
        elif choice == "a":
            _wl_add(category="core")
        elif choice == "b":
            _wl_add(category="satellite")
        elif choice == "c":
            _wl_remove(wl_cores, wl_sats)
        elif choice == "d":
            _wl_promote(wl_cores, wl_sats)
        elif choice == "v":
            _view_portfolio()
        else:
            print("  Please enter a, b, c, d, e, or v.")


# ═══════════════════════════════════════════════════════════════════════════════
# PRE-RUN TRANSACTION UPDATES
# ═══════════════════════════════════════════════════════════════════════════════

def _tx_prompt_ticker(active: dict[str, Any]) -> str | None:
    """Ask for a ticker and validate it's in the active portfolio."""
    try:
        ticker = input("  Ticker: ").strip().upper()
    except EOFError:
        return None
    if not ticker:
        return None
    if ticker not in active:
        print(f"  {ticker} not found in active portfolio.")
        return None
    return ticker


def _tx_record(record: dict[str, Any], tx: dict[str, Any]) -> None:
    txns = _ensure_dict_list(record.get("transactions"))
    txns.append(tx)
    record["transactions"] = txns
    save_single_ticker(record)


def _tx_buy(active: dict[str, Any]) -> None:
    try:
        ticker = input("  Ticker: ").strip().upper()
    except EOFError:
        return
    if not ticker:
        return

    if ticker in active:
        rec = dict(active[ticker])
        try:
            qty   = int(input("  Qty bought: ").strip())
            price = float(input("  Buy price (\u20b9): ").strip())
        except (ValueError, EOFError):
            print("  Invalid qty or price.")
            return
        try:
            notes = input("  Notes (optional): ").strip()
        except EOFError:
            notes = ""

        old_invested = _safe_float(rec.get("total_invested"), 0.0) or 0.0
        old_qty      = _safe_int(rec.get("total_qty"), 0) or 0
        new_qty      = old_qty + qty
        new_invested = old_invested + qty * price
        new_avg      = round(new_invested / new_qty, 4) if new_qty else rec.get("avg_price") or price

        rec["total_qty"]      = new_qty
        rec["total_invested"] = round(new_invested, 2)
        rec["avg_price"]      = new_avg
        _tx_record(rec, {"date": TODAY, "type": "buy", "qty": qty, "price": price, "notes": notes})
        print(
            f"  \u2713 BUY {qty} \u00d7 {ticker} @ \u20b9{price}"
            f"  |  new avg \u20b9{new_avg}  |  total qty {new_qty}"
        )
    else:
        rec, category = _find_in_watchlist(ticker)
        if rec is None:
            print(f"  {ticker} not found in watchlist or active portfolio. Add to watchlist first.")
            return
        if _do_promote_from_watchlist(ticker, rec, category) is None:
            return


def _tx_sell(active: dict[str, Any]) -> None:
    ticker = _tx_prompt_ticker(active)
    if not ticker:
        return
    rec = dict(active[ticker])
    old_qty = _safe_int(rec.get("total_qty"), 0) or 0
    try:
        qty   = int(input(f"  Qty sold (held {old_qty}): ").strip())
        price = float(input("  Sell price (\u20b9): ").strip())
    except (ValueError, EOFError):
        print("  Invalid qty or price.")
        return
    if qty > old_qty:
        print(f"  Cannot sell {qty} — only {old_qty} held.")
        return
    try:
        notes = input("  Notes (optional): ").strip()
    except EOFError:
        notes = ""

    avg_price    = _safe_float(rec.get("avg_price"), price) or price
    new_qty      = old_qty - qty
    new_invested = round(avg_price * new_qty, 2)

    rec["total_qty"]      = new_qty
    rec["total_invested"] = new_invested
    _tx_record(rec, {"date": TODAY, "type": "sell", "qty": qty, "price": price, "notes": notes})
    print(f"  \u2713 SELL {qty} \u00d7 {ticker} @ \u20b9{price}  |  remaining qty {new_qty}")

    if new_qty == 0:
        try:
            confirm = input("  Qty is now 0. Mark as fully exited? [y/N]: ").strip().lower()
        except EOFError:
            confirm = "n"
        if confirm == "y":
            _do_exit(rec, ticker, exit_price=price)


def _do_exit(rec: dict[str, Any], ticker: str, exit_price: float | None = None) -> None:
    if rec.get("status") != "active":
        log.warning(f"[{ticker}] Skipping exit — ticker is not an active holding (status={rec.get('status')}). Only active tickers can be exited.")
        return
    try:
        reason = input("  Exit reason: ").strip()
    except EOFError:
        reason = ""
    if exit_price is None:
        try:
            ep_raw = input("  Exit price (\u20b9, or blank to skip): ").strip()
            exit_price = float(ep_raw) if ep_raw else None
        except (ValueError, EOFError):
            exit_price = None

    _old_category = rec.get("category", "satellite")
    rec["status"]    = "exited"
    rec["exit_date"] = TODAY
    rec["exit_reason"] = reason
    if exit_price is not None:
        _tx_record(rec, {
            "date": TODAY, "type": "exit",
            "qty": rec.get("total_qty", 0), "price": exit_price, "notes": reason,
        })
    else:
        save_single_ticker(rec)
    print(f"  \u2713 {ticker} marked as exited on {TODAY}.")
    move_ticker_reports(ticker, "active", "exited", _old_category)


def _tx_exit(active: dict[str, Any]) -> None:
    ticker = _tx_prompt_ticker(active)
    if not ticker:
        return
    rec = dict(active[ticker])
    _do_exit(rec, ticker)


def transaction_update_loop() -> None:
    """Interactive loop to record buy/sell/exit transactions before running the pipeline."""
    while True:
        active = load_all_tickers([PORTFOLIO_CURRENT_CORES_DIR, PORTFOLIO_CURRENT_SATS_DIR])
        if not active:
            print("\n  No active holdings. Skipping transaction updates.")
            break

        print("\n  \u2500\u2500 ACTIVE HOLDINGS \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
        for t, s in sorted(active.items()):
            avg = s.get("avg_price")
            qty = s.get("total_qty", 0)
            cat = s.get("category", "satellite")
            avg_str = f"\u20b9{avg:.2f}" if avg is not None else "—"
            print(f"  {t:<16} [{cat}]  avg {avg_str}  qty {qty}")
        print("  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
        print("  b) Record BUY")
        print("  s) Record SELL (partial)")
        print("  x) Full EXIT a position")
        print("  d) Done \u2014 proceed")

        try:
            choice = input("\n  Choice [b/s/x/d]: ").strip().lower()
        except EOFError:
            break

        if choice == "d":
            break
        elif choice == "b":
            _tx_buy(active)
        elif choice == "s":
            _tx_sell(active)
        elif choice == "x":
            _tx_exit(active)
        else:
            print("  Please enter b, s, x, or d.")


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP MENU
# ═══════════════════════════════════════════════════════════════════════════════

def startup_menu() -> tuple[list[str], int]:
    """Interactive startup menu — returns (selected_tickers, choice)."""
    print(
        "\n  INDIAN EQUITY RESEARCH PIPELINE"
        "\n  ================================"
        "\n  1) Current cores only"
        "\n  2) Current satellites only"
        "\n  3) Both current (cores + satellites)"
        "\n  4) Watchlist cores only"
        "\n  5) Watchlist satellites only"
        "\n  6) Both watchlist (cores + satellites)"
        "\n  7) Everything (current + watchlist, all categories)"
    )
    while True:
        try:
            raw = input("\n  Choice [1-7]: ").strip()
            choice = int(raw)
            if 1 <= choice <= 7:
                break
        except (ValueError, EOFError):
            pass
        print("  Please enter a number between 1 and 7.")

    folder_map: dict[int, list[Path]] = {
        1: [PORTFOLIO_CURRENT_CORES_DIR],
        2: [PORTFOLIO_CURRENT_SATS_DIR],
        3: [PORTFOLIO_CURRENT_CORES_DIR, PORTFOLIO_CURRENT_SATS_DIR],
        4: [PORTFOLIO_WATCHLIST_CORES_DIR],
        5: [PORTFOLIO_WATCHLIST_SATS_DIR],
        6: [PORTFOLIO_WATCHLIST_CORES_DIR, PORTFOLIO_WATCHLIST_SATS_DIR],
        7: _all_portfolio_dirs(),
    }
    records = load_all_tickers(folder_map[choice])
    tickers = list(records.keys())
    label = ", ".join(tickers) if tickers else "none"
    print(f"  → {len(tickers)} ticker(s) selected: {label}\n")
    return tickers, choice


# ═══════════════════════════════════════════════════════════════════════════════
# DATA VERIFICATION GATE
# ═══════════════════════════════════════════════════════════════════════════════

_SCREENER_MAX_AGE_DAYS = 7


async def data_verification_gate(
    tickers: list[str],
    *,
    show_live: bool = False,
) -> list[str]:
    """
    Lightweight pre-run data check (cache presence + age only — no live fetches).
    For each issue, prompts the user:
      a) skip ticker   b) run anyway   c) refresh screener (when applicable)   d) abort pipeline
    Returns the subset of tickers cleared to run.
    """
    print("\n  \u2500\u2500\u2500\u2500 DATA VERIFICATION GATE \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
    now = datetime.now()

    budget_index = BUDGET_SUMMARY_DIR / "index.json"
    if budget_index.exists():
        try:
            age_days = (now - datetime.fromtimestamp(budget_index.stat().st_mtime)).days
            if age_days > 90:
                last_updated = datetime.fromtimestamp(budget_index.stat().st_mtime).strftime("%Y-%m-%d")
                print(f"\n  \u26a0 Budget data is stale (last updated {last_updated}). Consider re-running budget_processor.py.")
                print("    Options: a) Continue anyway  b) Abort")
                try:
                    ans = input("  Choice [a/b]: ").strip().lower()
                except EOFError:
                    ans = "a"
                if ans == "b":
                    print("\n  Pipeline aborted.")
                    sys.exit(0)
        except Exception:
            pass

    cleared: list[str] = []

    for ticker in tickers:
        issues: list[tuple[str, str, bool]] = []  # (key, description, refreshable)

        screener_path = SCREENER_DIR / f"{ticker}.json"
        if not screener_path.exists():
            issues.append(("screener_missing", "screener cache missing", True))
        else:
            age_days = (now - datetime.fromtimestamp(screener_path.stat().st_mtime)).days
            if age_days > _SCREENER_MAX_AGE_DAYS:
                issues.append(("screener_stale", f"screener cache is {age_days}d old (>{_SCREENER_MAX_AGE_DAYS}d)", True))

        report_root = _find_matching_subdir(ANNUAL_REPORTS_SUMMARY_DIR, ticker)
        if report_root is None:
            issues.append(("annual_missing", "annual report folder missing", False))
        elif not (report_root / "index.json").exists():
            issues.append(("annual_index", "annual report index.json missing", False))

        if not issues:
            print(f"  {ticker:<16} \u2713 all checks passed")
            cleared.append(ticker)
            continue

        skip_ticker = False
        abort_all   = False

        for key, description, refreshable in issues:
            while True:
                print(f"\n  [{ticker}] {description}")
                print("    a) Skip this ticker for this run")
                print("    b) Run anyway (accept data gap)")
                if refreshable:
                    print("    c) Refresh screener now")
                print("    d) Abort pipeline")
                opts = "a/b/c/d" if refreshable else "a/b/d"
                try:
                    ans = input(f"  Choice [{opts}]: ").strip().lower()
                except EOFError:
                    ans = "a"

                if ans == "a":
                    skip_ticker = True
                    break
                elif ans == "b":
                    break
                elif ans == "c" and refreshable:
                    print(f"  Refreshing screener for {ticker} \u2026")
                    try:
                        await cmd_screener(ticker, show_live=show_live)
                        print(f"  \u2713 Screener refreshed.")
                    except Exception as exc:
                        print(f"  Refresh failed: {exc}")
                    break
                elif ans == "d":
                    abort_all = True
                    break
                else:
                    valid = "a, b, c, or d" if refreshable else "a, b, or d"
                    print(f"  Please enter {valid}.")

            if skip_ticker or abort_all:
                break

        if abort_all:
            print("\n  Pipeline aborted.")
            sys.exit(0)
        if skip_ticker:
            print(f"  Skipping {ticker}.")
            continue
        cleared.append(ticker)

    print(
        f"\n  \u2192 {len(cleared)}/{len(tickers)} ticker(s) cleared: "
        f"{', '.join(cleared) or 'none'}\n"
    )
    return cleared


# ═══════════════════════════════════════════════════════════════════════════════
# POST-RUN TRANSACTION UPDATE
# ═══════════════════════════════════════════════════════════════════════════════

def _post_run_transaction_prompt(all_results: list[dict[str, Any]]) -> None:
    """Print LLM recommendations summary, then optionally run the buy/sell loop.

    Position update functions (_tx_buy, _tx_sell, _tx_exit) are only reachable
    from interactive prompts — never called from LLM stage functions — making it
    architecturally impossible for the pipeline to mutate position data automatically.
    """
    if not all_results:
        return

    print("\n  RUN COMPLETE — LLM RECOMMENDATIONS")
    print("  " + "=" * 47)
    print(f"  {'TICKER':<16} {'RECOMMENDATION':<18} CONVICTION")
    print("  " + "─" * 47)
    for result in all_results:
        ticker = result["ticker"]
        auditor = result.get("auditor", {})
        screener = result.get("screener", {})
        decision = auditor.get("decision") or screener.get("quality_verdict") or "—"
        conviction = auditor.get("score")
        conv_str = f"{conviction:.1f}" if isinstance(conviction, (int, float)) else "—"
        print(f"  {ticker:<16} {decision:<18} (Conviction: {conv_str})")
    print("  " + "─" * 47)

    try:
        ans = input("\n  Update any buy/sell transactions based on your own decisions? (y/n): ").strip().lower()
    except EOFError:
        return
    if ans != "y":
        return

    transaction_update_loop()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

async def run_pipeline(watchlist: list[str], searxng_url: str, *, show_live: bool = False) -> None:
    log.info("═" * 60)
    log.info(f"  Weekly Satellite Portfolio Auditor  │  Week {WEEK_NO} | {TODAY}")
    log.info("═" * 60)

    satellites = load_satellites()
    satellites_before_update = copy.deepcopy(satellites)
    verify_ollama_models_available()

    stage_times = {
        "Stage 1": 0.0,
        "Stage 2": 0.0,
        "Stage 3": 0.0,
        "Stage 4": 0.0,
    }

    active = [t for t, s in satellites.items() if s.get("status") == "active"]
    full_list = list(dict.fromkeys(watchlist))  # watchlist already scoped by menu selection
    log.info(f"  Active satellites in scope: {[t for t in full_list if satellites.get(t, {}).get('status') == 'active']}")
    log.info(f"  All tickers in scope: {full_list}")

    data_warnings: list[str] = []
    cleared = await data_verification_gate(full_list, show_live=show_live)
    if not cleared:
        log.error("No tickers passed the data verification gate. Exiting.")
        return

    all_results: list[dict[str, Any]] = []

    for ticker in cleared:
        log.info(f"\n{'─'*55}")
        log.info(f"  Processing: {ticker}")
        log.info(f"{'─'*55}")
        is_held = ticker in active

        try:
            ohlcv = await run_blocking(fetch_ohlcv, ticker)
            if isinstance(ohlcv, dict):
                for warning in ohlcv.get("data_warnings", []):
                    text = str(warning).strip()
                    if text:
                        data_warnings.append(text)
            news = await run_blocking(fetch_news, ticker, searxng_url)
            fundamentals = await load_screener(ticker, show_live=show_live)
            if isinstance(fundamentals, dict):
                for warning in fundamentals.get("data_warnings", []):
                    text = str(warning).strip()
                    if text:
                        data_warnings.append(text)
            prior_sat = satellites.get(ticker, {})
            _ar_str = prior_sat.get("annual_report_path", "") if isinstance(prior_sat, dict) else ""
            ar_path: Path | None = Path(_ar_str) if _ar_str and Path(_ar_str).exists() else None
            stage_failures: list[str] = []
            category = str(prior_sat.get("category", "satellite")).lower() if isinstance(prior_sat, dict) else "satellite"

            started = time.perf_counter()
            ok, screener, stage_err = await run_stage_with_retries(
                "Stage 1",
                ticker,
                lambda: run_blocking(run_screener, ticker, ohlcv, news, fundamentals, annual_report_path=ar_path, category=category),
            )
            elapsed = time.perf_counter() - started
            stage_times["Stage 1"] += elapsed
            log.info("[%s] Stage 1 completed in %.2fs", ticker, elapsed)
            if not ok or not isinstance(screener, dict):
                stage_failures.append("Stage 1")
                screener = build_stage_failure_payload(1, _stage_model(category, 1), stage_err)
                screener.setdefault("score", None)
                screener.setdefault("quality_verdict", "N/A")
                screener.setdefault("trend_read", "Stage 1 failed - model did not respond")
            else:
                screener.setdefault("stage_failed", False)

            started = time.perf_counter()
            ok, thesis, stage_err = await run_stage_with_retries(
                "Stage 2",
                ticker,
                lambda: run_blocking(run_thesis, ticker, ohlcv, screener, news, fundamentals, prior_sat, annual_report_path=ar_path, category=category),
            )
            elapsed = time.perf_counter() - started
            stage_times["Stage 2"] += elapsed
            log.info("[%s] Stage 2 completed in %.2fs", ticker, elapsed)
            if not ok or not isinstance(thesis, dict):
                stage_failures.append("Stage 2")
                thesis = build_stage_failure_payload(2, _stage_model(category, 2), stage_err)
                thesis.setdefault("thesis", "Stage 2 failed - model did not respond")
                thesis.setdefault("narrative_change", "Stage 2 failed - model did not respond")
                thesis.setdefault("changed_from_prior", "Stage 2 failed - model did not respond")
                thesis.setdefault("assumptions", [])
            else:
                thesis.setdefault("stage_failed", False)

            # Immediately persist thesis to per-ticker JSON after Stage 2
            thesis_text = thesis.get("thesis", "")
            if thesis_text and "Stage 2 failed" not in thesis_text:
                _rec = dict(satellites.get(ticker, prior_sat))
                if not _rec.get("original_thesis"):
                    _rec["original_thesis"] = thesis_text
                _rec["thesis_assumptions"] = _ensure_text_list(
                    thesis.get("assumptions", _rec.get("thesis_assumptions", []))
                )
                _thesis_entry = {
                    "week": current_week_key(),
                    "date": TODAY,
                    "score": screener.get("score"),
                    "thesis": thesis_text,
                    "changed_from_prior": thesis.get("changed_from_prior", ""),
                    "narrative_change": thesis.get("narrative_change", ""),
                    "assumptions": thesis.get("assumptions", []),
                    "bull_case": thesis.get("bull_case", ""),
                    "bear_case": thesis.get("bear_case", ""),
                    "time_horizon": thesis.get("time_horizon", ""),
                    "exit_triggers": thesis.get("exit_triggers", []),
                    "price": ohlcv.get("latest_close"),
                    "one_year_return_pct": ohlcv.get("one_year_return_pct"),
                    "three_month_return_pct": ohlcv.get("three_month_return_pct"),
                }
                _rec.setdefault("thesis_history", []).append(_thesis_entry)
                satellites[ticker] = _rec
                save_single_ticker(_rec)
                log.info("[%s] Thesis entry saved to per-ticker JSON.", ticker)

            started = time.perf_counter()
            ok, auditor, stage_err = await run_stage_with_retries(
                "Stage 3",
                ticker,
                lambda: run_blocking(run_auditor, ticker, ohlcv, prior_sat, thesis, news, fundamentals, screener, annual_report_path=ar_path, category=category),
            )
            elapsed = time.perf_counter() - started
            stage_times["Stage 3"] += elapsed
            log.info("[%s] Stage 3 completed in %.2fs", ticker, elapsed)
            if not ok or not isinstance(auditor, dict):
                stage_failures.append("Stage 3")
                auditor = build_stage_failure_payload(3, _stage_model(category, 3), stage_err)
                fallback_decision = str(screener.get("quality_verdict") or "HOLD").upper()
                if fallback_decision not in {"ADD", "HOLD", "TRIM", "EXIT"}:
                    fallback_decision = "HOLD"
                auditor.setdefault("score", None)
                auditor.setdefault("decision", fallback_decision)
                auditor.setdefault("decision_rationale", "Stage 3 failed - model did not respond")
                auditor.setdefault("thesis_intact", False)
                auditor.setdefault("assumptions_status", {})
                auditor.setdefault("broken_assumptions", [])
                auditor.setdefault("new_risks", [])
                auditor.setdefault("invalidation_triggers", [])
                auditor.setdefault("red_flags", [])
                auditor.setdefault("devils_advocate", "Stage 3 failed - model did not respond")
            else:
                auditor.setdefault("stage_failed", False)

            # Immediately persist audit entry to per-ticker JSON after Stage 3
            # (allocation fields filled in after compute_sip_sizing)
            _rec = dict(satellites.get(ticker, prior_sat))
            _audit_entry: dict[str, Any] = {
                "week": current_week_key(),
                "date": TODAY,
                "price": ohlcv.get("latest_close"),
                "one_year_return_pct": ohlcv.get("one_year_return_pct"),
                "three_month_return_pct": ohlcv.get("three_month_return_pct"),
                "screener_score": screener.get("score"),
                "screener_verdict": screener.get("quality_verdict"),
                "auditor_score": auditor.get("score"),
                "conviction": _safe_float(auditor.get("score"), None),
                "smoothed_conviction": None,
                "sip_multiplier": 0.0,
                "sip_amount": 0,
                "action": "PENDING",
                "assumptions_status": auditor.get("assumptions_status", {}),
                "broken_assumptions": auditor.get("broken_assumptions", []),
                "new_risks": auditor.get("new_risks", []),
                "invalidation_triggers": auditor.get("invalidation_triggers", []),
                "red_flags": auditor.get("red_flags", []),
                "thesis_intact": auditor.get("thesis_intact", True),
                "decision": auditor.get("decision", "HOLD"),
                "decision_rationale": auditor.get("decision_rationale", ""),
                "add_recommended": auditor.get("add_recommended", False),
                "add_rationale": auditor.get("add_rationale", ""),
                "devils_advocate": auditor.get("devils_advocate", ""),
                "narrative_change": thesis.get("narrative_change", ""),
                "changed_from_prior": thesis.get("changed_from_prior", ""),
                "stage_failed": bool(stage_failures),
                "failed_stages": list(stage_failures),
            }
            _rec.setdefault("audit_log", []).append(_audit_entry)
            if auditor.get("decision") == "EXIT" and _rec.get("status") == "active":
                _rec["status"] = "exited"
                _rec["exit_date"] = TODAY
                _rec["exit_reason"] = auditor.get("decision_rationale", "")
            satellites[ticker] = _rec
            save_single_ticker(_rec)
            log.info("[%s] Audit entry saved to per-ticker JSON.", ticker)

            # Write per-ticker report immediately after Stage 3
            _final_status = str(_rec.get("status", "watchlist"))
            _ticker_md = build_ticker_report(ticker, _rec, screener, thesis, auditor, ohlcv)
            _report_path = write_ticker_report(ticker, _final_status, category, _ticker_md)
            log.info("[%s] Per-ticker report written → %s", ticker, _report_path)

            annual_warning = ANNUAL_REPORT_CONTEXT_WARNINGS.get(ticker)
            if annual_warning:
                data_warnings.append(annual_warning)

            all_results.append(
                {
                    "ticker": ticker,
                    "is_held": is_held,
                    "ohlcv": ohlcv,
                    "fundamentals": fundamentals,
                    "screener": screener,
                    "thesis": thesis,
                    "auditor": auditor,
                    "stage_failures": stage_failures,
                }
            )

        except Exception as exc:
            log.error(f"[{ticker}] FAILED: {exc}")
            continue

    allocations = compute_sip_sizing(all_results, satellites)
    log.info(f"\n  Allocations: {allocations}")

    weekly_memo = ""
    if all_results:
        started = time.perf_counter()
        _categories = [str(satellites.get(r["ticker"], {}).get("category", "satellite")).lower() for r in all_results]
        _memo_category = "core" if _categories.count("core") > _categories.count("satellite") else "satellite"
        ok, weekly_memo, stage4_err = await run_stage_with_retries(
            "Stage 4",
            "PORTFOLIO",
            lambda: run_blocking(run_weekly_portfolio_memo, all_results, allocations, category=_memo_category),
        )
        elapsed = time.perf_counter() - started
        stage_times["Stage 4"] += elapsed
        if ok and isinstance(weekly_memo, str):
            log.info("Stage 4 completed in %.2fs", elapsed)
        else:
            weekly_memo = ""
            log.warning("Stage 4 synthesis failed: %s", stage4_err)
            log.warning("Stage 4 failed after %.2fs", elapsed)

    # Backfill allocation fields into the last audit_log entry saved per-stage above
    for result in all_results:
        ticker = result["ticker"]
        allocation = allocations.get(ticker, {})
        rec = satellites.get(ticker)
        if not isinstance(rec, dict):
            continue
        audit_log = rec.get("audit_log")
        if not isinstance(audit_log, list) or not audit_log:
            continue
        last = audit_log[-1]
        recommendation_amount = allocation.get("amount", 0) if allocation else 0
        last["smoothed_conviction"] = allocation.get("conviction") if allocation else None
        last["sip_multiplier"] = allocation.get("multiplier", 0.0) if allocation else 0.0
        last["sip_amount"] = recommendation_amount
        last["action"] = allocation.get("action", "HOLD_CASH") if allocation else "HOLD_CASH"
        if recommendation_amount and recommendation_amount > 0 and rec.get("status") != "exited":
            rec["total_invested"] = float(rec.get("total_invested", 0.0) or 0.0) + float(recommendation_amount)
        satellites[ticker] = rec
        save_single_ticker(rec)

    log.info("✅  Per-ticker JSON files updated")

    md = build_report(
        all_results,
        satellites,
        allocations,
        data_warnings=data_warnings,
        previous_satellites=satellites_before_update,
    )
    if weekly_memo:
        md = md.rstrip() + "\n\n## Weekly Portfolio Memo\n\n" + weekly_memo.strip() + "\n"
    atomic_write_text(OUTPUT_MD, md)
    log.info(f"✅  Report → {OUTPUT_MD}")

    detailed_md = build_detailed_report(all_results, satellites, allocations)
    atomic_write_text(DETAILED_OUTPUT_MD, detailed_md)
    log.info(f"✅  Detailed report → {DETAILED_OUTPUT_MD}")

    watchlist_set = {str(item).upper().strip() for item in watchlist if str(item).strip()}
    historical_satellites = {
        ticker
        for ticker, sat in satellites.items()
        if isinstance(sat, dict) and sat.get("status") in {"active", "exited"} and str(sat.get("category", "satellite")).lower() == "satellite"
    }
    drift_tickers = sorted(historical_satellites - watchlist_set)
    if drift_tickers:
        log.warning(
            "Watchlist drift alert: these satellites have prior history but are not reviewed this week: %s",
            ", ".join(drift_tickers),
        )

    log.info("\n" + "═" * 60)
    log.info("  SUMMARY")
    log.info("═" * 60)
    log.info(f"  {'TICKER':<15} {'HELD':<6} {'SCREEN':>7} {'AUDIT':>6} {'DECISION'}")
    log.info(f"  {'─'*15} {'─'*6} {'─'*7} {'─'*6} {'─'*8}")
    for result in all_results:
        log.info(
            f"  {result['ticker']:<15} {'📌' if result['is_held'] else '  ':<6} "
            f"{result.get('screener', {}).get('score', '─'):>7} "
            f"{result.get('auditor', {}).get('score', '─'):>6} "
            f"{result.get('auditor', {}).get('decision', result.get('screener', {}).get('quality_verdict', '─'))}"
        )
    log.info("═" * 60)
    log.info(f"\n🚀  Paste {OUTPUT_MD} into Claude Pro for final review.")
    total_llm_time = stage_times["Stage 1"] + stage_times["Stage 2"] + stage_times["Stage 3"] + stage_times["Stage 4"]
    log.info("LLM STAGE TIMING (seconds)")
    log.info("Stage 1 | Stage 2 | Stage 3 | Stage 4 | Total")
    log.info(
        "%.2f | %.2f | %.2f | %.2f | %.2f",
        stage_times["Stage 1"],
        stage_times["Stage 2"],
        stage_times["Stage 3"],
        stage_times["Stage 4"],
        total_llm_time,
    )

    _post_run_transaction_prompt(all_results)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Weekly Satellite Portfolio Auditor — run weekly",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python market_pipeline.py
  python market_pipeline.py --add    "DIXON.NS:840:30"
  python market_pipeline.py --paper  "DIXON.NS:840:30"
  python market_pipeline.py --exit   "DIXON.NS"
  python market_pipeline.py --screener "DIXON.NS"
  python market_pipeline.py --watchlist my_stocks.json
        """,
    )
    parser.add_argument("--watchlist", default=None)
    parser.add_argument("--searxng", default="http://localhost:8080")
    parser.add_argument("--add", metavar="TICKER:AVG_PRICE:ALLOC_PCT")
    parser.add_argument("--paper", metavar="TICKER:AVG_PRICE:ALLOC_PCT")
    parser.add_argument("--exit", metavar="TICKER")
    parser.add_argument(
        "--screener",
        metavar="TICKER",
        help="Fetch screener.in data via browser-use MCP and cache it locally",
    )
    parser.add_argument(
        "--browser-live",
        action="store_true",
        help="Open the Browser Use live preview URL when one is available",
    )
    return parser


async def main_async() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # Auto-migrate legacy data on first run if portfolio_data/ is empty
    if _portfolio_dirs_empty() and (SATELLITES_FILE.exists() or WATCHLIST_FILE.exists()):
        migrate_legacy_data()

    if args.add:
        cmd_add(args.add)
        return
    if args.paper:
        cmd_paper(args.paper)
        return
    if args.exit:
        cmd_exit(args.exit)
        return
    if args.screener:
        await cmd_screener(args.screener, show_live=args.browser_live or BROWSER_USE_SHOW_LIVE)
        return

    if args.watchlist is not None:
        wl_path = Path(args.watchlist)
        if not wl_path.exists():
            log.error("--watchlist file not found: %s", wl_path)
            sys.exit(1)
        watchlist_raw = _load_json_file(wl_path, [])
        if not isinstance(watchlist_raw, list) or not watchlist_raw:
            log.error("--watchlist file must be a non-empty JSON array.")
            sys.exit(1)
        tickers = [str(item).upper() for item in watchlist_raw if str(item).strip()]
    else:
        tickers, _choice = startup_menu()
        if not tickers:
            log.error("No tickers found for the selected category. Add tickers first.")
            sys.exit(1)

    watchlist_management_loop()
    transaction_update_loop()

    await run_pipeline(
        tickers,
        args.searxng,
        show_live=args.browser_live or BROWSER_USE_SHOW_LIVE,
    )


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
