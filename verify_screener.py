#!/usr/bin/env python3
"""Standalone Screener.in verifier for local payload inspection."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

WATCHLIST_FILE = SCRIPT_DIR / "watchlist.json"

from market_pipeline import (  # noqa: E402
    SCREENER_COOKIE,
    TODAY,
    fetch_screener_json_api,
    fetch_screener_via_browser_use,
    fetch_screener_via_http,
    screener_company_slug,
)

KEY_RATIO_EXPECTED_COLUMNS = [
    ("market_cap", ("mar cap", "market cap")),
    ("price_to_earnings", ("p/e", "pe")),
    ("roe", ("roe",)),
    ("roce", ("roce",)),
    ("debt", ("debt", "d/e")),
]

QUARTERLY_EXPECTED_COLUMNS = [
    ("sales", ("sales", "revenue", "sales +")),
    ("profit", ("profit", "pat", "net profit", "net profit +", "profit +")),
    ("eps", ("eps", "eps +")),
]

SHAREHOLDING_EXPECTED_VALUES = [
    ("promoter", ("promoter",)),
    ("fii", ("fii", "foreign")),
    ("dii", ("dii", "domestic")),
]

SEVERITY_RANK = {"PASS": 0, "WARN": 1, "FAIL": 2}


def _normalize_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def _section(payload: dict[str, Any], key: str) -> Any:
    value = payload.get(key)
    return value if isinstance(value, (dict, list)) else {}


def _section_dict(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def _rows(section: Any) -> list[Any]:
    if isinstance(section, dict):
        value = section.get("rows")
        return value if isinstance(value, list) else []
    return []


def _columns(section: Any) -> list[Any]:
    if isinstance(section, dict):
        value = section.get("columns")
        return value if isinstance(value, list) else []
    return []


def _status_print(status: str, label: str, detail: str) -> None:
    print(f"{status:<5} {label}: {detail}")


def _match_column(columns: list[Any], patterns: tuple[str, ...]) -> str | None:
    normalized_columns = [str(column) for column in columns]
    for column in normalized_columns:
        normalized_column = _normalize_text(column)
        if any(_normalize_text(pattern) in normalized_column for pattern in patterns):
            return column
    return None


def _row_values(section: Any) -> list[str]:
    rows = _rows(section)
    values: list[str] = []
    for row in rows:
        if isinstance(row, dict):
            values.extend(str(value) for value in row.values())
        elif isinstance(row, list):
            values.extend(str(value) for value in row)
        else:
            values.append(str(row))
    return values


def _first_column_values(section: Any) -> list[str]:
    rows = _rows(section)
    values: list[str] = []
    for row in rows:
        if isinstance(row, dict):
            first_value = next(iter(row.values()), None)
            if first_value is not None:
                values.append(str(first_value))
        elif isinstance(row, list) and row:
            values.append(str(row[0]))
        else:
            values.append(str(row))
    return values


def _find_value(values: list[str], patterns: tuple[str, ...]) -> str | None:
    for value in values:
        normalized_value = _normalize_text(value)
        if any(_normalize_text(pattern) in normalized_value for pattern in patterns):
            return value
    return None


def _fetch_payload_with_fallbacks(ticker: str) -> tuple[dict[str, Any], str, list[str]]:
    notes: list[str] = []

    if SCREENER_COOKIE:
        try:
            payload = fetch_screener_json_api(ticker)
            return payload, "screener-json-api", notes
        except RuntimeError as exc:
            message = str(exc)
            if message == "Screener session expired":
                notes.append(f"JSON API session expired: {message}")
            elif message == "SCREENER_SESSION_ID not set":
                notes.append("SCREENER_SESSION_ID not set; skipping JSON API")
            else:
                notes.append(f"JSON API failed: {message}")
        except Exception as exc:
            notes.append(f"JSON API failed: {exc}")
    else:
        notes.append("SCREENER_SESSION_ID not set; skipping JSON API")

    try:
        payload = asyncio.run(fetch_screener_via_browser_use(ticker, show_live=False))
        return payload, "browser-use-mcp", notes
    except Exception as exc:
        notes.append(f"Browser Use MCP failed: {exc}")

    try:
        payload = fetch_screener_via_http(ticker)
        return payload, "fallback-http", notes
    except Exception as exc:
        notes.append(f"HTTP fallback failed: {exc}")

    slug = screener_company_slug(ticker)
    return (
        {
            "ticker": ticker,
            "updated": TODAY,
            "source": "unavailable",
            "company_url": f"https://www.screener.in/company/{slug}/",
            "key_ratios": {"columns": [], "rows": []},
            "quarterly_results": {"columns": [], "rows": []},
            "shareholding_pattern": {"columns": [], "rows": []},
            "balance_sheet": {},
            "cash_flows": {},
            "peers": [],
            "error": "all sources failed",
        },
        "unavailable",
        notes,
    )


def _overall_status(statuses: list[str]) -> str:
    rank = max(SEVERITY_RANK.get(status, 0) for status in statuses) if statuses else 0
    for label, value in SEVERITY_RANK.items():
        if value == rank:
            return label
    return "PASS"


def _evaluate_key_ratios(payload: dict[str, Any]) -> str:
    section = _section_dict(payload, "key_ratios")
    rows = _rows(section)
    columns = _columns(section)
    top_ratios = section.get("top_ratios") if isinstance(section, dict) else []
    statuses: list[str] = []

    statuses.append("PASS" if rows else "FAIL")
    statuses.append("PASS" if columns else "FAIL")
    if 0 < len(rows) < 5:
        statuses.append("WARN")

    row_values = _row_values(section)
    top_ratio_names: list[str] = []
    if isinstance(top_ratios, list):
        for item in top_ratios:
            if isinstance(item, dict):
                name = item.get("name")
                if name is not None:
                    top_ratio_names.append(str(name))

    for _, patterns in KEY_RATIO_EXPECTED_COLUMNS:
        matched = _match_column(columns, patterns)
        if not matched:
            matched = _find_value(row_values, patterns)
        if not matched:
            matched = _find_value(top_ratio_names, patterns)
        if not matched:
            statuses.append("FAIL")

    return _overall_status(statuses)


def _evaluate_quarterly_results(payload: dict[str, Any]) -> str:
    section = _section_dict(payload, "quarterly_results")
    rows = _rows(section)
    columns = _columns(section)
    first_column_values = _first_column_values(section)
    statuses: list[str] = []

    statuses.append("PASS" if rows else "FAIL")
    statuses.append("PASS" if columns else "FAIL")
    if 0 < len(rows) < 4:
        statuses.append("WARN")

    for _, patterns in QUARTERLY_EXPECTED_COLUMNS:
        matched = _match_column(columns, patterns)
        if not matched:
            matched = _find_value(first_column_values, patterns)
        if not matched:
            statuses.append("FAIL")

    return _overall_status(statuses)


def _evaluate_shareholding_pattern(payload: dict[str, Any]) -> str:
    section = _section_dict(payload, "shareholding_pattern")
    rows = _rows(section)
    row_values = _row_values(section)
    statuses: list[str] = ["PASS" if rows else "FAIL"]

    for _, patterns in SHAREHOLDING_EXPECTED_VALUES:
        if not _find_value(row_values, patterns):
            statuses.append("FAIL")

    return _overall_status(statuses)


def _evaluate_balance_sheet(payload: dict[str, Any]) -> str:
    value = payload.get("balance_sheet")
    if isinstance(value, dict):
        return "PASS" if value else "WARN"
    if isinstance(value, list):
        return "PASS" if value else "WARN"
    return "WARN"


def _evaluate_cash_flows(payload: dict[str, Any]) -> str:
    value = payload.get("cash_flows")
    if isinstance(value, dict):
        return "PASS" if value else "WARN"
    if isinstance(value, list):
        return "PASS" if value else "WARN"
    return "WARN"


def evaluate_payload_structure(payload: dict[str, Any]) -> dict[str, Any]:
    """Return non-printing structural validation for screener payloads."""
    checks = {
        "key_ratios": _evaluate_key_ratios(payload),
        "quarterly_results": _evaluate_quarterly_results(payload),
        "shareholding_pattern": _evaluate_shareholding_pattern(payload),
        "balance_sheet": _evaluate_balance_sheet(payload),
        "cash_flows": _evaluate_cash_flows(payload),
    }
    overall = _overall_status(list(checks.values()))
    return {"overall": overall, "checks": checks}


def _load_tickers_from_watchlist(watchlist_path: Path) -> list[str]:
    try:
        raw_text = watchlist_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise FileNotFoundError(watchlist_path) from exc

    try:
        watchlist = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{watchlist_path} is not valid JSON: {exc.msg}") from exc

    if not isinstance(watchlist, list) or not watchlist:
        raise ValueError(f"{watchlist_path} must be a non-empty JSON array of tickers")

    tickers = [str(item).strip().upper() for item in watchlist if str(item).strip()]
    if not tickers:
        raise ValueError(f"{watchlist_path} does not contain any tickers")
    return tickers


def _check_key_ratios(payload: dict[str, Any]) -> str:
    section = _section_dict(payload, "key_ratios")
    rows = _rows(section)
    columns = _columns(section)
    top_ratios = section.get("top_ratios") if isinstance(section, dict) else []
    statuses: list[str] = []

    if rows:
        _status_print("PASS", "key_ratios.rows", f"{len(rows)} rows")
        statuses.append("PASS")
    else:
        _status_print("FAIL", "key_ratios.rows", "rows list is empty")
        statuses.append("FAIL")

    if columns:
        _status_print("PASS", "key_ratios.columns", f"{len(columns)} columns")
        statuses.append("PASS")
    else:
        _status_print("FAIL", "key_ratios.columns", "columns list is empty")
        statuses.append("FAIL")

    if 0 < len(rows) < 5:
        _status_print("WARN", "key_ratios.row_count", f"{len(rows)} rows (likely incomplete)")
        statuses.append("WARN")

    row_values = _row_values(section)
    top_ratio_names: list[str] = []
    if isinstance(top_ratios, list):
        for item in top_ratios:
            if isinstance(item, dict):
                name = item.get("name")
                if name is not None:
                    top_ratio_names.append(str(name))

    print("  expected columns:")
    for label, patterns in KEY_RATIO_EXPECTED_COLUMNS:
        matched = _match_column(columns, patterns)
        if not matched:
            matched = _find_value(row_values, patterns)
        if not matched:
            matched = _find_value(top_ratio_names, patterns)
        if matched:
            print(f"    FOUND  {label}: matched '{matched}'")
        else:
            print(f"    MISSING {label}: expected one of {', '.join(patterns)}")
            statuses.append("FAIL")

    return _overall_status(statuses)


def _check_quarterly_results(payload: dict[str, Any]) -> str:
    section = _section_dict(payload, "quarterly_results")
    rows = _rows(section)
    columns = _columns(section)
    first_column_values = _first_column_values(section)
    statuses: list[str] = []

    if rows:
        _status_print("PASS", "quarterly_results.rows", f"{len(rows)} rows")
        statuses.append("PASS")
    else:
        _status_print("FAIL", "quarterly_results.rows", "rows list is empty")
        statuses.append("FAIL")

    if columns:
        _status_print("PASS", "quarterly_results.columns", f"{len(columns)} columns")
        statuses.append("PASS")
    else:
        _status_print("FAIL", "quarterly_results.columns", "columns list is empty")
        statuses.append("FAIL")

    if 0 < len(rows) < 4:
        _status_print("WARN", "quarterly_results.row_count", f"{len(rows)} rows (need at least 4 quarters)")
        statuses.append("WARN")

    print("  expected columns:")
    for label, patterns in QUARTERLY_EXPECTED_COLUMNS:
        matched = _match_column(columns, patterns)
        if not matched:
            matched = _find_value(first_column_values, patterns)
        if matched:
            print(f"    FOUND  {label}: matched '{matched}'")
        else:
            print(f"    MISSING {label}: expected one of {', '.join(patterns)}")
            statuses.append("FAIL")

    return _overall_status(statuses)


def _check_shareholding_pattern(payload: dict[str, Any]) -> str:
    section = _section_dict(payload, "shareholding_pattern")
    rows = _rows(section)
    statuses: list[str] = []

    if rows:
        _status_print("PASS", "shareholding_pattern.rows", f"{len(rows)} rows")
        statuses.append("PASS")
    else:
        _status_print("FAIL", "shareholding_pattern.rows", "rows list is empty")
        statuses.append("FAIL")

    row_values = _row_values(section)
    print("  expected row values:")
    for label, patterns in SHAREHOLDING_EXPECTED_VALUES:
        matched = _find_value(row_values, patterns)
        if matched:
            print(f"    FOUND  {label}: matched '{matched}'")
        else:
            print(f"    MISSING {label}: expected one of {', '.join(patterns)}")
            statuses.append("FAIL")

    return _overall_status(statuses)


def _check_balance_sheet(payload: dict[str, Any]) -> str:
    value = payload.get("balance_sheet")
    if isinstance(value, dict):
        if value:
            _status_print("PASS", "balance_sheet", f"dict with {len(value)} keys")
            return "PASS"
        _status_print("WARN", "balance_sheet", "empty dict")
        return "WARN"
    if isinstance(value, list):
        if value:
            _status_print("PASS", "balance_sheet", f"list with {len(value)} items")
            return "PASS"
        _status_print("WARN", "balance_sheet", "empty list")
        return "WARN"
    if value is None:
        _status_print("WARN", "balance_sheet", "missing")
        return "WARN"
    _status_print("WARN", "balance_sheet", f"unsupported type {type(value).__name__}")
    return "WARN"


def _check_cash_flows(payload: dict[str, Any]) -> str:
    value = payload.get("cash_flows")
    if isinstance(value, dict):
        if value:
            _status_print("PASS", "cash_flows", f"dict with {len(value)} keys")
            return "PASS"
        _status_print("WARN", "cash_flows", "empty dict")
        return "WARN"
    if isinstance(value, list):
        if value:
            _status_print("PASS", "cash_flows", f"list with {len(value)} items")
            return "PASS"
        _status_print("WARN", "cash_flows", "empty list")
        return "WARN"
    if value is None:
        _status_print("WARN", "cash_flows", "missing")
        return "WARN"
    _status_print("WARN", "cash_flows", f"unsupported type {type(value).__name__}")
    return "WARN"


def verify_ticker(ticker: str) -> None:
    slug = screener_company_slug(ticker)
    payload, source, notes = _fetch_payload_with_fallbacks(ticker)
    company_url = payload.get("company_url") or f"https://www.screener.in/company/{slug}/consolidated/"

    print("=" * 78)
    print(f"Ticker: {ticker}")
    print(f"Slug: {slug}")
    print(f"Date: {TODAY}")
    print(f"Source: {source}")
    print(f"Company URL: {company_url}")
    print(f"SCREENER_SESSION_ID: {'set' if SCREENER_COOKIE else 'not set'}")
    if notes:
        print("Fallback trail:")
        for note in notes:
            print(f"  - {note}")
    print("")

    overall_checks = [
        _check_key_ratios(payload),
        _check_quarterly_results(payload),
        _check_shareholding_pattern(payload),
        _check_balance_sheet(payload),
        _check_cash_flows(payload),
    ]
    overall = _overall_status(overall_checks)
    print("")
    print(f"OVERALL: {overall}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify Screener.in payloads independently of the full pipeline."
    )
    parser.add_argument(
        "tickers",
        nargs="*",
        help="One or more tickers to verify, e.g. DIXON.NS. Omit to use watchlist.json.",
    )
    parser.add_argument(
        "--watchlist",
        default=str(WATCHLIST_FILE),
        help="Path to a JSON array of tickers used when no positional tickers are provided.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    tickers = [str(ticker).strip().upper() for ticker in args.tickers if str(ticker).strip()]
    if not tickers:
        try:
            tickers = _load_tickers_from_watchlist(Path(args.watchlist))
        except (OSError, ValueError) as exc:
            parser.error(str(exc))

    for index, ticker in enumerate(tickers):
        if index:
            print("")
        verify_ticker(ticker)


if __name__ == "__main__":
    main()
