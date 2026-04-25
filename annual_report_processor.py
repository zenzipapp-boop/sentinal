#!/usr/bin/env python3
"""Extract and summarize annual reports into saved text and JSON artifacts."""

from __future__ import annotations

import argparse
import json
import logging
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from pypdf import PdfReader

from market_pipeline import extract_json, ollama_generate, screener_company_slug

BASE_DIR = Path(__file__).parent.resolve()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("annual-report")
SKIP_LLM_PLACEHOLDER = "raw text extracted"

# Bouncer: Skip pages that are noise (legal forms, proxy statements, etc.)
BLACKLISTED_PAGES = [
    "proxy form",
    "proxy statement",
    "attendance slip",
    "e-voting instructions",
    "ballot paper",
    "voting instruction form",
    "notice of annual general meeting",
    "agm notice",
    "shareholder notice",
    "consent form",
    "power of attorney",
    "resolution",
    "director certification",
]


@dataclass
class ReportArtifact:
    pdf_paths: list[Path]
    text_path: Path
    summary_path: Path
    page_count: int
    extracted_chars: int
    model_used: str
    year: str


CHUNK_SUMMARY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "chunk_summary": {"type": "string"},
        "main_points": {"type": "array", "items": {"type": "string"}},
        "financial_signals": {"type": "array", "items": {"type": "string"}},
        "capex_and_projects": {"type": "array", "items": {"type": "string"}},
        "risk_flags": {"type": "array", "items": {"type": "string"}},
        "thesis_implications": {"type": "array", "items": {"type": "string"}},
        "notable_numbers": {"type": "array", "items": {"type": "string"}},
        "confidence_score": {"type": "number"},
    },
    "required": [
        "chunk_summary",
        "main_points",
        "financial_signals",
        "capex_and_projects",
        "risk_flags",
        "thesis_implications",
        "notable_numbers",
        "confidence_score",
    ],
}

REPORT_SUMMARY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "report_title": {"type": "string"},
        "one_line_summary": {"type": "string"},
        "main_points": {"type": "array", "items": {"type": "string"}},
        "financial_summary": {
            "type": "object",
            "properties": {
                "revenue": {"type": "string"},
                "revenue_yoy": {"type": "string"},
                "net_profit": {"type": "string"},
                "net_profit_yoy": {"type": "string"},
                "ebitda_margin": {"type": "string"},
                "ebitda_margin_yoy": {"type": "string"},
                "roce": {"type": "string"},
                "roe": {"type": "string"},
                "debt_to_equity": {"type": "string"},
                "eps": {"type": "string"},
                "eps_yoy": {"type": "string"},
                "cash_from_operations": {"type": "string"},
                "capex": {"type": "string"},
                "free_cash_flow": {"type": "string"},
            },
        },
        "financial_trend": {"type": "string"},
        "capex_and_growth": {"type": "array", "items": {"type": "string"}},
        "risks": {"type": "array", "items": {"type": "string"}},
        "thesis_implications": {"type": "array", "items": {"type": "string"}},
        "management_tone": {"type": "string"},
        "management_commentary_highlights": {"type": "array", "items": {"type": "string"}},
        "agm_highlights": {"type": "array", "items": {"type": "string"}},
        "red_flags": {"type": "array", "items": {"type": "string"}},
        "yoy_highlights": {
            "type": "object",
            "properties": {
                "biggest_improvement": {"type": "string"},
                "biggest_deterioration": {"type": "string"},
                "margin_trend": {"type": "string"},
                "debt_trend": {"type": "string"},
                "overall_quality_score": {"type": "number"},
            },
        },
        "thesis_impact": {"type": "string"},
        "confidence_score": {"type": "number"},
    },
    "required": [
        "report_title",
        "one_line_summary",
        "main_points",
        "financial_trend",
        "capex_and_growth",
        "risks",
        "thesis_implications",
        "management_tone",
        "confidence_score",
    ],
}

INDEX_SUMMARY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "ticker": {"type": "string"},
        "portfolio_summary": {"type": "string"},
        "main_trends": {"type": "array", "items": {"type": "string"}},
        "repeated_themes": {"type": "array", "items": {"type": "string"}},
        "key_risks": {"type": "array", "items": {"type": "string"}},
        "thesis_implications": {"type": "array", "items": {"type": "string"}},
        "management_tone": {"type": "string"},
        "confidence_score": {"type": "number"},
    },
    "required": [
        "ticker",
        "portfolio_summary",
        "main_trends",
        "repeated_themes",
        "key_risks",
        "thesis_implications",
        "management_tone",
        "confidence_score",
    ],
}


def normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def find_source_dir(root: Path, ticker: str) -> Path:
    if root.is_file():
        return root.parent

    candidates = {
        normalize_label(ticker),
        normalize_label(screener_company_slug(ticker)),
    }

    direct_candidates = [
        root / ticker,
        root / ticker.upper(),
        root / ticker.lower(),
        root / screener_company_slug(ticker),
        root / screener_company_slug(ticker).lower(),
    ]
    for candidate in direct_candidates:
        if candidate.is_dir():
            log.info("Found ticker folder (direct match): %s", candidate)
            return candidate

    for child in root.iterdir():
        if not child.is_dir():
            continue
        label = normalize_label(child.name)
        if label in candidates or any(label in wanted or wanted in label for wanted in candidates):
            log.info("Found ticker folder (case-insensitive match): %s (normalized: %s)", child, label)
            return child

    log.warning("No matching ticker folder found in %s for ticker %s", root, ticker)
    return root


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON atomically to avoid corruption on failure."""
    ensure_dir(path.parent)
    with tempfile.NamedTemporaryFile(
        "w",
        delete=False,
        encoding="utf-8",
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    ) as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        temp_name = handle.name
    Path(temp_name).replace(path)


def load_baseline(path: Path) -> dict[str, dict[str, Any]]:
    """Load baseline data from JSON, return empty dict if not found."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Failed to load baseline from %s: %s", path, exc)
        return {}


def extract_numeric_baseline(summary: dict[str, Any]) -> dict[str, Any]:
    """Extract numeric values from annual report summary for storage in baselines."""
    baseline: dict[str, Any] = {}

    financial = summary.get("financial_summary", {})
    if isinstance(financial, dict):
        for key in ["revenue", "net_profit", "ebitda_margin", "roce", "roe", "debt_to_equity", "eps", "dividend_per_share", "cash_from_operations", "capex", "order_book", "employee_count"]:
            if key in financial:
                baseline[key] = str(financial[key])

    return baseline


def save_baseline(path: Path, year: str, baseline: dict[str, Any]) -> None:
    """Save baseline data for a year to baselines.json."""
    baselines = load_baseline(path)
    baselines[year] = baseline
    atomic_write_json(path, baselines)
    log.info("Saved baseline for %s to %s", year, path)


def read_pdf_pages(pdf_path: Path) -> list[dict[str, Any]]:
    reader = PdfReader(str(pdf_path))
    if reader.is_encrypted:
        try:
            reader.decrypt("")
        except Exception:
            pass

    pages: list[dict[str, Any]] = []
    for page_number, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception:
            try:
                text = page.extract_text(extraction_mode="layout") or ""
            except Exception:
                text = ""
        text = re.sub(r"\s+", " ", text.replace("\x00", " ")).strip()

        # Bouncer: Skip pages that match blacklisted keywords
        if any(keyword in text.lower() for keyword in BLACKLISTED_PAGES):
            log.info(f"Skipping Page {page_number} (Detected as Noise: {[k for k in BLACKLISTED_PAGES if k in text.lower()][0]})")
            continue

        pages.append({"page": page_number, "text": text})
    return pages


def chunk_pages(pages: list[dict[str, Any]], max_chars: int = 32000) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    current_pages: list[dict[str, Any]] = []
    current_chars = 0

    for page in pages:
        page_text = str(page.get("text", "")).strip()
        if not page_text:
            continue
        page_size = len(page_text)
        if current_pages and current_chars + page_size > max_chars:
            chunks.append(
                {
                    "page_start": int(current_pages[0]["page"]),
                    "page_end": int(current_pages[-1]["page"]),
                    "text": "\n\n".join(
                        f"[Page {item['page']}] {item['text']}" for item in current_pages
                    ),
                }
            )
            current_pages = []
            current_chars = 0

        current_pages.append(page)
        current_chars += page_size

    if current_pages:
        chunks.append(
            {
                "page_start": int(current_pages[0]["page"]),
                "page_end": int(current_pages[-1]["page"]),
                "text": "\n\n".join(f"[Page {item['page']}] {item['text']}" for item in current_pages),
            }
        )

    return chunks


def safe_json_load(text: str) -> dict[str, Any]:
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    except Exception:
        return extract_json(text)


def generate_with_fallback(
    prompt: str,
    models: list[str],
    schema: dict[str, Any],
    num_predict: int,
    num_ctx: int = 32768,
    temperature: float = 0.1,
) -> tuple[dict[str, Any], str, str]:
    last_error = ""
    for model in models:
        try:
            effective_ctx = 96000 if model == "gemma4:latest" else num_ctx
            raw = ollama_generate(
                model,
                prompt,
                num_predict=num_predict,
                format_schema=schema,
                num_ctx=effective_ctx,
                temperature=temperature,
            )
            parsed = safe_json_load(raw)
            if not parsed:
                parsed = {"summary_text": raw.strip()}
            return parsed, raw, model
        except Exception as exc:
            last_error = f"{model}: {exc}"
            log.warning("Model failed for prompt: %s", last_error)
    raise RuntimeError(last_error or "No Ollama model could summarize the report")


def summarize_chunk(ticker: str, report_name: str, chunk: dict[str, Any], models: list[str]) -> dict[str, Any]:
    prompt = f"""You are a Forensic Equity Auditor extracting structured notes from an annual report.

Company: {ticker}
Report: {report_name}
Pages: {chunk['page_start']} to {chunk['page_end']}

Do not just summarize; investigate. Look for gaps between management's promises and the Notes to Accounts. If a number looks unusual compared to prior context, flag it as a Discrepancy.

Focus on business changes, revenue growth, margins, capex, capacity, working capital, debt, guidance, governance, litigation, and any risks or surprises. Ignore boilerplate and repeated legal text.

TEXT:
{chunk['text']}

Return valid JSON only:
{{
  "chunk_summary": "2-4 sentence summary",
  "main_points": ["point 1", "point 2"],
  "financial_signals": ["signal 1", "signal 2"],
  "capex_and_projects": ["capex or project note"],
  "risk_flags": ["risk 1", "risk 2"],
  "thesis_implications": ["what this means for the investment thesis"],
  "notable_numbers": ["key number 1", "key number 2"],
  "confidence_score": 1-10
}}"""

    parsed, raw, model_used = generate_with_fallback(prompt, models, CHUNK_SUMMARY_SCHEMA, num_predict=900)
    parsed["raw"] = raw
    parsed["model"] = model_used
    parsed["page_start"] = chunk["page_start"]
    parsed["page_end"] = chunk["page_end"]
    return parsed


def summarize_report(ticker: str, report_name: str, chunk_summaries: list[dict[str, Any]], models: list[str], prior_baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    prior_section = ""
    if prior_baseline:
        prior_section = f"""
PRIOR YEAR FINANCIALS (FY{int(report_name[2:]) - 1 if report_name.startswith('FY') else 'N/A'}):
{json.dumps(prior_baseline, indent=2, ensure_ascii=False)}

Using these prior year numbers compute exact YoY changes for all key financial metrics. Express as absolute change and percentage change. Flag any metric that deteriorated significantly.
"""

    prompt = f"""You are a Forensic Equity Auditor synthesizing annual-report chunks into a comprehensive investor thesis.

Company: {ticker}
Report: {report_name}
{prior_section}

Investigate discrepancies between management claims and actual results. Cross-reference numbers across sections. Flag inconsistencies.

Extract structured financial metrics with YoY comparison where available. Identify key management commentary from MD&A or Chairman's letter. Extract AGM discussion points. Highlight red flags and thesis implications.

Chunk summaries:
{json.dumps(chunk_summaries, indent=2, ensure_ascii=False)}

Return valid JSON only:
{{
  "report_title": "short report title",
  "one_line_summary": "one sentence thesis or key finding",
  "main_points": ["point 1", "point 2"],
  "financial_summary": {{
    "revenue": "string",
    "revenue_yoy": "string — e.g. +18.2% or N/A",
    "net_profit": "string",
    "net_profit_yoy": "string",
    "ebitda_margin": "string",
    "ebitda_margin_yoy": "string — e.g. expanded 120bps or contracted 80bps",
    "roce": "string",
    "roe": "string",
    "debt_to_equity": "string",
    "eps": "string",
    "eps_yoy": "string",
    "cash_from_operations": "string",
    "capex": "string",
    "free_cash_flow": "string",
    "dividend_per_share": "string",
    "order_book": "string if applicable, else N/A",
    "employee_count": "string if mentioned, else N/A"
  }},
  "financial_trend": "detailed description of revenue/margin/cash flow direction and quality of earnings",
  "yoy_highlights": {{
    "biggest_improvement": "which metric improved most and by how much",
    "biggest_deterioration": "which metric worsened most and by how much",
    "margin_trend": "expanding/contracting/stable",
    "debt_trend": "increasing/decreasing/stable",
    "overall_quality_score": "1-10 where 10 = strongest YoY improvement"
  }},
  "capex_and_growth": ["capex or growth note with forensic implications"],
  "risks": ["material risk 1", "material risk 2"],
  "red_flags": ["any concerning trend or disclosure found in the annual report"],
  "thesis_implications": ["implication 1", "implication 2", "implication 3"],
  "management_tone": "bullish/cautious/mixed and credibility assessment",
  "management_commentary_highlights": ["key point 1 from MD&A or chairman letter", "key point 2", "key point 3"],
  "agm_highlights": ["key resolution or discussion point from AGM transcript if available", "key point 2"],
  "thesis_impact": "one paragraph on how this annual report strengthens or weakens a long-term investment thesis for this company",
  "confidence_score": 1-10
}}"""

    parsed, raw, model_used = generate_with_fallback(prompt, models, REPORT_SUMMARY_SCHEMA, num_predict=3500)
    parsed["raw"] = raw
    parsed["model"] = model_used
    parsed["report_name"] = report_name
    return parsed


def summarize_index(ticker: str, report_summaries: list[dict[str, Any]], models: list[str]) -> dict[str, Any]:
    prompt = f"""You are a Forensic Equity Auditor combining multiple annual-report summaries into a comprehensive investment thesis.

Ticker: {ticker}

Identify patterns, divergences, and red flags across reports. Flag if management tone is inconsistent or if risks are escalating.

Report summaries:
{json.dumps(report_summaries, indent=2, ensure_ascii=False)}

Return valid JSON only:
{{
  "ticker": "{ticker}",
  "portfolio_summary": "2-4 sentence synthesis of the latest annual-report story with thesis implications",
  "main_trends": ["trend 1", "trend 2"],
  "repeated_themes": ["theme 1", "theme 2"],
  "key_risks": ["material risk 1", "material risk 2"],
  "thesis_implications": ["implication 1", "implication 2"],
  "management_tone": "bullish/cautious/mixed and credibility trend",
  "confidence_score": 1-10
}}"""

    parsed, raw, model_used = generate_with_fallback(prompt, models, INDEX_SUMMARY_SCHEMA, num_predict=2048)
    parsed["raw"] = raw
    parsed["model"] = model_used
    return parsed


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def process_report(pdf_paths: list[Path], ticker: str, year: str, raw_root: Path, output_root: Path, models: list[str], skip_llm: bool) -> ReportArtifact:
    """Process one or more PDFs for a given year, with YoY baseline tracking."""
    extracted_text_parts: list[str] = []

    for pdf_path in sorted(pdf_paths):
        pages = read_pdf_pages(pdf_path)
        pdf_text = "\n\n".join(
            f"[Page {page['page']}] {page['text']}" for page in pages if str(page.get("text", "")).strip()
        )
        extracted_text_parts.append(f"=== SOURCE: {pdf_path.name} ===\n{pdf_text}")

    extracted_text = "\n\n".join(extracted_text_parts)

    ticker_dir = output_root / ticker.upper()
    raw_dir = ticker_dir / "raw"
    summary_dir = ticker_dir / "summaries"
    ensure_dir(raw_dir)
    ensure_dir(summary_dir)

    text_path = raw_dir / f"{year}.txt"
    text_path.write_text(extracted_text, encoding="utf-8")

    pages = read_pdf_pages(pdf_paths[0]) if pdf_paths else []
    chunks = chunk_pages(pages)
    summary_payload: dict[str, Any]
    model_used = "none"

    # Load prior year baseline for YoY comparison
    baseline_path = ticker_dir / "baselines.json"
    baselines = load_baseline(baseline_path)
    prior_year = f"fy{int(year[2:]) - 1}"
    prior_baseline = baselines.get(prior_year, {}) if not skip_llm else None

    if skip_llm:
        summary_payload = {
            "report_title": year,
            "one_line_summary": f"LLM summarization skipped; {SKIP_LLM_PLACEHOLDER} only.",
            "main_points": [],
            "financial_trend": "Unavailable",
            "capex_and_growth": [],
            "risks": [],
            "thesis_implications": [],
            "management_tone": "Unavailable",
            "confidence_score": 0,
            "source_files": [p.name for p in pdf_paths],
        }
    else:
        chunk_summaries = [summarize_chunk(ticker, year, chunk, models) for chunk in chunks or [{"page_start": 1, "page_end": 1, "text": extracted_text[:20000]}]]
        summary_payload = summarize_report(ticker, year, chunk_summaries, models, prior_baseline=prior_baseline)
        summary_payload["chunk_summaries"] = chunk_summaries
        model_used = str(summary_payload.get("model", models[0] if models else "unknown"))

        # Extract and save baseline for next year's processing
        extracted_baseline = extract_numeric_baseline(summary_payload)
        if extracted_baseline:
            save_baseline(baseline_path, year, extracted_baseline)

    summary_payload.update(
        {
            "source_files": [p.name for p in pdf_paths],
            "pdf_paths": [str(p) for p in pdf_paths],
            "page_count": len(pages),
            "extracted_chars": len(extracted_text),
            "extraction_quality": "good" if len(extracted_text) > 2000 else "limited",
            "processed_at": datetime.now().isoformat(timespec="seconds"),
            "model_used": model_used,
        }
    )

    summary_path = summary_dir / f"{year}.json"
    write_json(summary_path, summary_payload)

    return ReportArtifact(
        pdf_paths=pdf_paths,
        text_path=text_path,
        summary_path=summary_path,
        page_count=len(pages),
        extracted_chars=len(extracted_text),
        model_used=model_used,
        year=year,
    )


def build_index(ticker: str, artifacts: list[ReportArtifact], output_root: Path, models: list[str], skip_llm: bool) -> Path:
    ticker_dir = output_root / ticker.upper()
    summary_dir = ticker_dir / "summaries"
    index_path = ticker_dir / "index.json"

    report_summaries: list[dict[str, Any]] = []
    for artifact in sorted(artifacts, key=lambda item: item.year):
        payload = json.loads(artifact.summary_path.read_text(encoding="utf-8"))
        report_summaries.append(
            {
                "year": artifact.year,
                "report_title": payload.get("report_title", artifact.year),
                "one_line_summary": payload.get("one_line_summary", ""),
                "main_points": payload.get("main_points", []),
                "financial_trend": payload.get("financial_trend", ""),
                "capex_and_growth": payload.get("capex_and_growth", []),
                "risks": payload.get("risks", []),
                "thesis_implications": payload.get("thesis_implications", []),
                "management_tone": payload.get("management_tone", ""),
                "confidence_score": payload.get("confidence_score", 0),
                "source_files": payload.get("source_files", [p.name for p in artifact.pdf_paths]),
                "summary_file": artifact.summary_path.name,
            }
        )

    # Sort reports in descending order by year
    report_summaries_sorted = sorted(report_summaries, key=lambda x: x.get("year", ""), reverse=True)

    if not skip_llm and report_summaries_sorted:
        combined_summary = summarize_index(ticker, report_summaries_sorted, models)
    else:
        combined_summary = {
            "ticker": ticker,
            "portfolio_summary": "LLM index synthesis skipped; use individual report summaries.",
            "main_trends": [],
            "repeated_themes": [],
            "key_risks": [],
            "thesis_implications": [],
            "management_tone": "Unavailable",
            "confidence_score": 0,
        }

    payload = {
        "ticker": ticker,
        "source_dir": str(output_root),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "last_updated": datetime.now().isoformat(timespec="seconds"),
        "years": [r.get("year") for r in report_summaries_sorted],
        "report_count": len(report_summaries_sorted),
        "reports": report_summaries_sorted,
        "combined_summary": combined_summary,
        "summary_files": [str(artifact.summary_path) for artifact in artifacts],
        "text_files": [str(artifact.text_path) for artifact in artifacts],
        "models": models,
        "skip_llm": skip_llm,
    }
    ensure_dir(index_path.parent)
    write_json(index_path, payload)

    log.info("[%s] Wrote index with %d reports to %s", ticker, len(report_summaries), index_path)
    return index_path


def discover_reports(source_dir: Path) -> list[tuple[str, list[Path]]]:
    """Discover PDFs grouped by year folder (fy* case-insensitive) or single PDFs.
    Returns list of (year_label, pdf_paths) tuples."""
    reports: list[tuple[str, list[Path]]] = []

    if source_dir.is_file() and source_dir.suffix.lower() == ".pdf":
        # Single PDF at root level - backward compatibility
        return [("root", [source_dir])]

    if not source_dir.is_dir():
        return []

    # Look for fy* subdirectories (case-insensitive)
    fy_dirs = sorted([d for d in source_dir.iterdir() if d.is_dir() and d.name.lower().startswith("fy")], key=lambda d: d.name.lower())

    if fy_dirs:
        # Multi-file per year structure
        for fy_dir in fy_dirs:
            pdfs = sorted(fy_dir.glob("*.pdf"))
            if pdfs:
                reports.append((fy_dir.name, pdfs))
        return reports

    # Fallback: single PDF per folder (backward compatibility)
    pdfs = sorted(source_dir.glob("*.pdf"))
    if pdfs:
        return [("root", pdfs)]

    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract and summarize annual reports into saved artifacts.")
    parser.add_argument("--input-dir", default=str(BASE_DIR / "annual_reports" / "raw"), help="Folder that contains annual report PDFs.")
    parser.add_argument("--output-dir", default=str(BASE_DIR / "annual_reports" / "processed"), help="Folder where extracted text and summaries are saved.")
    parser.add_argument("--ticker", default="DIXON.NS", help="Ticker label used for the output folder.")
    parser.add_argument(
        "--models",
        nargs="+",
        default=["gemma4:latest", "gemma4:26b", "gpt-oss:20b"],
        help="Ordered list of Ollama models to try for summarization.",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Explicitly skip model summaries and only extract raw text.",
    )
    args = parser.parse_args()
    skip_llm = bool(args.skip_llm)

    input_root = Path(args.input_dir)
    output_root = Path(args.output_dir)

    source_dir = find_source_dir(input_root, args.ticker)
    year_groups = discover_reports(source_dir)
    if not year_groups:
        log.error("No PDFs found under %s", source_dir)
        return 1

    log.info("Using source directory: %s", source_dir)
    log.info("Found %d year groups", len(year_groups))

    artifacts: list[ReportArtifact] = []
    for year, pdf_paths in year_groups:
        pdf_names = ", ".join(p.name for p in pdf_paths)
        log.info("Processing %s: %s", year, pdf_names)
        artifact = process_report(pdf_paths, args.ticker, year, input_root, output_root, args.models, skip_llm)
        artifacts.append(artifact)
        log.info(
            "Saved %s (%d pages, %d chars, model=%s)",
            artifact.summary_path.name,
            artifact.page_count,
            artifact.extracted_chars,
            artifact.model_used,
        )

    build_index(args.ticker, artifacts, output_root, args.models, skip_llm)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
