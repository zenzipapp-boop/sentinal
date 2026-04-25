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

import pytesseract
from PIL import Image
from pypdf import PdfReader

import fitz

from market_pipeline import extract_json, ollama_generate, screener_company_slug

BASE_DIR = Path(__file__).parent.resolve()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("annual-report")
SKIP_LLM_PLACEHOLDER = "raw text extracted"

# OCR availability check
OCR_AVAILABLE = True
try:
    pytesseract.get_tesseract_version()
except Exception:
    log.warning("Tesseract not installed. OCR fallback disabled. Install with: sudo apt install tesseract-ocr")
    OCR_AVAILABLE = False

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






def extract_page_with_ocr(pdf_path: Path, page_num: int) -> str:
    """Extract text from PDF page using pytesseract OCR at 300 DPI.

    Returns extracted text or empty string if OCR fails.
    """
    if not OCR_AVAILABLE:
        return ""

    try:
        doc = fitz.open(str(pdf_path))
        page = doc[page_num]
        # Render at 300 DPI for good OCR quality
        mat = fitz.Matrix(300 / 72, 300 / 72)
        pix = page.get_pixmap(matrix=mat)
        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        # pytesseract with English, PSM 6 for single-column text
        text = pytesseract.image_to_string(img, lang="eng", config="--psm 6")
        doc.close()
        return text.strip()
    except Exception as exc:
        log.warning("OCR failed on page %d: %s", page_num + 1, exc)
        return ""


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


def extract_pdf_pages(pdf_path: Path) -> list[dict[str, Any]]:
    """Extract text from every PDF page. Use OCR fallback for sparse pages.

    Returns list of page dicts with 'page', 'text', and 'extraction_method' keys.
    """
    reader = PdfReader(str(pdf_path))
    if reader.is_encrypted:
        try:
            reader.decrypt("")
        except Exception:
            pass

    all_pages: list[dict[str, Any]] = []

    for page_number, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception:
            try:
                text = page.extract_text(extraction_mode="layout") or ""
            except Exception:
                text = ""

        text = re.sub(r"\s+", " ", text.replace("\x00", " ")).strip()
        extraction_method = "pdfplumber"

        # If pdfplumber extracted less than 50 chars, try OCR fallback
        if len(text) < 50:
            ocr_text = extract_page_with_ocr(pdf_path, page_number - 1)
            if len(ocr_text) >= 50:
                text = ocr_text
                extraction_method = "pytesseract/OCR"

        page_data = {
            "page": page_number,
            "text": text,
            "extraction_method": extraction_method,
        }
        all_pages.append(page_data)

    return all_pages


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
            effective_ctx = num_ctx if num_ctx else 131072
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
    # Parse fiscal year from report_name (e.g., "fy20" = April 2019 to March 2020)
    fy_num = report_name[2:] if report_name.lower().startswith('fy') else report_name
    fy_clarification = f"April 20{int(fy_num)-1} to March 20{fy_num}" if fy_num.isdigit() and int(fy_num) > 0 else "unspecified period"

    prompt = f"""You are a Forensic Equity Auditor extracting structured notes from an annual report.

*** FISCAL YEAR: {report_name.upper()} ({fy_clarification}) ***

Company: {ticker}
Report: {report_name}
Pages: {chunk['page_start']} to {chunk['page_end']}

IMPORTANT: The fiscal year label (e.g., {report_name.upper()}) does NOT refer to a calendar year. {report_name.upper()} means the financial period ending in March 20{fy_num}. Do not confuse the fiscal year number with calendar years mentioned in the document text.

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
    # Parse fiscal year from report_name (e.g., "fy20" = April 2019 to March 2020)
    fy_num = report_name[2:] if report_name.lower().startswith('fy') else report_name
    fy_clarification = f"April 20{int(fy_num)-1} to March 20{fy_num}" if fy_num.isdigit() and int(fy_num) > 0 else "unspecified period"

    prior_section = ""
    if prior_baseline:
        prior_fy = int(fy_num) - 1 if fy_num.isdigit() else "N/A"
        if isinstance(prior_fy, int):
            prior_fy_text = f"FY{prior_fy}"
            prior_section = f"""
PRIOR YEAR FINANCIALS ({prior_fy_text} = April 20{prior_fy-1} to March 20{prior_fy}):
{json.dumps(prior_baseline, indent=2, ensure_ascii=False)}

Using these prior year numbers compute exact YoY changes for all key financial metrics. Express as absolute change and percentage change. Flag any metric that deteriorated significantly.
"""
        else:
            prior_section = f"""
PRIOR YEAR FINANCIALS (N/A — fiscal year could not be determined):
{json.dumps(prior_baseline, indent=2, ensure_ascii=False)}

Using these baseline numbers compute exact changes for key financial metrics where available.
"""

    prompt = f"""You are a Forensic Equity Auditor synthesizing annual-report chunks into a comprehensive investor thesis.

*** FISCAL YEAR: {report_name.upper()} ({fy_clarification}) ***

Company: {ticker}
Report: {report_name}

IMPORTANT: The fiscal year label (e.g., {report_name.upper()}) does NOT refer to a calendar year. {report_name.upper()} means the financial period ending in March 20{fy_num}. Do not confuse the fiscal year number with any year numbers mentioned in the document text (e.g., "2020" or "2019" in text refers to calendar years, not fiscal years).
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
    first_pdf_pages: list[dict[str, Any]] | None = None

    for i, pdf_path in enumerate(sorted(pdf_paths)):
        pages = extract_pdf_pages(pdf_path)
        if i == 0:
            first_pdf_pages = pages
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

    pages = first_pdf_pages if first_pdf_pages else []
    ocr_count = sum(1 for p in pages if p.get("extraction_method") == "pytesseract/OCR")
    pdf_names = ", ".join(p.name for p in pdf_paths)
    log.info("Processing %s %s: %s", ticker, year, pdf_names)
    log.info("Extracted %d pages, %d used OCR fallback", len(pages), ocr_count)
    log.info("Saved raw text: %s", text_path)
    chunks = chunk_pages(pages)
    summary_payload: dict[str, Any]
    model_used = "none"

    # Load prior year baseline for YoY comparison
    baseline_path = ticker_dir / "baselines.json"
    baselines = load_baseline(baseline_path)
    prior_baseline = None
    if not skip_llm and year.lower().startswith('fy') and len(year) > 2:
        try:
            year_num = int(year[2:])
            prior_year = f"fy{year_num - 1}"
            prior_baseline = baselines.get(prior_year, {})
        except (ValueError, IndexError):
            prior_baseline = None

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


def discover_tickers(input_root: Path) -> list[str]:
    """Return list of ticker folder names found in input_root (actual case on disk)."""
    if not input_root.is_dir():
        return []
    return sorted(d.name for d in input_root.iterdir() if d.is_dir())


def list_tickers(input_root: Path, output_root: Path) -> None:
    """Print all ticker folders with year counts and processed status."""
    tickers = discover_tickers(input_root)
    if not tickers:
        print(f"No ticker folders found in {input_root}")
        return

    print(f"{'Ticker':<20} {'Years':<8} {'Processed'}")
    print("-" * 50)
    for folder_name in tickers:
        source_dir = input_root / folder_name
        fy_dirs = [d for d in source_dir.iterdir() if d.is_dir() and d.name.lower().startswith("fy")] if source_dir.is_dir() else []
        year_count = len(fy_dirs)
        processed_dir = output_root / folder_name.upper()
        processed = "yes" if processed_dir.exists() else "no"
        print(f"{folder_name.upper():<20} {year_count:<8} {processed}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract and summarize annual reports into saved artifacts.")
    parser.add_argument("--input-dir", default=str(BASE_DIR / "annual_reports" / "raw"), help="Folder that contains annual report PDFs.")
    parser.add_argument("--output-dir", default=str(BASE_DIR / "annual_reports" / "processed"), help="Folder where extracted text and summaries are saved.")
    parser.add_argument("--ticker", default=None, help="Ticker(s) to process. Comma-separated for multiple. If omitted, all tickers in input-dir are processed.")
    parser.add_argument("--list", action="store_true", help="List all ticker folders found in input-dir and exit.")
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

    if args.list:
        list_tickers(input_root, output_root)
        return 0

    # Resolve tickers to process
    if args.ticker:
        requested_tickers = [t.strip() for t in args.ticker.split(",")]
    else:
        discovered = discover_tickers(input_root)
        if not discovered:
            log.error("No ticker folders found in %s", input_root)
            return 1
        requested_tickers = discovered
        log.info("Discovered %d tickers: %s", len(requested_tickers), ", ".join(requested_tickers))

    # Process each ticker
    for ticker_input in requested_tickers:
        source_dir = find_source_dir(input_root, ticker_input)
        ticker_upper = ticker_input.upper()
        year_groups = discover_reports(source_dir)
        if not year_groups:
            log.warning("[%s] No PDFs found under %s — skipping", ticker_upper, source_dir)
            continue

        log.info("[%s] Using source directory: %s", ticker_upper, source_dir)
        log.info("[%s] Found %d year groups", ticker_upper, len(year_groups))

        artifacts: list[ReportArtifact] = []
        for year, pdf_paths in year_groups:
            pdf_names = ", ".join(p.name for p in pdf_paths)
            log.info("[%s] Processing %s: %s", ticker_upper, year, pdf_names)
            artifact = process_report(pdf_paths, ticker_upper, year, input_root, output_root, args.models, skip_llm)
            artifacts.append(artifact)
            log.info(
                "[%s] Saved %s (%d pages, %d chars, model=%s)",
                ticker_upper,
                artifact.summary_path.name,
                artifact.page_count,
                artifact.extracted_chars,
                artifact.model_used,
            )

        build_index(ticker_upper, artifacts, output_root, args.models, skip_llm)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
