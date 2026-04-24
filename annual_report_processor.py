#!/usr/bin/env python3
"""Extract and summarize annual reports into saved text and JSON artifacts."""

from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from pypdf import PdfReader

from market_pipeline import extract_json, ollama_generate, screener_company_slug

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("annual-report")
SKIP_LLM_PLACEHOLDER = "raw text extracted"


@dataclass
class ReportArtifact:
    pdf_path: Path
    text_path: Path
    summary_path: Path
    page_count: int
    extracted_chars: int
    model_used: str


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
        "financial_trend": {"type": "string"},
        "capex_and_growth": {"type": "array", "items": {"type": "string"}},
        "risks": {"type": "array", "items": {"type": "string"}},
        "thesis_implications": {"type": "array", "items": {"type": "string"}},
        "management_tone": {"type": "string"},
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
            return candidate

    for child in root.iterdir():
        if not child.is_dir():
            continue
        label = normalize_label(child.name)
        if label in candidates or any(label in wanted or wanted in label for wanted in candidates):
            return child

    return root


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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
        pages.append({"page": page_number, "text": text})
    return pages


def chunk_pages(pages: list[dict[str, Any]], max_chars: int = 12000) -> list[dict[str, Any]]:
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


def generate_with_fallback(prompt: str, models: list[str], schema: dict[str, Any], num_predict: int) -> tuple[dict[str, Any], str, str]:
    last_error = ""
    for model in models:
        try:
            raw = ollama_generate(model, prompt, num_predict=num_predict, format_schema=schema)
            parsed = safe_json_load(raw)
            if not parsed:
                parsed = {"summary_text": raw.strip()}
            return parsed, raw, model
        except Exception as exc:
            last_error = f"{model}: {exc}"
            log.warning("Model failed for prompt: %s", last_error)
    raise RuntimeError(last_error or "No Ollama model could summarize the report")


def summarize_chunk(ticker: str, report_name: str, chunk: dict[str, Any], models: list[str]) -> dict[str, Any]:
    prompt = f"""You are extracting structured notes from an annual report for a long-term equity analyst.

Company: {ticker}
Report: {report_name}
Pages: {chunk['page_start']} to {chunk['page_end']}

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


def summarize_report(ticker: str, report_name: str, chunk_summaries: list[dict[str, Any]], models: list[str]) -> dict[str, Any]:
    prompt = f"""You are synthesizing the annual-report chunks of a single company into a compact investor note.

Company: {ticker}
Report: {report_name}

Chunk summaries:
{json.dumps(chunk_summaries, indent=2, ensure_ascii=False)}

Return valid JSON only:
{{
  "report_title": "short report title",
  "one_line_summary": "one sentence",
  "main_points": ["point 1", "point 2"],
  "financial_trend": "short description of revenue/margin/cash flow direction",
  "capex_and_growth": ["capex or growth note"],
  "risks": ["risk 1", "risk 2"],
  "thesis_implications": ["implication 1", "implication 2"],
  "management_tone": "bullish/cautious/mixed and why",
  "confidence_score": 1-10
}}"""

    parsed, raw, model_used = generate_with_fallback(prompt, models, REPORT_SUMMARY_SCHEMA, num_predict=900)
    parsed["raw"] = raw
    parsed["model"] = model_used
    parsed["report_name"] = report_name
    return parsed


def summarize_index(ticker: str, report_summaries: list[dict[str, Any]], models: list[str]) -> dict[str, Any]:
    prompt = f"""You are combining multiple annual-report summaries into a compact context block for a stock analyst.

Ticker: {ticker}

Report summaries:
{json.dumps(report_summaries, indent=2, ensure_ascii=False)}

Return valid JSON only:
{{
  "ticker": "{ticker}",
  "portfolio_summary": "2-4 sentence synthesis of the latest annual-report story",
  "main_trends": ["trend 1", "trend 2"],
  "repeated_themes": ["theme 1", "theme 2"],
  "key_risks": ["risk 1", "risk 2"],
  "thesis_implications": ["implication 1", "implication 2"],
  "management_tone": "bullish/cautious/mixed and why",
  "confidence_score": 1-10
}}"""

    parsed, raw, model_used = generate_with_fallback(prompt, models, INDEX_SUMMARY_SCHEMA, num_predict=900)
    parsed["raw"] = raw
    parsed["model"] = model_used
    return parsed


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def process_report(pdf_path: Path, ticker: str, raw_root: Path, output_root: Path, models: list[str], skip_llm: bool) -> ReportArtifact:
    pages = read_pdf_pages(pdf_path)
    extracted_text = "\n\n".join(
        f"[Page {page['page']}] {page['text']}" for page in pages if str(page.get("text", "")).strip()
    )
    report_stem = pdf_path.stem
    ticker_dir = output_root / ticker.upper()
    raw_dir = ticker_dir / "raw"
    summary_dir = ticker_dir / "summaries"
    ensure_dir(raw_dir)
    ensure_dir(summary_dir)

    text_path = raw_dir / f"{report_stem}.txt"
    text_path.write_text(extracted_text, encoding="utf-8")

    chunks = chunk_pages(pages)
    summary_payload: dict[str, Any]
    model_used = "none"

    if skip_llm:
        summary_payload = {
            "report_title": report_stem,
            "one_line_summary": f"LLM summarization skipped; {SKIP_LLM_PLACEHOLDER} only.",
            "main_points": [],
            "financial_trend": "Unavailable",
            "capex_and_growth": [],
            "risks": [],
            "thesis_implications": [],
            "management_tone": "Unavailable",
            "confidence_score": 0,
            "source": pdf_path.name,
        }
    else:
        chunk_summaries = [summarize_chunk(ticker, pdf_path.name, chunk, models) for chunk in chunks or [{"page_start": 1, "page_end": 1, "text": extracted_text[:20000]}]]
        summary_payload = summarize_report(ticker, pdf_path.name, chunk_summaries, models)
        summary_payload["chunk_summaries"] = chunk_summaries
        model_used = str(summary_payload.get("model", models[0] if models else "unknown"))

    summary_payload.update(
        {
            "source": pdf_path.name,
            "pdf_path": str(pdf_path),
            "page_count": len(pages),
            "extracted_chars": len(extracted_text),
            "extraction_quality": "good" if len(extracted_text) > 2000 else "limited",
            "processed_at": datetime.now().isoformat(timespec="seconds"),
            "model_used": model_used,
        }
    )

    summary_path = summary_dir / f"{report_stem}.json"
    write_json(summary_path, summary_payload)

    return ReportArtifact(
        pdf_path=pdf_path,
        text_path=text_path,
        summary_path=summary_path,
        page_count=len(pages),
        extracted_chars=len(extracted_text),
        model_used=model_used,
    )


def build_index(ticker: str, artifacts: list[ReportArtifact], output_root: Path, models: list[str], skip_llm: bool) -> Path:
    ticker_dir = output_root / ticker.upper()
    summary_dir = ticker_dir / "summaries"
    index_path = ticker_dir / "index.json"

    report_summaries: list[dict[str, Any]] = []
    for artifact in sorted(artifacts, key=lambda item: item.pdf_path.name):
        payload = json.loads(artifact.summary_path.read_text(encoding="utf-8"))
        report_summaries.append(
            {
                "report_title": payload.get("report_title", artifact.pdf_path.stem),
                "one_line_summary": payload.get("one_line_summary", ""),
                "main_points": payload.get("main_points", []),
                "financial_trend": payload.get("financial_trend", ""),
                "capex_and_growth": payload.get("capex_and_growth", []),
                "risks": payload.get("risks", []),
                "thesis_implications": payload.get("thesis_implications", []),
                "management_tone": payload.get("management_tone", ""),
                "confidence_score": payload.get("confidence_score", 0),
                "source": artifact.pdf_path.name,
                "summary_file": artifact.summary_path.name,
            }
        )

    if not skip_llm and report_summaries:
        combined_summary = summarize_index(ticker, report_summaries, models)
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
        "report_count": len(report_summaries),
        "reports": report_summaries,
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


def discover_reports(source_dir: Path) -> list[Path]:
    if source_dir.is_file() and source_dir.suffix.lower() == ".pdf":
        return [source_dir]
    if source_dir.is_dir():
        return sorted(source_dir.rglob("*.pdf"))
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract and summarize annual reports into saved artifacts.")
    parser.add_argument("--input-dir", default="annual_reports", help="Folder that contains annual report PDFs.")
    parser.add_argument("--output-dir", default="annual_reports/processed", help="Folder where extracted text and summaries are saved.")
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
    pdfs = discover_reports(source_dir)
    if not pdfs:
        log.error("No PDFs found under %s", source_dir)
        return 1

    log.info("Using source directory: %s", source_dir)
    log.info("Found %d annual reports", len(pdfs))

    artifacts: list[ReportArtifact] = []
    for pdf_path in pdfs:
        log.info("Processing %s", pdf_path.name)
        artifact = process_report(pdf_path, args.ticker, input_root, output_root, args.models, skip_llm)
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
