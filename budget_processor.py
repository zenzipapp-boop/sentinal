#!/usr/bin/env python3
"""Extract and summarize government budgets into saved text and JSON artifacts."""

from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pypdf import PdfReader

from market_pipeline import extract_json, ollama_generate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("budget-processor")
SKIP_LLM_PLACEHOLDER = "raw text extracted"


@dataclass
class BudgetArtifact:
    pdf_paths: list[Path]
    text_path: Path
    summary_path: Path
    page_count: int
    extracted_chars: int
    model_used: str
    year: str


BUDGET_SUMMARY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "budget_title": {"type": "string"},
        "one_line_summary": {"type": "string"},
        "fiscal_stance": {"type": "string"},
        "fiscal_deficit_gdp": {"type": "string"},
        "capex_outlay": {"type": "string"},
        "key_themes": {"type": "array", "items": {"type": "string"}},
        "pli_schemes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "scheme_name": {"type": "string"},
                    "sector": {"type": "string"},
                    "allocation": {"type": "string"},
                    "beneficiary_companies": {"type": "array", "items": {"type": "string"}},
                    "notes": {"type": "string"},
                },
            },
        },
        "sector_allocations": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "sector": {"type": "string"},
                    "allocation": {"type": "string"},
                    "yoy_change": {"type": "string"},
                    "key_programs": {"type": "array", "items": {"type": "string"}},
                    "equity_impact": {"type": "string"},
                },
            },
        },
        "import_duty_changes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item": {"type": "string"},
                    "change": {"type": "string"},
                    "old_rate": {"type": "string"},
                    "new_rate": {"type": "string"},
                    "equity_impact": {"type": "string"},
                },
            },
        },
        "tax_changes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "description": {"type": "string"},
                    "equity_impact": {"type": "string"},
                },
            },
        },
        "infrastructure_push": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "allocation": {"type": "string"},
                    "key_projects": {"type": "array", "items": {"type": "string"}},
                    "equity_impact": {"type": "string"},
                },
            },
        },
        "sector_headwinds": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "sector": {"type": "string"},
                    "reason": {"type": "string"},
                    "equity_impact": {"type": "string"},
                },
            },
        },
        "sector_tailwinds": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "sector": {"type": "string"},
                    "reason": {"type": "string"},
                    "equity_impact": {"type": "string"},
                },
            },
        },
        "divestment_targets": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "company": {"type": "string"},
                    "target_amount": {"type": "string"},
                    "notes": {"type": "string"},
                },
            },
        },
        "confidence_score": {"type": "number"},
    },
    "required": [
        "budget_title",
        "one_line_summary",
        "fiscal_stance",
        "fiscal_deficit_gdp",
        "capex_outlay",
        "key_themes",
        "pli_schemes",
        "sector_allocations",
        "import_duty_changes",
        "tax_changes",
        "infrastructure_push",
        "sector_headwinds",
        "sector_tailwinds",
        "confidence_score",
    ],
}


def normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


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
        if text:
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
    raise RuntimeError(last_error or "No Ollama model could summarize the budget")


def summarize_budget(year: str, extracted_text: str, models: list[str]) -> dict[str, Any]:
    prompt = f"""You are a senior equity research analyst extracting budget intelligence for stock market impact analysis. Extract every detail that could affect listed Indian companies. Be specific about scheme names, allocation amounts, and which sectors or companies benefit or are hurt. Do not summarize vaguely — extract concrete numbers and named schemes.

Year: {year}

CRITICAL EXTRACTIONS:
- Every PLI scheme mentioned with allocation amounts and eligible sectors
- Every import duty change with old and new rates, and which sectors/companies are affected
- Which listed Indian companies or sectors directly benefit or are hurt by each major allocation
- Infrastructure capex programs with specific amounts and key projects
- Any changes to capital gains tax, STT, or corporate tax rates and their market impact
- Sector-specific regulatory changes embedded in the budget

TEXT:
{extracted_text[:16000]}

Return valid JSON only with complete and specific details (no vague summaries):
{{
  "budget_title": "e.g. Union Budget FY26 (2025-2026)",
  "one_line_summary": "one sentence on the overall budget stance",
  "fiscal_stance": "expansionary/contractionary/neutral with brief reason",
  "fiscal_deficit_gdp": "fiscal deficit as % of GDP",
  "capex_outlay": "total capital expenditure amount and YoY change",
  "key_themes": ["3-5 overarching themes"],
  "pli_schemes": [
    {{
      "scheme_name": "exact scheme name",
      "sector": "sector covered",
      "allocation": "amount in crores",
      "beneficiary_companies": ["specific companies mentioned"],
      "notes": "key conditions or targets"
    }}
  ],
  "sector_allocations": [
    {{
      "sector": "sector name",
      "allocation": "amount in crores",
      "yoy_change": "% change vs prior year",
      "key_programs": ["specific schemes funded"],
      "equity_impact": "which listed companies or sub-sectors benefit"
    }}
  ],
  "import_duty_changes": [
    {{
      "item": "product or category",
      "change": "increased/decreased/removed",
      "old_rate": "previous rate",
      "new_rate": "new rate",
      "equity_impact": "sectors or companies affected"
    }}
  ],
  "tax_changes": [
    {{
      "type": "corporate/personal/capital_gains/STT/other",
      "description": "specific change",
      "equity_impact": "market or sector impact"
    }}
  ],
  "infrastructure_push": [
    {{
      "category": "roads/railways/ports/energy/digital etc",
      "allocation": "amount",
      "key_projects": ["specific projects"],
      "equity_impact": "listed companies that benefit"
    }}
  ],
  "sector_headwinds": [
    {{
      "sector": "sector name",
      "reason": "what in the budget hurts this sector",
      "equity_impact": "specific companies or sub-sectors affected"
    }}
  ],
  "sector_tailwinds": [
    {{
      "sector": "sector name",
      "reason": "what in the budget helps this sector",
      "equity_impact": "specific companies or sub-sectors affected"
    }}
  ],
  "divestment_targets": [
    {{
      "company": "company name",
      "target_amount": "amount",
      "notes": "any conditions"
    }}
  ],
  "confidence_score": 1-10
}}"""

    parsed, raw, model_used = generate_with_fallback(prompt, models, BUDGET_SUMMARY_SCHEMA, num_predict=3000)
    parsed["raw"] = raw
    parsed["model"] = model_used
    return parsed


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def process_budget(pdf_paths: list[Path], year: str, output_root: Path, models: list[str], skip_llm: bool) -> BudgetArtifact:
    """Process one or more budget PDFs for a given year."""
    extracted_text_parts: list[str] = []

    for pdf_path in sorted(pdf_paths):
        pages = read_pdf_pages(pdf_path)
        pdf_text = "\n\n".join(
            f"[Page {page['page']}] {page['text']}" for page in pages if str(page.get("text", "")).strip()
        )
        extracted_text_parts.append(f"=== SOURCE: {pdf_path.name} ===\n{pdf_text}")

    extracted_text = "\n\n".join(extracted_text_parts)

    raw_dir = output_root / "raw"
    summary_dir = output_root / "summaries"
    ensure_dir(raw_dir)
    ensure_dir(summary_dir)

    text_path = raw_dir / f"{year}.txt"
    text_path.write_text(extracted_text, encoding="utf-8")

    pages = read_pdf_pages(pdf_paths[0]) if pdf_paths else []
    summary_payload: dict[str, Any]
    model_used = "none"

    if skip_llm:
        summary_payload = {
            "budget_title": year,
            "one_line_summary": f"LLM summarization skipped; {SKIP_LLM_PLACEHOLDER} only.",
            "key_allocations": [],
            "fiscal_measures": [],
            "sector_focus": [],
            "growth_initiatives": [],
            "confidence_score": 0,
            "source_files": [p.name for p in pdf_paths],
        }
    else:
        summary_payload = summarize_budget(year, extracted_text, models)
        model_used = str(summary_payload.get("model", models[0] if models else "unknown"))
        summary_payload["source_files"] = [p.name for p in pdf_paths]

    summary_payload.update(
        {
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

    return BudgetArtifact(
        pdf_paths=pdf_paths,
        text_path=text_path,
        summary_path=summary_path,
        page_count=len(pages),
        extracted_chars=len(extracted_text),
        model_used=model_used,
        year=year,
    )


def build_index(artifacts: list[BudgetArtifact], output_root: Path, skip_llm: bool) -> Path:
    index_path = output_root / "index.json"

    budget_summaries: list[dict[str, Any]] = []
    for artifact in sorted(artifacts, key=lambda item: item.year, reverse=True):
        payload = json.loads(artifact.summary_path.read_text(encoding="utf-8"))
        budget_summaries.append(
            {
                "year": artifact.year,
                "budget_title": payload.get("budget_title", artifact.year),
                "one_line_summary": payload.get("one_line_summary", ""),
                "key_allocations": payload.get("key_allocations", []),
                "fiscal_measures": payload.get("fiscal_measures", []),
                "sector_focus": payload.get("sector_focus", []),
                "growth_initiatives": payload.get("growth_initiatives", []),
                "confidence_score": payload.get("confidence_score", 0),
                "source_files": payload.get("source_files", [p.name for p in artifact.pdf_paths]),
                "summary_file": artifact.summary_path.name,
            }
        )

    payload = {
        "last_updated": datetime.now().isoformat(timespec="seconds"),
        "years": [s.get("year") for s in budget_summaries],
        "budget_count": len(budget_summaries),
        "summaries": {s.get("year"): s for s in budget_summaries},
    }
    ensure_dir(index_path.parent)
    write_json(index_path, payload)

    log.info("Wrote budget index with %d years to %s", len(budget_summaries), index_path)
    return index_path


def check_budget_staleness(index_path: Path, max_age_days: int = 400) -> bool:
    """Check if budget index is stale. Returns True if index is fresh, False if stale."""
    if not index_path.exists():
        return True

    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
        last_updated_str = payload.get("last_updated")
        if not last_updated_str:
            return True

        last_updated = datetime.fromisoformat(last_updated_str)
        age = datetime.now() - last_updated

        if age > timedelta(days=max_age_days):
            formatted_date = last_updated.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n⚠ Budget data is stale (last updated {formatted_date}). Consider re-running")
            print("budget_processor.py with the latest budget.")
            print("\nOptions:")
            print("  a) Continue anyway")
            print("  b) Abort\n")

            choice = input("Enter choice (a/b): ").strip().lower()
            if choice == "b":
                log.info("Aborting due to stale budget data")
                return False
            elif choice != "a":
                print("Invalid choice. Aborting.")
                return False
            return True

        return True
    except Exception as exc:
        log.warning("Failed to check budget staleness: %s", exc)
        return True


def discover_budgets(source_dir: Path) -> list[tuple[str, list[Path]]]:
    """Discover budget PDFs named FY##.pdf."""
    budgets: list[tuple[str, list[Path]]] = []

    if not source_dir.is_dir():
        return []

    # Look for FY*.pdf files
    fy_files = sorted([f for f in source_dir.glob("FY*.pdf")])

    for pdf_file in fy_files:
        year = pdf_file.stem  # e.g., "FY26" from "FY26.pdf"
        budgets.append((year, [pdf_file]))

    return budgets


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract and summarize government budgets into saved artifacts.")
    parser.add_argument("--input-dir", default="govt_budgets/raw", help="Folder containing budget PDFs named as FY##.pdf (e.g., FY26.pdf).")
    parser.add_argument("--output-dir", default="govt_budgets/processed", help="Folder where extracted text and summaries are saved.")
    parser.add_argument(
        "--year",
        help="Process only a specific year (e.g., fy26). If not specified, processes all years.",
    )
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

    index_path = output_root / "index.json"
    if not check_budget_staleness(index_path):
        return 1

    year_groups = discover_budgets(input_root)
    if not year_groups:
        log.error("No budget PDFs found under %s", input_root)
        return 1

    if args.year:
        year_groups = [(year, pdfs) for year, pdfs in year_groups if year == args.year]
        if not year_groups:
            log.error("No budgets found for year: %s", args.year)
            return 1

    log.info("Using input directory: %s", input_root)
    log.info("Found %d year groups", len(year_groups))

    artifacts: list[BudgetArtifact] = []
    for year, pdf_paths in year_groups:
        pdf_names = ", ".join(p.name for p in pdf_paths)
        log.info("Processing %s: %s", year, pdf_names)
        artifact = process_budget(pdf_paths, year, output_root, args.models, skip_llm)
        artifacts.append(artifact)
        log.info(
            "Saved %s (%d pages, %d chars, model=%s)",
            artifact.summary_path.name,
            artifact.page_count,
            artifact.extracted_chars,
            artifact.model_used,
        )

    build_index(artifacts, output_root, skip_llm)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
