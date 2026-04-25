#!/usr/bin/env python3
"""Download and manage government policy documents for the sentinel pipeline."""

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

from market_pipeline import extract_json, ollama_generate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("policy-processor")

BASE_DIR = Path(__file__).parent.resolve()
SKIP_LLM_PLACEHOLDER = "raw text extracted"


@dataclass
class PolicyArtifact:
    pdf_path: Path
    sector: str
    summary_path: Path
    extracted_chars: int
    model_used: str


POLICY_SUMMARY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "sector": {"type": "string"},
        "policy_title": {"type": "string"},
        "effective_date": {"type": "string"},
        "issuing_authority": {"type": "string"},
        "policy_type": {"type": "string"},
        "key_provisions": {"type": "array", "items": {"type": "string"}},
        "compliance_requirements": {"type": "array", "items": {"type": "string"}},
        "beneficiaries": {"type": "array", "items": {"type": "string"}},
        "headwinds": {"type": "array", "items": {"type": "string"}},
        "equity_impact": {"type": "string"},
        "effective_until": {"type": "string"},
        "confidence_score": {"type": "number"},
    },
    "required": [
        "sector",
        "policy_title",
        "effective_date",
        "issuing_authority",
        "policy_type",
        "key_provisions",
        "compliance_requirements",
        "beneficiaries",
        "headwinds",
        "equity_impact",
        "effective_until",
        "confidence_score",
    ],
}


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
            effective_ctx = 131072
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
    raise RuntimeError(last_error or "No Ollama model could summarize the policy")


def summarize_policy(sector: str, pdf_name: str, extracted_text: str, models: list[str]) -> dict[str, Any]:
    prompt = f"""You are a financial policy analyst extracting structured intelligence from government policy documents for stock market impact analysis. Extract every detail that could affect listed Indian companies.

Document: {pdf_name}
Sector: {sector}

CRITICAL EXTRACTIONS:
- Policy title and official name
- Effective date and expiration/review date
- Issuing authority (RBI, SEBI, MeitY, TRAI, Ministry of X, etc.)
- Policy classification: incentive/regulation/restriction/guideline
- Key provisions that affect companies in this sector
- Compliance requirements for companies
- Companies or segments that directly benefit (beneficiaries)
- Companies or segments negatively affected (headwinds)
- Listed company impact with specific names where applicable
- Effective until date or "ongoing" if no end date

SENTIMENT & SCORING:
Score the policy's impact on listed companies from 1-10 where 10 means highly beneficial and 1 means highly detrimental. Provide a concise paragraph on equity market impact focusing on specific sectors and companies.

TEXT:
{extracted_text[:16000]}

Return valid JSON only:
{{
  "sector": "{sector}",
  "policy_title": "official policy name",
  "effective_date": "YYYY-MM-DD or N/A if unknown",
  "issuing_authority": "RBI/SEBI/MeitY/TRAI/Ministry name",
  "policy_type": "incentive/regulation/restriction/guideline",
  "key_provisions": ["provision 1", "provision 2", "provision 3"],
  "compliance_requirements": ["requirement 1", "requirement 2"],
  "beneficiaries": ["company or segment 1", "company or segment 2"],
  "headwinds": ["company or segment negatively affected"],
  "equity_impact": "1-2 paragraphs on which listed companies benefit or are hurt, with specific names",
  "effective_until": "YYYY-MM-DD or 'ongoing' if no end date",
  "confidence_score": 1-10
}}"""

    parsed, raw, model_used = generate_with_fallback(prompt, models, POLICY_SUMMARY_SCHEMA, num_predict=1200)
    parsed["model"] = model_used
    return parsed


def process_policy(pdf_path: Path, sector: str, output_root: Path, models: list[str], skip_llm: bool) -> PolicyArtifact:
    """Process a single policy PDF for a given sector."""
    pages = read_pdf_pages(pdf_path)
    extracted_text = "\n\n".join(
        f"[Page {page['page']}] {page['text']}" for page in pages if str(page.get("text", "")).strip()
    )

    summary_dir = output_root / sector / "summaries"
    ensure_dir(summary_dir)

    pdf_stem = pdf_path.stem
    summary_payload: dict[str, Any]
    model_used = "none"

    if skip_llm:
        summary_payload = {
            "sector": sector,
            "policy_title": pdf_stem,
            "effective_date": "N/A",
            "issuing_authority": "Unknown",
            "policy_type": "unknown",
            "key_provisions": [],
            "compliance_requirements": [],
            "beneficiaries": [],
            "headwinds": [],
            "equity_impact": f"LLM summarization skipped; {SKIP_LLM_PLACEHOLDER} only.",
            "effective_until": "N/A",
            "confidence_score": 0,
            "source_file": pdf_path.name,
        }
    else:
        summary_payload = summarize_policy(sector, pdf_path.name, extracted_text, models)
        model_used = str(summary_payload.get("model", models[0] if models else "unknown"))
        summary_payload["source_file"] = pdf_path.name

    summary_payload.update(
        {
            "extracted_chars": len(extracted_text),
            "page_count": len(pages),
            "extraction_quality": "good" if len(extracted_text) > 2000 else "limited",
            "processed_at": datetime.now().isoformat(timespec="seconds"),
        }
    )

    summary_path = summary_dir / f"{pdf_stem}.json"
    atomic_write_json(summary_path, summary_payload)

    return PolicyArtifact(
        pdf_path=pdf_path,
        sector=sector,
        summary_path=summary_path,
        extracted_chars=len(extracted_text),
        model_used=model_used,
    )


def build_policy_index(artifacts: list[PolicyArtifact], output_root: Path, sector: str) -> Path:
    """Build index.json for a sector's policies."""
    index_path = output_root / sector / "index.json"

    policy_summaries: list[dict[str, Any]] = []
    for artifact in sorted(artifacts, key=lambda a: a.pdf_path.name):
        try:
            payload = json.loads(artifact.summary_path.read_text(encoding="utf-8"))
            policy_summaries.append(
                {
                    "policy_title": payload.get("policy_title", artifact.pdf_path.stem),
                    "effective_date": payload.get("effective_date", "N/A"),
                    "issuing_authority": payload.get("issuing_authority", "Unknown"),
                    "policy_type": payload.get("policy_type", "unknown"),
                    "confidence_score": payload.get("confidence_score", 0),
                    "source_file": payload.get("source_file", artifact.pdf_path.name),
                    "summary_file": artifact.summary_path.name,
                }
            )
        except Exception as exc:
            log.warning("Failed to read summary for %s: %s", artifact.summary_path, exc)

    index_payload = {
        "sector": sector,
        "last_updated": datetime.now().isoformat(timespec="seconds"),
        "policy_count": len(policy_summaries),
        "policies": policy_summaries,
    }
    atomic_write_json(index_path, index_payload)

    log.info("Wrote policy index for %s with %d policies to %s", sector, len(policy_summaries), index_path)
    return index_path


def discover_policies(source_dir: Path, sector_filter: str | None = None) -> dict[str, list[Path]]:
    """Discover policy PDFs organized as {sector}/{filename}.pdf."""
    policies: dict[str, list[Path]] = {}

    if not source_dir.is_dir():
        return {}

    for sector_dir in source_dir.glob("*/"):
        if not sector_dir.is_dir():
            continue

        sector = sector_dir.name.lower()
        if sector_filter and sector != sector_filter.lower():
            continue

        pdf_files = sorted([f for f in sector_dir.glob("*.pdf")])
        if pdf_files:
            policies[sector] = pdf_files

    return policies


def show_import_helper() -> None:
    """Show manual import instructions for policy documents."""
    print("\n" + "=" * 50)
    print("MANUAL POLICY DOCUMENT IMPORT")
    print("=" * 50)
    print("\nDownload PDFs from government websites in your browser and save")
    print("them to the correct folder.\n")

    print("Folder structure:")
    sectors = [
        ("electronics", "MeitY, PLI electronics, Semicon Mission"),
        ("banking", "RBI circulars, master directions"),
        ("infrastructure", "NIP, PM Gati Shakti"),
        ("pharma", "PLI Pharma, drug policy"),
        ("auto", "FAME, PLI Auto, scrappage"),
        ("telecom", "PLI Telecom, TRAI, 5G"),
        ("energy", "Solar Mission, Green H2, Electricity Act"),
        ("it", "DPDP Act, Digital India"),
        ("fmcg", "FSSAI, FDI retail"),
    ]
    for sector, description in sectors:
        print(f"  govt_policies/raw/{sector:20} → {description}")

    print("\nSuggested sources to download from your browser:")
    sources = [
        "pib.gov.in          → Press releases and policy documents",
        "meity.gov.in        → Electronics and IT policies",
        "rbi.org.in          → Banking circulars and master directions",
        "sebi.gov.in         → Capital markets regulations",
        "mnre.gov.in         → Renewable energy policies",
        "dot.gov.in          → Telecom policies",
        "dpiit.gov.in        → Industrial and investment policies",
        "pharmaceuticals.gov.in → Pharma policies",
    ]
    for source in sources:
        print(f"  {source}")

    print("\nAfter saving files, run:")
    print("  python policy_processor.py --process")
    print("  python policy_processor.py --process --sector electronics\n")

    print("Currently saved files:")
    list_saved_files()


def list_saved_files(sector_filter: str | None = None) -> None:
    """List all saved PDFs in govt_policies/raw/ grouped by sector."""
    raw_dir = BASE_DIR / "govt_policies" / "raw"

    if not raw_dir.exists():
        print("  (no files saved yet)\n")
        return

    has_any_files = False
    for sector_dir in sorted(raw_dir.glob("*/")):
        if not sector_dir.is_dir():
            continue

        sector = sector_dir.name.lower()
        if sector_filter and sector != sector_filter.lower():
            continue

        pdf_files = sorted(sector_dir.glob("*.pdf"))

        processed_dir = BASE_DIR / "govt_policies" / "processed" / sector / "summaries"
        processed_files = set(processed_dir.glob("*.json")) if processed_dir.exists() else set()

        if pdf_files:
            has_any_files = True
            print(f"  Sector: {sector}")
            for pdf_file in pdf_files:
                size_kb = pdf_file.stat().st_size / 1024
                is_processed = any(
                    p.stem == pdf_file.stem for p in processed_files
                )
                status = "processed: yes" if is_processed else "processed: no"
                print(f"    {pdf_file.name} ({size_kb:.1f} KB) — {status}")

    if not has_any_files:
        print("  (no files saved yet)")

    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage government policy documents for the sentinel pipeline.")
    parser.add_argument(
        "--import",
        action="store_true",
        dest="show_import",
        help="Show manual import instructions for policy documents"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List saved policy PDFs and their processing status"
    )
    parser.add_argument(
        "--process",
        action="store_true",
        help="Process and summarize policy PDFs with LLM"
    )
    parser.add_argument(
        "--sector",
        metavar="SECTOR",
        help="Filter to a specific sector (e.g., electronics)"
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
        help="Extract raw text only, skip LLM summarization.",
    )

    args = parser.parse_args()

    if args.show_import:
        show_import_helper()
        return 0

    if args.list:
        print("Saved policy files:\n")
        list_saved_files(sector_filter=args.sector)
        return 0

    if args.process:
        input_root = BASE_DIR / "govt_policies" / "raw"
        output_root = BASE_DIR / "govt_policies" / "processed"

        policy_groups = discover_policies(input_root, sector_filter=args.sector)
        if not policy_groups:
            log.error("No policy PDFs found under %s", input_root)
            return 1

        log.info("Using input directory: %s", input_root)
        log.info("Found %d sectors", len(policy_groups))

        for sector, pdf_paths in sorted(policy_groups.items()):
            log.info("Processing sector: %s (%d policies)", sector, len(pdf_paths))
            artifacts: list[PolicyArtifact] = []

            for pdf_path in pdf_paths:
                log.info("  Processing %s", pdf_path.name)
                artifact = process_policy(pdf_path, sector, output_root, args.models, args.skip_llm)
                artifacts.append(artifact)
                log.info(
                    "    Saved %s (%d chars, model=%s)",
                    artifact.summary_path.name,
                    artifact.extracted_chars,
                    artifact.model_used,
                )

            build_policy_index(artifacts, output_root, sector)

        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
