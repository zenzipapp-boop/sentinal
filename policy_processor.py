#!/usr/bin/env python3
"""Download and manage government policy documents for the sentinel pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import tempfile
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("policy-processor")

BASE_DIR = Path(__file__).parent.resolve()
POLICY_SOURCES_PATH = BASE_DIR / "govt_policies" / "policy_sources.json"


def load_policy_sources() -> dict:
    if not POLICY_SOURCES_PATH.exists():
        return {"sources": []}
    try:
        with open(POLICY_SOURCES_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"Failed to load policy sources: {e}")
        return {"sources": []}


def save_policy_sources(sources: dict) -> None:
    try:
        POLICY_SOURCES_PATH.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            'w',
            delete=False,
            encoding='utf-8',
            dir=str(POLICY_SOURCES_PATH.parent),
            prefix=f".{POLICY_SOURCES_PATH.name}.",
            suffix=".tmp",
        ) as handle:
            json.dump(sources, handle, indent=2, ensure_ascii=False)
            temp_name = handle.name
        Path(temp_name).replace(POLICY_SOURCES_PATH)
        log.info(f"Saved policy sources to {POLICY_SOURCES_PATH}")
    except Exception as e:
        log.error(f"Failed to save policy sources: {e}")


def verify_pdf(content: bytes) -> bool:
    if len(content) < 10240:
        return False
    if not content.startswith(b'%PDF'):
        return False
    return True


def download_policies(sector_filter: str | None = None, force: bool = False) -> None:
    sources = load_policy_sources()
    download_dir = BASE_DIR / "govt_policies" / "pdfs"

    if not sources.get("sources"):
        log.warning("No policy sources configured")
        return

    for source in sources["sources"]:
        sector = source.get("sector", "").lower()
        if sector_filter and sector != sector_filter.lower():
            continue

        filename = source.get("filename", "")
        policy_name = source.get("policy_name", "unknown")
        urls = source.get("urls", [])

        if not filename or not urls:
            log.warning(f"Skipping {policy_name}: missing filename or URLs")
            continue

        output_path = download_dir / sector / filename
        if output_path.exists() and not force:
            log.info(f"⊘ {sector}/{filename} already exists (use --force to overwrite)")
            continue

        content = None
        for url in urls:
            try:
                response = requests.get(url, timeout=15)
                if response.status_code == 200 and verify_pdf(response.content):
                    content = response.content
                    log.info(f"✓ Downloaded from {url}")
                    break
                else:
                    log.debug(f"✗ Failed from {url}: status {response.status_code}")
            except Exception as e:
                log.debug(f"✗ Failed to fetch {url}: {e}")

        if content:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(content)
            log.info(f"✓ Saved: {sector}/{filename}")
        else:
            log.warning(f"✗ Could not download {policy_name} from any source")


def add_source_interactive() -> None:
    print("\n=== Add Policy Source ===\n")

    sector = input("Sector (e.g., electronics): ").strip().lower()
    if not sector:
        print("Sector is required")
        return

    policy_name = input("Policy name (e.g., PLI Electronics): ").strip()
    if not policy_name:
        print("Policy name is required")
        return

    authority = input("Authority (e.g., MeitY): ").strip()
    filename = input("Output filename (e.g., pli_electronics.pdf): ").strip()
    if not filename:
        print("Filename is required")
        return

    urls = []
    print("Enter URLs (one per line, blank line to finish):")
    while True:
        url = input("  URL: ").strip()
        if not url:
            break
        urls.append(url)

    if not urls:
        print("At least one URL is required")
        return

    new_source = {
        "sector": sector,
        "policy_name": policy_name,
        "authority": authority or "Unknown",
        "urls": urls,
        "filename": filename,
    }

    sources = load_policy_sources()
    sources.setdefault("sources", []).append(new_source)
    save_policy_sources(sources)

    print(f"\n✓ Added source: {policy_name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Download and manage government policy documents.")
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download policy PDFs from configured sources"
    )
    parser.add_argument(
        "--sector",
        metavar="SECTOR",
        help="Filter downloads to a specific sector (e.g., electronics)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing downloaded PDFs"
    )
    parser.add_argument(
        "--add-source",
        action="store_true",
        help="Interactively add a new policy source"
    )

    args = parser.parse_args()

    if args.download:
        download_policies(sector_filter=args.sector, force=args.force)
        return 0

    if args.add_source:
        add_source_interactive()
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
