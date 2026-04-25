import re
import shutil
from pathlib import Path

print("=== Annual Report Organizer (Name-based) ===\n")

SOURCE_DIR = Path(input("Source folder (parent folder containing year subfolders): ").strip())
TICKER = input("Ticker (e.g. HDFCBANK.NS): ").strip()
OUTPUT_BASE = Path(input("Output base (default: annual_reports/raw): ").strip() or "annual_reports/raw")

# Rules: checked in order, first match wins
# (keywords that must ALL be present, output name)
RULES = [
    (["agm", "transcript"],         "agm-transcript.pdf"),
    (["agm", "notice"],             "agm-notice.pdf"),
    (["annual", "report"],          "annual-report.pdf"),
    (["integrated", "report"],      "annual-report.pdf"),
    (["management", "discussion"],  "management-discussion.pdf"),
    (["directors", "report"],       "directors-report.pdf"),
    (["chairman"],                  "chairman-statement.pdf"),
    (["financial", "statement"],    "financial-statement.pdf"),
    (["sustainability"],            "sustainability-report.pdf"),
    (["concall", "transcript"],     "concall-transcript.pdf"),
    (["investor", "presentation"],  "investor-presentation.pdf"),
]

def classify(filename):
    lower = filename.lower()
    for keywords, output in RULES:
        if all(kw in lower for kw in keywords):
            return output
    return None

def extract_year(folder_name):
    # Accepts: fy24, FY24, 2024, 2023-24, fy2024 etc
    folder = folder_name.lower()
    m = re.search(r'fy(\d{2,4})', folder)
    if m:
        return m.group(1).zfill(2)[-2:]
    m = re.search(r'(\d{4})-\d{2}', folder)
    if m:
        return m.group(1)[-2:]
    m = re.search(r'(\d{4})', folder)
    if m:
        return m.group(1)[-2:]
    return None

print("\nProcessing...\n")

matched = 0
skipped = 0

for year_folder in sorted(SOURCE_DIR.iterdir()):
    if not year_folder.is_dir():
        continue

    year = extract_year(year_folder.name)
    if not year:
        print(f"SKIPPED folder (can't extract year): {year_folder.name}")
        continue

    for file in year_folder.iterdir():
        if file.suffix.lower() != ".pdf":
            continue

        rename_to = classify(file.name)
        if not rename_to:
            print(f"SKIPPED (no rule match): {year_folder.name}/{file.name}")
            skipped += 1
            continue

        dest_folder = OUTPUT_BASE / TICKER / f"fy{year}"
        dest_folder.mkdir(parents=True, exist_ok=True)
        dest = dest_folder / rename_to

        if dest.exists():
            stem = Path(rename_to).stem
            dest = dest_folder / f"{stem}_2.pdf"

        shutil.copy2(file, dest)
        print(f"{year_folder.name}/{file.name} → fy{year}/{rename_to}")
        matched += 1

print(f"\nDone. {matched} file(s) copied, {skipped} skipped.")