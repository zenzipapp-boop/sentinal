# Implementation Summary: Conviction Score Tracking & Budget Sector Mapping

## 1. Change Log Section in Weekly Reports

### What was added:
- **Location:** `market_pipeline.py` → `build_detailed_report()` function
- **New function:** `_conviction_changelog()` compares Conviction Scores week-over-week

### How it works:
- Reads the `audit_log` from each satellite's record
- Compares the most recent conviction score with the previous week's score
- Displays changes with directional arrows (↑ increase, ↓ decrease, → unchanged)
- Only shows tickers with at least 2 audit log entries (historical data)

### Output format:
```markdown
## Change Log

Conviction Score tracking (previous week → this week):

- **TICKER1:** 7.2 → 7.8 ↑ +0.6
- **TICKER2:** 6.5 → 6.5 → 0.0
- **TICKER3:** 8.1 → 7.4 ↓ -0.7
```

### Integration:
- Automatically inserted at the top of `detailed_satellite_report_YYYY-MM-DD.md`
- Positioned after the portfolio summary metrics
- Only renders if there's conviction history available

---

## 2. Budget Sector Tagging & Portfolio Impact Mapping

### What was added:
- **Location:** `budget_processor.py`
- **New functions:**
  - `extract_sector_tags()` - Extracts all sectors mentioned in budget
  - `flag_portfolio_by_sector()` - Matches portfolio tickers to budget-impacted sectors
  - `SECTOR_KEYWORDS_MAP` - Intelligent sector-to-ticker mapping

### How it works:

#### Sector extraction from budget:
1. Collects sectors from:
   - `sector_allocations` - Allocated funds by sector
   - `pli_schemes` - Manufacturing incentive programs
   - `infrastructure_push` - Infrastructure categories
   - `sector_headwinds` - Negative impacts
   - `sector_tailwinds` - Positive impacts

2. Tags each sector with the budget action type:
   - `allocation` - Direct funding
   - `pli_scheme` - Production incentives
   - `infra_*` - Infrastructure category
   - `headwind` - Sector negative impact
   - `tailwind` - Sector positive impact

#### Portfolio flagging:
Uses a pre-defined `SECTOR_KEYWORDS_MAP` to identify tickers in your portfolio that belong to budget-tagged sectors:

```python
SECTOR_KEYWORDS_MAP = {
    "infrastructure": ["L&T", "BHARTIARTL", "POWERINDIA", ...],
    "defense": ["HAL", "BEL", "MAZAGON", ...],
    "semiconductors": ["INFY", "TCS", "WIPRO", ...],
    "banking": ["HDFCBANK", "ICICIBANK", "AXISBANK", ...],
    # ... more sectors
}
```

### Output:
Creates a new file `{year}_sector_impact.json` with:
```json
{
  "year": "fy26",
  "sector_tags": {
    "Infrastructure": ["allocation", "infra_roads"],
    "Defense": ["pli_scheme", "tailwind"],
    "Semiconductors": ["allocation"]
  },
  "flagged_tickers": {
    "L&T": ["allocation", "infra_roads"],
    "HAL": ["pli_scheme", "tailwind"],
    "INFY": ["allocation"]
  },
  "generated_at": "2026-04-25T15:30:00"
}
```

### Data structure updates:
- `BudgetArtifact` dataclass now includes `sector_tags: dict[str, list[str]]`
- Sector tags are populated during `process_budget()` execution

---

## 3. Usage & Next Steps

### Running the pipeline:
```bash
# Budget processing with sector tagging
python budget_processor.py --year fy26

# Satellite report generation (includes Change Log)
python market_pipeline.py
```

### Viewing results:
1. **Change Log** → Check `reports/detailed_satellite_report_YYYY-MM-DD.md`
2. **Sector Impact** → Check `govt_budgets/processed/summaries/FY##_sector_impact.json`
3. **Flagged Tickers** → Filter by sectors in the sector_impact JSON

---

## 4. Enhancement Opportunities

### Future improvements:
1. **Dynamic sector mapping** - Load sector-ticker associations from a CSV/database
2. **Conviction trend analysis** - Multi-week conviction trajectories with smoothing
3. **Budget-to-thesis linking** - Flag satellites whose thesis aligns with budget sectors
4. **Sector sensitivity scoring** - Weight tickers by their sector exposure intensity
5. **Budget year comparison** - Track how budget treatment of sectors evolves YoY

