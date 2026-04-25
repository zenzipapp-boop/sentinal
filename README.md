# Sentinal

A local Indian equity research pipeline for weekly satellite portfolio review.

This repository combines per-ticker portfolio state, Screener.in fundamentals, annual report context, market data, and a three-stage local LLM flow to generate human-readable markdown research reports.

## What is in this repo

- `market_pipeline.py` — the main weekly research pipeline and portfolio manager.
- `annual_report_processor.py` — extracts and processes annual report text and summaries.
- `run_mcp_server.py` — starts a local Browser Use MCP server for authenticated Screener.in extraction.
- `verify_screener.py` — validates Screener.in extraction payloads.
- `portfolio_data/` — current/watchlist/exited per-ticker JSON state.
- `screener_data/` — cached Screener.in JSON snapshots.
- `annual_reports/` — raw PDFs plus processed extracted text and summaries.
- `reports/` — generated weekly markdown outputs.
- `requirements.txt` — Python dependency list.

## Current workflow

1. `market_pipeline.py` loads portfolio state from `portfolio_data/`.
2. It fetches or reads cached Screener.in fundamentals from `screener_data/`.
3. It loads annual-report context from `annual_reports/processed/`.
4. It downloads market data for tickers.
5. It runs a three-stage Ollama model sequence (quality screen, thesis writer, auditor).
6. It writes report artifacts into `reports/`.

## Data layout

- `portfolio_data/current/cores/` — active core holdings.
- `portfolio_data/current/satellites/` — active satellite holdings.
- `portfolio_data/watchlist/cores/` — watchlist core candidates.
- `portfolio_data/watchlist/satellites/` — watchlist satellite candidates.
- `portfolio_data/exited/` — exited positions.
- `screener_data/` — Screener.in extraction JSON files per ticker.
- `annual_reports/processed/` — extracted text, summaries, and per-ticker indices.

## Installation

```bash
cd /home/yeet/sentinal
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Run the main weekly pipeline:

```bash
python market_pipeline.py
```

Optional flags:

```bash
python market_pipeline.py --watchlist my_stocks.json
python market_pipeline.py --screener DIXON.NS
python market_pipeline.py --browser-live
python market_pipeline.py --add "DIXON.NS:840:30"
python market_pipeline.py --exit DIXON.NS
```

## Annual report processing

Process or reprocess annual reports with:

```bash
python annual_report_processor.py --input-dir annual_reports --output-dir annual_reports/processed
```

This writes extracted text and JSON summaries for each ticker under `annual_reports/processed/`.

## Browser Use MCP

`run_mcp_server.py` starts a local MCP service that the pipeline can use to fetch authenticated Screener.in data when direct API extraction is unavailable.

## Screener verification

`verify_screener.py` checks that Screener payloads contain the expected sections and table structure.

## Notes

- The pipeline now uses `portfolio_data/` as the primary persistent state store.
- Legacy `watchlist.json` and `satellites.json` support is still available, but the active project layout is per-ticker JSON files under `portfolio_data/`.
- Generated reports are written to `reports/`, not the repository root.
- `screener_data/` contains the cached fundamentals the pipeline prefers.

## Recommended setup

- Python 3.11+
- Local Ollama or compatible LLM environment for the three-stage model flow.
- Browser Use credentials or profile for reliable Screener.in extraction.

## Existing outputs

The current workspace includes generated reports in `reports/` and processed annual-report artifacts in `annual_reports/processed/`.
