# Project Overview

## What this project is

This repository is a local Indian equity research and satellite portfolio audit pipeline. It is designed to help a single investor or analyst maintain a compact set of ideas by combining:

- per-ticker portfolio state
- Screener.in fundamentals
- market data from Yahoo Finance
- optional news context
- annual report extraction
- local three-stage Ollama model analysis
- weekly markdown research reports

The goal is not automated trading. It is a human-first weekly review workflow.

## Core purpose

The project exists to answer questions such as:

- Should this ticker stay on the watchlist?
- Does the business quality still support a satellite allocation?
- Has price action validated or contradicted the thesis?
- What changed since the last review?
- How should SIP allocation move this week?

## Current architecture

The current repo is centered on file-based state and local services.

```text
portfolio_data/         <- current/watchlist/exited portfolio state
screener_data/          <- cached Screener.in JSON snapshots
annual_reports/         <- raw PDFs + processed text/summaries
reports/                <- generated weekly markdown outputs
market_pipeline.py      <- main pipeline
annual_report_processor.py
run_mcp_server.py
verify_screener.py
```

### Main stages

1. Portfolio state load from `portfolio_data/`.
2. Fundamental extraction or reuse of cached Screener data.
3. Annual report context ingestion from `annual_reports/processed/`.
4. Market data download and feature derivation.
5. Three-stage local LLM analysis:
   - quality screen
   - thesis writer
   - skeptical auditor
6. Report generation into `reports/`.

## Main files and roles

### `market_pipeline.py`

The main driver for the weekly workflow. It handles:

- portfolio state loading and management
- Screener data caching and fetching
- annual report context loading
- market data extraction
- three-stage model prompting and parsing
- watchlist and active position management
- markdown report generation

### `annual_report_processor.py`

Extracts annual report text from files under `annual_reports/` and writes processed artifacts into `annual_reports/processed/`.

### `run_mcp_server.py`

Starts a local Browser Use MCP service to fetch authenticated Screener.in pages if needed.

### `verify_screener.py`

Validates Screener.in payload structure and content.

## Data and state layout

- `portfolio_data/current/cores/`
- `portfolio_data/current/satellites/`
- `portfolio_data/watchlist/cores/`
- `portfolio_data/watchlist/satellites/`
- `portfolio_data/exited/`
- `screener_data/`
- `annual_reports/processed/`
- `reports/`

## Legacy support

The project still understands legacy `watchlist.json` and `satellites.json`, but the preferred current state model is per-ticker JSON files under `portfolio_data/`.

## What the pipeline uses

- `yfinance` for price history and derived market features
- Screener.in for company fundamentals
- Browser Use MCP for authenticated Screener extraction
- local Ollama models for analysis
- annual report PDFs for deeper business context
- optional news sources when configured

## Current repo state

Based on the current workspace:

- The main pipeline and report generation flow are present.
- Annual-report extraction artifacts are present for `DIXON.NS`.
- Screener snapshots exist in `screener_data/`.
- Portfolio state is managed through `portfolio_data/`.
- Generated markdown reports are stored in `reports/`.

## How the main pipeline works

### Inputs

- `portfolio_data/`
- `screener_data/`
- `annual_reports/processed/`
- environment variables such as `SCREENER_SESSION_ID`

### Fundamental flow

- Load portfolio state and watchlist candidates.
- Fetch or reuse Screener fundamentals.
- Compute market context from price history.
- Build prompt context from annual reports and news.
- Run the three local models sequentially.
- Write report artifacts and update state.

## Model stages

### Stage 1: Quality screen

- Model assesses whether the ticker should remain a candidate or be a satellite.
- Produces a quality verdict and risk summary.

### Stage 2: Thesis writer

- Writes or refreshes a thesis for the ticker.
- Compares the current story to prior history.

### Stage 3: Thesis auditor

- Acts skeptically.
- Finds weak points, invalidation triggers, and red flags.

## Report outputs

The pipeline writes reports into `reports/`.

- `SATELLITE_REPORT_YYYY-MM-DD.md` — summary review for human reading.
- `detailed satellite report llm version YYYY-MM-DD.md` — trace report with prompts and raw outputs.

## Annual report processing

`annual_report_processor.py` supports:

- PDF text extraction
- chunked report processing
- optional local summarization
- per-report and per-ticker JSON summaries

## Browser Use MCP

`run_mcp_server.py` starts a local server for authenticated Screener.in extraction. This is useful when direct HTTP extraction is insufficient.

## Screener verification

`verify_screener.py` can validate a Screener payload or a watchlist of tickers.

## Recommended environment

- Python 3.11+
- local Ollama models or compatible LLM runtime
- Browser Use credentials for Screener extraction
- enough disk space for cached JSON and report artifacts

## Current practical state

The current workspace is configured for watchlist-style research with a working pipeline. The portfolio memory path is active through `portfolio_data/`, and current generated reports are stored in `reports/`.
