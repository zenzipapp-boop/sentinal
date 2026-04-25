# Project Overview

## What this project is

This repository is a local Indian equity research and satellite portfolio audit pipeline. It supports a single-investor workflow that combines:

- per-ticker portfolio state in `portfolio_data/`
- Screener.in fundamentals cached in `screener_data/`
- annual report context from `annual_reports/`
- market price history from Yahoo Finance
- local Ollama-powered model analysis
- weekly markdown research reports
- optional government budget summarization via `budget_processor.py`

The goal is human-led weekly review, not automated trading.

## Core purpose

The project exists to answer questions such as:

- Should this ticker stay on the watchlist or be sold?
- Does the business quality still support a satellite allocation?
- Has market action validated or contradicted the thesis?
- What changed since the last review?
- How should SIP allocation move this week?

## Current architecture

The repo is centered on file-based state, cached fundamentals, annual-report context, and local LLM analysis.

```text
portfolio_data/         <- current/watchlist/exited per-ticker state
screener_data/          <- cached Screener.in JSON snapshots
annual_reports/         <- raw report files and processed text/summaries
govt_budgets/           <- raw budget PDFs, processed text, and budget summaries
reports/                <- generated weekly markdown outputs
market_pipeline.py      <- main weekly research pipeline
annual_report_processor.py <- annual report extraction and summarize pipeline
budget_processor.py     <- government budget extraction and summary pipeline
run_mcp_server.py       <- Browser Use MCP helper for authenticated Screener extraction
verify_screener.py      <- Screener payload validation
```

### Main stages

1. Load portfolio state from `portfolio_data/`.
2. Fetch or reuse cached Screener fundamentals from `screener_data/`.
3. Load annual report context from `annual_reports/processed/`.
4. Download market data and derive features.
5. Run three Ollama model stages:
   - quality screener
   - thesis writer
   - thesis auditor
6. Generate markdown reports in `reports/`.

## Main files and roles

### `market_pipeline.py`

The main driver for weekly portfolio research. It handles:

- loading and normalizing portfolio records
- Screener.in extraction and caching
- market data download and feature generation
- annual report context ingestion
- running the three-stage Ollama model flow
- watchlist and satellite state updates
- writing weekly markdown reports

### `annual_report_processor.py`

Extracts and summarizes annual reports into structured artifacts under `annual_reports/processed/`.

### `budget_processor.py`

Extracts government budget PDFs from `govt_budgets/raw/`, processes text, and writes summaries into `govt_budgets/processed/`.

### `run_mcp_server.py`

Starts a local Browser Use MCP service and sets environment variables for authenticated Screener.in extraction.

### `verify_screener.py`

Validates Screener payload structure and expected table content.

## Data and state layout

- `portfolio_data/current/cores/`
- `portfolio_data/current/satellites/`
- `portfolio_data/watchlist/cores/`
- `portfolio_data/watchlist/satellites/`
- `portfolio_data/exited/`
- `screener_data/`
- `annual_reports/processed/`
- `govt_budgets/processed/`
- `reports/`

## Legacy support

The repo prefers per-ticker JSON under `portfolio_data/`, but still understands legacy `watchlist.json` and `satellites.json` if present.

## What the pipeline uses

- `yfinance` for price history and derived market features
- Screener.in for fundamentals
- Browser Use MCP for authenticated Screener extraction
- local Ollama models for multi-stage analysis
- annual report text for business context
- government budget summaries when available
- optional news sources when configured

## Current repo state

Based on the current workspace:

- `market_pipeline.py` is present and runnable.
- annual-report artifacts are generated for `DIXON.NS`.
- Screener snapshots are stored in `screener_data/`.
- portfolio state is held in `portfolio_data/`.
- Markdown reports exist in `reports/`.
- budget summary artifacts are available under `govt_budgets/processed/`.

## How the main pipeline works

### Inputs

- `portfolio_data/`
- `screener_data/`
- `annual_reports/processed/`
- environment variables such as `SCREENER_SESSION_ID`
- local Ollama service at `http://localhost:11434`

### Fundamental flow

- Load portfolio records and watchlist candidates.
- Fetch or reuse Screener fundamentals.
- Compute market context from price history.
- Assemble annual-report and optional budget context.
- Run the three-stage Ollama analysis flow.
- Write reports and update portfolio state.

## Model stages

### Stage 1: Quality screener

- Evaluates whether the ticker remains a satellite candidate.
- Produces a conviction score, risk summary, and quality verdict.

### Stage 2: Thesis writer

- Crafts or refreshes the investment thesis.
- Compares the current story to prior history and context.

### Stage 3: Thesis auditor

- Acts as a skeptical risk manager.
- Identifies weak points, invalidation triggers, and red flags.

## Report outputs

The pipeline writes reports into `reports/`.

- `SATELLITE_REPORT_YYYY-MM-DD.md` — human-readable summary review.
- `detailed_satellite_report_YYYY-MM-DD.md` — trace report with prompts and raw outputs.

## Annual report processing

`annual_report_processor.py` supports:

- PDF text extraction
- chunked report processing
- structured JSON summaries
- per-ticker annual report indices

## Budget processing

`budget_processor.py` supports:

- government budget PDF extraction
- text conversion and page-level artifacts
- structured budget summaries for fiscal analysis

## Browser Use MCP

`run_mcp_server.py` starts a local MCP helper for authenticated Screener.in extraction and configures the runtime for Ollama.

## Screener verification

`verify_screener.py` checks that Screener payloads contain expected sections and table structure.

## Recommended environment

- Python 3.11+
- local Ollama models and `langchain-ollama`
- Browser Use credentials for Screener.in extraction
- enough disk space for cached JSON, annual reports, and generated markdown

## Practical state

The workspace is set up for weekly satellite research using `portfolio_data/`, with generated reports in `reports/` and processed artifacts in `annual_reports/processed/` and `govt_budgets/processed/`.
