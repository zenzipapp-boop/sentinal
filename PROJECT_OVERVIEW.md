# Project Overview

## What This Project Is

This repository is a local, Python-based weekly stock research pipeline focused on Indian equity "satellite" positions. Its job is not to place trades. Instead, it:

1. Collects market data and recent news for each ticker.
2. Pulls fundamentals from Screener.in.
3. Runs a three-stage local LLM analysis loop.
4. Updates a persistent portfolio/state file.
5. Writes markdown reports for a human to review.

The overall design is opinionated and intentionally manual. It is built to help a single investor or analyst maintain a small set of higher-conviction names over time, review thesis drift, and size a weekly SIP contribution based on conviction.

## The Core Purpose

The project exists to answer questions like:

- Should this ticker remain on the watchlist?
- Does the business quality still justify holding it as a satellite position?
- Has the price action confirmed or contradicted the fundamentals?
- What changed since the last weekly review?
- How much of this week’s SIP should go to each active position?

The output is meant to be readable by a human, and in the main flow it is also designed to be pasted into Claude Sonnet for a final synthesis pass.

## High-Level Architecture

The pipeline currently looks like this:

```text
watchlist.json + satellites.json
  |
  +--> yfinance --> OHLCV, trend, volume, RSI, moving averages
  +--> News collectors --> NSE, BSE, Moneycontrol, yfinance, RSS, SearXNG
  +--> Screener.in fetch --> browser-use MCP, JSON API, or HTTP fallback
  +--> annual_report_processor outputs --> processed annual report context
  |
  +--> Stage 1 LLM --> quality screen
  +--> Stage 2 LLM --> thesis writer
  +--> Stage 3 LLM --> skeptical auditor
  |
  +--> SIP sizing engine
  +--> satellites.json update
  +--> SATELLITE_REPORT_YYYY-MM-DD.md
  +--> detailed satellite report llm version YYYY-MM-DD.md
```

The code is built around local services and cached artifacts instead of a remote backend.

## Main Files And Their Roles

### [`market_pipeline.py`](/home/yeet/PycharmProjects/PythonProject/market_pipeline.py)

This is the main application. It contains:

- CLI entry points.
- Watchlist and satellite-state loading/saving.
- Market data extraction.
- News extraction.
- Screener.in extraction.
- Ollama prompt generation and response parsing.
- SIP sizing.
- Markdown report generation.
- The weekly end-to-end pipeline.

This file is the heart of the project.

### [`annual_report_processor.py`](/home/yeet/PycharmProjects/PythonProject/annual_report_processor.py)

This script reads annual report PDFs from `annual_reports/`, extracts text, optionally summarizes it with local models, and writes:

- raw text files
- per-report summary JSON files
- an index JSON file per ticker

The main pipeline later reads this annual-report context and injects it into its prompts.

### [`run_mcp_server.py`](/home/yeet/PycharmProjects/PythonProject/run_mcp_server.py)

This starts the Browser Use MCP server locally. The main pipeline uses it to fetch authenticated Screener.in data through a browser session when possible.

### [`verify_screener.py`](/home/yeet/PycharmProjects/PythonProject/verify_screener.py)

A standalone validation tool for Screener.in payloads. It checks that the extracted tables contain expected columns and values.

### Data Files

- [`watchlist.json`](/home/yeet/PycharmProjects/PythonProject/watchlist.json)
- [`satellites.json`](/home/yeet/PycharmProjects/PythonProject/satellites.json)
- [`screener_data/`](/home/yeet/PycharmProjects/PythonProject/screener_data)
- [`annual_reports/`](/home/yeet/PycharmProjects/PythonProject/annual_reports)
- Generated markdown reports in the project root

## What The Pipeline Is Based On

The project is based on a mix of local and external data sources:

- `yfinance` for price history and basic market context.
- Screener.in for fundamentals, shareholding, quarterly results, and ratios.
- Browser Use MCP for authenticated Screener.in extraction.
- Local Ollama models for all three analysis stages.
- `feedparser`, NSE/BSE endpoints, Moneycontrol, RSS feeds, and optional SearXNG for news.
- Local annual report PDFs for deeper context.

It is not based on a traditional database or web framework. The persistent state is file-based.

## Current State Of The Project

This is the important "where it is right now" snapshot based on the files in the workspace today.

### Current repository state

- The main pipeline, annual-report processor, Screener verifier, and MCP server are all present.
- Dependencies are listed in `requirements.txt`.
- The project is configured for Python 3.11+ and local Ollama usage.
- Cached Screener snapshots exist for `DIXON.NS`, `HDFCBANK.NS`, `POLYCAB.NS`, `TATAELXSI.NS`, and `TEST.NS`.

### Current watchlist

The active watchlist file currently contains:

- `DIXON.NS`
- `TATAELXSI.NS`
- `POLYCAB.NS`

### Current satellite portfolio state

`satellites.json` is currently empty, so there are no active held satellites in the persistent portfolio store right now.

That means the weekly pipeline currently runs more like a watchlist research loop than a live portfolio rebalancing loop.

### Current annual-report context

The annual report output exists for `DIXON.NS` and includes five extracted annual reports:

- `20-21`
- `21-22`
- `22-23`
- `23-24`
- `24-25`

The current generated index shows that LLM summarization was skipped for that run, so the report summaries are placeholders that say raw text was extracted only. The raw extracted text files are present, which means the annual-report ingestion path exists, but the synthesis stage was intentionally bypassed in that artifact set.

### Current generated reports

The root of the repo currently contains:

- [`SATELLITE_REPORT_2026-04-23.md`](/home/yeet/PycharmProjects/PythonProject/SATELLITE_REPORT_2026-04-23.md)
- [`detailed satellite report llm version 2026-04-23.md`](/home/yeet/PycharmProjects/PythonProject/detailed%20satellite%20report%20llm%20version%202026-04-23.md)

Those files show the project was last run on `2026-04-23` and that the current cycle ended with:

- 0 active satellites
- a weekly SIP of ₹2,500
- a gross SIP recommendation of ₹0

## How The Main Pipeline Works

### 1. Inputs

The pipeline starts by loading:

- `watchlist.json`
- `satellites.json`
- cached Screener snapshots if present
- annual report summaries if present

It also reads environment variables such as:

- `SCREENER_SESSION_ID`
- `.env` values loaded through `python-dotenv`

### 2. Market data

For each ticker, `market_pipeline.py` downloads one year of price history from `yfinance` and derives:

- latest close
- 1-year return
- 3-month return
- max drawdown
- RSI-14
- 50-day moving average
- 200-day moving average
- 52-week high and low
- distance from 52-week high and low
- volume ratio versus 20-day average
- monthly return series

This gives the models both trend and momentum context.

### 3. News collection

The pipeline aggregates news from multiple sources:

- NSE announcements
- BSE announcements
- Moneycontrol RSS/article extraction
- `yfinance` news
- Economic Times RSS
- Business Standard RSS
- SearXNG search, if configured

The results are deduplicated by title and truncated to a manageable list for the prompts.

### 4. Screener.in fundamentals

The Screener extraction flow is layered:

1. Try the Screener JSON API if `SCREENER_SESSION_ID` is available.
2. Try Browser Use MCP if the JSON API fails or is unavailable.
3. Fall back to a cached local file in `screener_data/`.
4. Fall back to scraping the public HTML page.
5. If everything fails, return an empty scaffold.

Extracted sections include:

- key ratios
- quarterly results
- profit and loss
- balance sheet
- cash flows
- shareholding pattern
- peers

### 5. Three-stage LLM analysis

The core modeling loop is sequential and intentionally conservative.

#### Stage 1: Quality Screener

Model: `qwen3.6:35b`

Purpose:

- Decide whether the stock deserves a satellite slot.
- Compare fundamentals with recent trend behavior.
- Assign a score and verdict.
- Produce a concise risk list and thesis assumptions.

#### Stage 2: Thesis Writer

Model: `gpt-oss:20b`

Purpose:

- Write or update a 3-paragraph thesis.
- Explain the business, price action, and exit triggers.
- Compare the new story to the previous thesis and previous audit.

#### Stage 3: Thesis Auditor

Model: `gemma4:26b`

Purpose:

- Act as the skeptic.
- Check assumptions.
- Look for invalidation triggers.
- Recommend `ADD`, `HOLD`, `TRIM`, or `EXIT`.
- Produce a devil’s-advocate paragraph and red flags.

The models are run one at a time to keep local resource usage manageable.

### 6. SIP sizing

The weekly SIP logic is simple and conviction-based.

Current constants in the code:

- Monthly SIP target: ₹10,000
- Weekly SIP: ₹2,500
- Maximum satellites: 5
- Minimum satellites: 3
- Max allocation cap: 40% of satellite book

The current sizing engine:

- gives 1.5x weekly SIP to conviction above 8
- gives 1.0x weekly SIP to conviction between 5 and 8
- gives 0x to conviction below 5

Only active satellites are included in the SIP allocation calculation.

### 7. Persistent state update

After the analysis finishes, the pipeline updates `satellites.json` with:

- thesis history
- audit log entries
- current conviction
- SIP allocation metadata
- exit state if the decision is `EXIT`

Updates are written atomically so partial writes do not corrupt the file.

### 8. Markdown outputs

The pipeline writes two markdown reports:

- `SATELLITE_REPORT_YYYY-MM-DD.md`
- `detailed satellite report llm version YYYY-MM-DD.md`

The first is meant for quick human review and Claude synthesis.
The second is a deeper trace with prompts, inputs, structured outputs, and raw model text.

## What Each Generated File Means

### `satellites.json`

Persistent portfolio memory. It stores per-ticker state such as:

- status
- entry date
- average price
- allocation percentage
- original thesis
- thesis assumptions
- thesis history
- audit log
- total invested
- exit date
- exit reason

### `screener_data/TICKER.json`

Cached fundamentals extracted from Screener.in. This is the main structured source of company fundamentals for the pipeline.

### `annual_reports/processed/TICKER/`

Contains extracted annual-report artifacts:

- `raw/*.txt`
- `summaries/*.json`
- `index.json`

### `SATELLITE_REPORT_YYYY-MM-DD.md`

The user-facing weekly report. It includes:

- screening summary
- extracted fundamentals
- stage 1 score
- stage 2 thesis
- stage 3 audit
- red flags
- SIP recommendation

### `detailed satellite report llm version YYYY-MM-DD.md`

A full trace report. It includes:

- the exact prompts sent to each stage
- the inputs given to each model
- the structured outputs
- the raw model outputs
- the current satellite record snapshot

## CLI Commands Supported By `market_pipeline.py`

The main script supports these modes:

```bash
python market_pipeline.py
python market_pipeline.py --watchlist my_stocks.json
python market_pipeline.py --add "DIXON.NS:840:30"
python market_pipeline.py --exit "DIXON.NS"
python market_pipeline.py --screener "DIXON.NS"
python market_pipeline.py --browser-live
```

### `--add`

Adds a ticker to `satellites.json` as an active satellite using:

- ticker
- average price
- allocation percentage

### `--exit`

Marks a ticker as exited and records the exit date.

### `--screener`

Fetches and caches the Screener.in payload for one ticker without running the full weekly pipeline.

### `--watchlist`

Lets you swap the default watchlist JSON file for another one.

### `--browser-live`

Opens the Browser Use live preview URL when available.

## How The Annual Report Processor Works

`annual_report_processor.py` is a separate ingestion tool for PDF annual reports.

### Flow

1. Find report PDFs under `annual_reports/`.
2. Extract text page by page using `pypdf`.
3. Chunk pages into manageable text blocks.
4. Optionally summarize each chunk with local Ollama models.
5. Build per-report summary JSON files.
6. Build a combined `index.json` for the ticker.

### Important detail

The script supports a `--skip-llm` mode. The current `DIXON.NS` output indicates this mode was used, which is why the summaries are placeholders rather than synthesized report notes.

### Output structure

For each ticker, the processor writes:

- raw extracted text files
- report summaries
- a combined index file

This output is later read by `market_pipeline.py` through `load_annual_report_context()`.

## How Screener Verification Works

`verify_screener.py` is a sanity-check script.

It checks whether a Screener payload contains the expected structure for:

- key ratios
- quarterly results
- shareholding pattern

It tries the following sources in order:

1. Screener JSON API using `SCREENER_SESSION_ID`
2. Browser Use MCP
3. HTTP fallback

This script is helpful when you want to know whether the cached extraction is complete enough for analysis.

## Browser Use MCP Server

`run_mcp_server.py` spins up a local MCP server backed by Browser Use.

Why it exists:

- Screener.in can be easier to extract through a logged-in browser than through direct requests.
- The local pipeline can talk to this server instead of trying to automate browser logic itself.

The server:

- runs on `127.0.0.1:8765`
- uses Chrome in headless mode
- can reuse a local Chrome profile
- wraps Browser Use in an MCP-compatible HTTP interface

## Dependencies

The project currently depends on:

- `yfinance`
- `requests`
- `pandas`
- `numpy`
- `beautifulsoup4`
- `browser-use`
- `feedparser`
- `mcp`
- `langchain-ollama`
- `python-dotenv`
- `pypdf`

These dependencies reflect the project’s hybrid data-ingestion and local-LLM approach.

## Design Choices

### Sequential LLM execution

The models run one after another rather than in parallel. This is an intentional design choice to reduce memory pressure on a local machine.

### Atomic file writes

JSON and markdown artifacts are written atomically to reduce the chance of partial corruption.

### Cached fundamentals

Screener data is cached locally so the pipeline can keep working even if one source is down.

### Multi-source news

News is aggregated from multiple places because no single source is complete enough for thesis monitoring.

### Narrative plus structure

The project combines structured outputs with narrative reports. That makes it useful both for computation and for human reading.

## Limitations And Risks

This codebase is useful, but it has some clear limits:

- It depends on external websites and local services that can fail.
- It is only as good as the source data and prompt quality.
- The annual-report summaries are not guaranteed to be fresh unless you regenerate them.
- The model outputs are advisory, not deterministic truth.
- `satellites.json` currently has no active positions, so the portfolio memory path is not fully exercised right now.
- The current reports show placeholder annual-report synthesis for DIXON because `--skip-llm` was used in that generation pass.

## What "Good" Looks Like For This Project

The project is in a healthy state when:

- `watchlist.json` contains the securities you actually want reviewed.
- `screener_data/` has fresh cached payloads.
- `annual_reports/processed/` has meaningful summaries rather than placeholders.
- `satellites.json` contains active positions with thesis history and audit logs.
- Running `market_pipeline.py` produces both markdown reports without errors.
- The SIP recommendation aligns with the current conviction levels.

## Practical Summary

If you want the shortest possible description:

This is a local Indian equity research system that combines Screener.in fundamentals, price history, news, annual reports, and three sequential Ollama models to produce a weekly thesis review and SIP recommendation for a small satellite portfolio.

## Where The Project Stands Right Now

As of the current workspace snapshot:

- the codebase is complete enough to run end-to-end
- the pipeline is wired for watchlist research
- there are cached Screener snapshots and annual-report artifacts
- the portfolio store is empty
- the latest generated reports are dated `2026-04-23`
- the latest cycle ended with zero active satellites and no SIP allocation

So the project is not a blank scaffold. It is a working research workflow that currently sits in a watchlist-heavy, portfolio-light state.
