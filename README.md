# Weekly Satellite Portfolio Auditor

A headless Python 3.11+ script that orchestrates **one browser-use MCP
extraction stage** plus **three local Ollama models** sequentially to produce a
Markdown audit for Claude Sonnet synthesis.

```
watchlist.json / satellites.json
  │
  ├─ yfinance ──────────── OHLCV + volume history
  ├─ Browser Use MCP ────── Authenticated Screener.in extraction
  ├─ SearXNG ────────────── Optional news context
  │
  ▼
┌──────────────────────────────────────────────────────┐
│ Step 1: Qwen 3.6 35B   — Quality screen             │
│ Step 2: GPT-OSS 20B     — 3-paragraph thesis        │   (strictly sequential)
│ Step 3: Gemma 4 26B     — Thesis audit / red flags   │
└──────────────────────────────────────────────────────┘
  │
  ├─ satellites.json              (atomic weekly state)
  └─ SATELLITE_REPORT_YYYY-MM-DD.md (Claude-ready markdown)
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| Python | 3.11+ |
| Ollama | Running locally at `localhost:11434` |
| Models pulled | `qwen3.6:35b`, `gpt-oss:20b`, `gemma4:26b` |
| Browser Use MCP | `BROWSER_USE_API_KEY` recommended for authenticated Screener.in extraction |
| SearXNG | Optional — running at `localhost:8080` |
| GPU | RTX 5070 Ti (or any 16 GB+ VRAM card) |

---

## Installation

```bash
# 1. Clone / copy files into a directory
cd ~/market_pipeline

# 2. Create a virtual environment
python3 -m venv .venv && source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Pull Ollama models (do this once)
ollama pull qwen3.6:35b
ollama pull gpt-oss:20b
ollama pull gemma4:26b
```

---

## Configuration

### watchlist.json
Edit `watchlist.json` to add/remove NSE tickers (use `.NS` suffix for Yahoo Finance):

```json
["HDFCBANK.NS", "ITC.NS", "RELIANCE.NS", "TCS.NS", "INFY.NS"]
```

### Model names
If you have different Ollama model tags installed, edit the constants at the top
of `market_pipeline.py`:

```python
MODEL_SCREENER = "qwen3.6:35b"   # Step 1
MODEL_THESIS   = "gpt-oss:20b"   # Step 2
MODEL_AUDITOR  = "gemma4:26b"    # Step 3
```

### Browser Use MCP
Set `BROWSER_USE_API_KEY` for authenticated Screener.in extraction. If you also
have a saved Browser Use profile, set `BROWSER_USE_PROFILE_ID` or place the ID
in `~/.chrome-browser-use/profile_id.txt`.

---

## Usage

```bash
# Default (watchlist.json + SearXNG at localhost:8080)
python market_pipeline.py

# Custom paths
python market_pipeline.py --watchlist my_stocks.json --searxng http://192.168.1.10:8080
```

### What happens
1. **Browser Use MCP** extracts Screener.in's Key Ratios, Shareholding Pattern,
  and Quarterly Results into `screener_data/TICKER.json`.
2. **yfinance** downloads 1 year of OHLCV and trend context.
3. The three Ollama models run **strictly one at a time** to respect 16 GB VRAM:
  - **Qwen 3.6 35B** → quality screen
  - **GPT-OSS 20B** → 3-paragraph thesis
  - **Gemma 4 26B** → skeptical audit and invalidation triggers
4. `satellites.json` is updated atomically with `thesis_history` and `audit_log`.
5. `SATELLITE_REPORT_YYYY-MM-DD.md` is written for downstream Claude synthesis.

---

## Output Files

### `SATELLITE_REPORT_YYYY-MM-DD.md`
Paste or upload to **Claude Sonnet**. The report contains:
- Screening summary
- Extracted fundamentals
- Stage 1 score
- Stage 2 thesis
- Stage 3 audit
- SIP recommendation
- Changes since last audit
- Key red flags and thesis invalidation triggers

### `satellites.json`
Atomic weekly portfolio state, including `thesis_history` and `audit_log` for each active satellite.

---

## SearXNG Setup (optional but recommended)

```bash
# Docker one-liner
docker run -d -p 8080:8080 \
  -e SEARXNG_SETTINGS_URL=http://localhost:8080 \
  --name searxng searxng/searxng
```

Without SearXNG, the pipeline runs in **offline mode** — sentiment analysis
falls back to the model's priors (still useful, just less current).

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `ollama: command not found` | Install from https://ollama.com |
| Model not found error | Run `ollama pull <model_name>` |
| OOM / CUDA out of memory | Keep the three model stages sequential; reduce `num_ctx` if needed |
| yfinance returns empty | Check ticker spelling (`.NS` for NSE, `.BO` for BSE) |
| SearXNG timeout | Pass `--searxng http://your-ip:8080` or leave offline |
| Screener extraction fails | Set `BROWSER_USE_API_KEY` and optionally `BROWSER_USE_PROFILE_ID`; fallback HTTP extraction is best-effort only |

---

## Architecture Notes

- **No parallelism** — models run one at a time to respect RTX 5070 Ti VRAM limits.
- **Explicit unload** — each Ollama stage uses `keep_alive=0s` plus `ollama stop` best effort.
- Browser Use MCP is the primary authenticated Screener.in path; direct HTTP parsing is only a fallback.
