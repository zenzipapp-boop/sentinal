#!/usr/bin/env python3
"""Browser Use MCP server for the local market pipeline.

Note: This file is kept for backward compatibility. The market_pipeline.py
now uses browser_use as a direct Python library instead of this MCP server.
"""

import asyncio
import os
from pathlib import Path

from browser_use.mcp.server import main

OLLAMA_BASE = "http://localhost:11434"
OLLAMA_MODEL = "gemma4:latest"
DEFAULT_CHROME_USER_DATA_DIR = str(Path.home() / ".config/google-chrome-pipeline")
DEFAULT_CHROME_PROFILE_DIR = "PipelineBot"


async def run_with_config() -> None:
    """Run BrowserUseServer with custom configuration for the pipeline."""
    chrome_user_data_dir = Path(os.environ.get("CHROME_USER_DATA_DIR", DEFAULT_CHROME_USER_DATA_DIR)).expanduser()
    chrome_user_data_dir.mkdir(parents=True, exist_ok=True)
    chrome_profile_directory = os.environ.get("CHROME_PROFILE_DIR", DEFAULT_CHROME_PROFILE_DIR).strip() or DEFAULT_CHROME_PROFILE_DIR

    # Set browser configuration via environment for the BrowserUseServer to pick up
    os.environ["BROWSER_USE_CHROME_USER_DATA_DIR"] = str(chrome_user_data_dir)
    os.environ["BROWSER_USE_CHROME_PROFILE_DIR"] = chrome_profile_directory
    os.environ["BROWSER_USE_HEADLESS"] = "false"  # Visible browser
    os.environ["BROWSER_USE_CHANNEL"] = "chrome"

    # Set LLM configuration
    os.environ["LLM_MODEL"] = OLLAMA_MODEL
    os.environ["LLM_BASE_URL"] = OLLAMA_BASE

    # Run the standard BrowserUseServer (stdio-based MCP server)
    await main(session_timeout_minutes=10)


if __name__ == "__main__":
    asyncio.run(run_with_config())
