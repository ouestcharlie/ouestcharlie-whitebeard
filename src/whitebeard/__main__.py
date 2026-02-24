"""Entry point for the Whitebeard MCP agent.

For mcp dev / MCP Inspector:
    WOOF_BACKEND_CONFIG='{"type":"filesystem","root":"/path/to/photos"}' \\
        mcp dev src/whitebeard/__main__.py

For production (stdio transport):
    python -m whitebeard   or   whitebeard

Logs are written to the platform log directory by default.
Override with WHITEBEARD_LOG_FILE=/path/to/file.log.
"""

from __future__ import annotations

import logging

from ouestcharlie_toolkit import setup_logging

# Set up logging before importing agent code (which may trigger library imports).
_log_file = setup_logging("whitebeard", log_file_env_var="WHITEBEARD_LOG_FILE")
logging.getLogger(__name__).info("Whitebeard starting — log: %s", _log_file)

from whitebeard.agent import WhitebeardAgent  # noqa: E402

_agent = WhitebeardAgent()
mcp = _agent.mcp  # module-level name required by `mcp dev`


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
