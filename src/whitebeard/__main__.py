"""Entry point: python -m whitebeard starts the MCP server on stdio."""

from .agent import WhitebeardAgent

WhitebeardAgent().run()
