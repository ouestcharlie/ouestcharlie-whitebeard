# ouestcharlie-whitebeard — Claude Working Rules

## Testing

Never use `python`, `python3`, or `uv run pytest` — same reasons as py-toolkit.

## MCP Error Handling

- **Always log exceptions** in MCP tool handlers — FastMCP swallows unhandled errors silently, so without explicit logging they are invisible.
- Wrap `ctx.report_progress()` calls in `try/except` and log failures at DEBUG level. The MCP client may disconnect or time out while the tool is still running; a failed progress notification must never abort the operation.
- For long-running tools, the MCP Inspector timeout must be increased in its settings (default is too low for full-library indexing).

## Key Design Rules

- `indexer.py` is **pure async logic** with no MCP dependency — easy to unit test.
- `agent.py` wraps `indexer.py` in `AgentBase` and registers it as an MCP tool.
- Index mode: photos stay in place; `.ouestcharlie/` subdirs are created alongside them.
- Partition = a folder relative to the backend root (e.g., `""` for root, `"Vacations/Italy 2023/"` for a subfolder).
- Only **direct children** of a partition are indexed — subdirectories are separate partitions.
- Existing XMP sidecars are preserved unless `force=True`.
- `ouestcharlie:contentHash` (SHA-256) is always written by `extract_exif()`.
