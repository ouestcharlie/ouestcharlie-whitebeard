# ouestcharlie-whitebeard — Claude Working Rules

## Testing

Never use `python`, `python3`, or `uv run pytest` — same reasons as py-toolkit.

## Key Design Rules

- `indexer.py` is **pure async logic** with no MCP dependency — easy to unit test.
- `agent.py` wraps `indexer.py` in `AgentBase` and registers it as an MCP tool.
- Index mode: photos stay in place; `.ouestcharlie/` subdirs are created alongside them.
- Partition = a folder relative to the backend root (e.g., `""` for root, `"Vacations/Italy 2023/"` for a subfolder).
- Only **direct children** of a partition are indexed — subdirectories are separate partitions.
- Existing XMP sidecars are preserved unless `force=True`.
- `ouestcharlie:contentHash` (SHA-256) is always written by `extract_exif()`.
