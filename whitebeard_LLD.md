# Whitebeard Low-Level Design

## Role

Whitebeard is the **indexing agent** for OuEstCharlie. It operates in index mode: scan an existing local photo library in place (no files moved), create XMP sidecars containing EXIF metadata and the `ouestcharlie:contentHash`, and write leaf manifests with per-partition summary statistics. It never reads or writes the gallery UI — that is Woof's responsibility.

## Module Structure

```
src/whitebeard/
  indexer.py   — pure async logic, no MCP dependency; independently testable
  agent.py     — WhitebeardAgent(AgentBase), registers MCP tools
  __main__.py  — entry point; sets up logging then runs the MCP server
```

`indexer.py` contains all business logic. `agent.py` is a thin wrapper that translates MCP tool calls into `indexer.py` function calls and formats the result dict.

## MCP Tools

### `index_partition(partition, force_extract_exif=False, generate_thumbnails=True)`

Indexes all photos directly in one folder (direct children only — subdirectories are separate partitions).

**Steps:**
1. List photo files in `partition` via `backend.list_files(partition, PHOTO_EXTENSIONS)`.
2. For each photo: read or create XMP sidecar (`XmpStore.read_or_create_from_picture`). If `force_extract_exif=True`, re-extract and overwrite.
3. If `generate_thumbnails=True`, call `generate_partition_thumbnails` to build the AVIF grid (256 px tiles). This is multi-threaded internally.
4. Create or update the leaf manifest at `<partition>/.ouestcharlie/manifest.json` (`_upsert_leaf_manifest`).
5. Atomically update the backend-wide `summary.json` (`ManifestStore.upsert_partition_in_summary`).

**Returns:** `IndexResult` (photos processed, sidecars created/skipped, errors, duration).

### `index_library(root="", force_extract_exif=False, generate_thumbnails=True)`

Recursively indexes the entire library under `root`.

**Steps:**
1. BFS-walk the directory tree from `root`. Hidden directories (name starts with `.`) are skipped — they are metadata or system folders.
2. Dispatch collected partitions to `index_partition` in parallel, capped at `_MAX_CONCURRENT_PARTITIONS = 4` concurrent workers (via `asyncio.Semaphore`). The cap is kept low because thumbnail generation is already multi-threaded; going wider would over-saturate I/O.
3. Progress is reported after each partition completes (not while it is running).

**Returns:** `LibraryIndexResult` aggregating all per-partition `IndexResult` values.

## Concurrency Model

```
index_library
  │
  ├── asyncio.gather (all partitions)
  │     │
  │     ├── Semaphore(4) → index_partition("2024/Jan/")
  │     ├── Semaphore(4) → index_partition("2024/Feb/")
  │     ├── Semaphore(4) → index_partition("2024/Mar/")  ← up to 4 at once
  │     ├── Semaphore(4) → index_partition("2024/Apr/")
  │     │     (queued)
  │     └── ...
```

Each `index_partition` call is independent: it writes its own `manifest.json` and then calls `upsert_partition_in_summary` to update the shared `summary.json`. The latter uses optimistic concurrency (read-modify-write with up to 5 retries on version conflict), so concurrent writes are safe — the only observable effect of parallelism is more frequent retries under high partition counts.

`LibraryIndexResult.partitions` preserves the BFS discovery order (same order as the input `partitions` list), because `asyncio.gather` returns results in submission order.

## Leaf Manifest Upsert (`_upsert_leaf_manifest`)

- Computes `ManifestSummary.from_photos()` from the current photo entries.
- Reads the existing manifest if one exists, to preserve unknown fields (`_extra`) and existing thumbnail chunks.
- Writes back with optimistic concurrency (`write_leaf` on existing, `create_leaf` on first index).
- If `thumbnail_chunks` is provided, replaces the stored chunks; otherwise preserves the existing ones.

## XMP Sidecar Handling

Delegated entirely to `XmpStore.read_or_create_from_picture` from `ouestcharlie_toolkit`. Whitebeard never reads XMP files directly — it receives a `(XmpSidecar, VersionToken, created)` tuple and converts it to a `PhotoEntry` via `PhotoEntry.from_sidecar`.

## Error Isolation

Per-photo errors are caught and recorded in `IndexResult.error_details`; they never abort the partition. Thumbnail and manifest errors are similarly caught and recorded. `index_library` has no additional isolation — a raised exception from `index_partition` would propagate through `asyncio.gather` and fail the whole library run (which is intentional: manifest corruption should surface loudly).

## Logging

`setup_logging("whitebeard", log_file_env_var="WHITEBEARD_LOG_FILE")` is called in `__main__.py` before any agent code is imported. All logs go to `~/Library/Logs/ouestcharlie/whitebeard.log` on macOS (shared `ouestcharlie/` folder, one log file per agent). Override with the `WHITEBEARD_LOG_FILE` environment variable.

Per MCP convention: exceptions in tool handlers are always logged with `exc_info=True` before re-raising, because FastMCP swallows unhandled errors silently on the stdio transport.

## References

- [HLD.md](../ouestcharlie/HLD.md) — system architecture, agent role
- [py_toolkit_LLD.md](../ouestcharlie-py-toolkit/py_toolkit_LLD.md) — `XmpStore`, `ManifestStore`, `Backend`, thumbnail builder
- [agent_LLD_rationale.md](../ouestcharlie/agent/agent_LLD_rationale.md) — agent design decisions