# Whitebeard Low-Level Design

## Role

Whitebeard is the **indexing agent** for OuEstCharlie. It operates in index mode: scan an existing local photo library in place (no files moved), create XMP sidecars containing EXIF metadata and the `ouestcharlie:contentHash`, upsert per-photo rows into the LanceDB columnar index, and update the backend-wide `summary.json`. It never reads or writes the gallery UI — that is Woof's responsibility.

## Module Structure

```
src/whitebeard/
  indexer.py   — pure async logic, no MCP dependency; independently testable
  agent.py     — WhitebeardAgent(AgentBase), registers MCP tools
  __main__.py  — entry point; sets up logging then runs the MCP server
```

`indexer.py` contains all business logic. `agent.py` is a thin wrapper that translates MCP tool calls into `indexer.py` function calls and formats the result dict.

## MCP Tools

### `index_partition(partition, force_extract_exif=False, generate_thumbnails=True, force_full_index=False)`

Indexes all photos directly in one folder (direct children only — subdirectories are separate partitions).

**Steps:**
1. Fetch existing LanceDB rows for the partition (`filename` + `content_hash` only) to build the incremental skip map.
2. List photo files on disk via `backend.list_files`.
3. Detect photos in the index but no longer on disk — log and schedule for deletion.
4. For each photo: if already indexed and `force_full_index=False`, skip (incremental). Otherwise read or create XMP sidecar. If `force_extract_exif=True`, re-extract and overwrite.
5. If `generate_thumbnails=True` and there are new photos, generate AVIF chunks for newly-processed photos only (or all when `force_full_index=True`).
6. Delete stale rows from the index; write all current entries in LanceIndex (preserving existing thumbnail data for unchanged photos).
7. Compute `ManifestSummary` using a DuckDB aggregate query over the LanceDB index.
8. Write the summary to the backend-wide `summary.json`.

**Returns:** `IndexResult` (photos processed, skipped, deleted, sidecars created/skipped, errors, duration).

### `index_library(force_extract_exif=False, generate_thumbnails=True, force_full_index=False)`

Indexes the entire library under the backend root.

**Steps:**
1. BFS-walk the full directory tree from `""`. Hidden directories (name starts with `.`) are skipped — they are metadata or system folders.
2. Dispatch collected partitions to `index_partition` in parallel, capped at `_MAX_CONCURRENT_PARTITIONS = 4` concurrent workers (via `asyncio.Semaphore`). The cap is kept low because thumbnail generation is already multi-threaded; going wider would over-saturate I/O.
3. Progress is reported after each partition completes (not while it is running).
4. After all partitions are indexed, compare the discovered partition set against `summary.json`. Remove stale entries from `summary.json` and delete their rows from the LanceDB index via `LanceIndex.delete_partition`, then delete their per-partition `.ouestcharlie/<partition>/` directories (thumbnails) via `backend.delete_dir()`.

**Returns:** `LibraryIndexResult` aggregating all per-partition `IndexResult` values plus `partitions_deleted`.

## Incremental Indexing

Whitebeard defaults to incremental mode — photos already present in the LanceDB index are skipped without re-reading their XMP sidecars or re-extracting EXIF. Only new photos (filenames not in the index) are processed.

- **Deleted photos**: photos in the LanceDB index but not on disk are deleted and logged at INFO level.
- **Deleted partitions**: partitions in `summary.json` but no longer on disk are removed from the summary, their LanceDB rows deleted, and their `.ouestcharlie/<partition>/` directories deleted after the gather step in `index_library`.
- **EXIF changes**: changes to EXIF fields in an already-indexed photo are NOT detected in incremental mode. Use `force_extract_exif=True` together with `force_full_index=True` to refresh all metadata.
- **Thumbnail strategy**: new AVIF chunks are generated only for newly-processed photos. `LanceIndex.upsert_partition` preserves existing thumbnail data for photos not included in the new thumbnail generation pass. `force_full_index=True` re-generates all thumbnail chunks for the partition.

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
  │
  └── _prune_deleted_partitions (sequential, after gather)
```

Each `index_partition` call is independent: it upserts its rows into the shared LanceDB table (which handles concurrent writes natively) and then calls `upsert_partition_in_summary` to update `summary.json`. The latter uses optimistic concurrency (read-modify-write with up to 5 retries on version conflict) — the only observable effect of parallelism is more frequent retries under high partition counts.

`LibraryIndexResult.partitions` preserves the BFS discovery order (same order as the input `partitions` list), because `asyncio.gather` returns results in submission order.

## LanceDB Write

`LanceIndex.upsert_partition` receives the final `list[PhotoEntry]` and a `thumbnail_lookup` for newly-generated chunks. It pre-queries existing thumbnail data so that photos absent from the lookup retain their existing AVIF reference — preventing incremental runs from wiping thumbnails. Rows are merged on `content_hash` (update if matched, insert if not). Stale photo deletion is handled separately by the caller before this step.

After the upsert, `compute_partition_summary` runs aggregates query over the index to produce the `ManifestSummary` (photo count, date range, GPS bbox, rating range), which is then written to `summary.json`.

## XMP Sidecar Handling

Delegated entirely to `XmpStore.read_or_create_from_picture` from `ouestcharlie_toolkit`. Whitebeard never reads XMP files directly — it receives a `(XmpSidecar, VersionToken, created)` tuple and converts it to a `PhotoEntry` via `PhotoEntry.from_sidecar`.

## Deleted Partition Cleanup (`_prune_deleted_partitions`)

Called by `index_library` after the gather step. Compares the BFS-discovered partition set against the existing `summary.json`:

1. Reads `summary.json` (returns 0 immediately if not found).
2. Identifies stale partitions (in summary but not discovered by BFS).
3. For each stale partition, calls `_delete_partition_metadata` which delegates to `backend.delete_dir()`.
4. Writes the pruned `summary.json` via `write_summary`.

`backend.delete_dir()` uses `shutil.rmtree` with an `onexc` callback (Python 3.12+): locked or open files are logged at WARNING and skipped rather than aborting the whole tree. Summary pruning happens regardless of deletion success — a partial cleanup is acceptable; the next library run will retry remaining files.

A safety guard in `_delete_partition_metadata` verifies the computed metadata path starts with `.ouestcharlie/` before deletion.

## Error Isolation

Per-photo errors are caught and recorded in `IndexResult.error_details`; they never abort the partition. Thumbnail and manifest errors are similarly caught and recorded. `index_library` has no additional isolation — a raised exception from `index_partition` would propagate through `asyncio.gather` and fail the whole library run (which is intentional: manifest corruption should surface loudly).

## Logging

`setup_logging("whitebeard", log_file_env_var="WHITEBEARD_LOG_FILE")` is called in `__main__.py` before any agent code is imported. All logs go to `~/Library/Logs/ouestcharlie/whitebeard.log` on macOS (shared `ouestcharlie/` folder, one log file per agent). Override with the `WHITEBEARD_LOG_FILE` environment variable.

Per MCP convention: exceptions in tool handlers are always logged with `exc_info=True` before re-raising, because FastMCP swallows unhandled errors silently on the stdio transport.

## References

- [HLD.md](../ouestcharlie/HLD.md) — system architecture, agent role
- [py_toolkit_LLD.md](../ouestcharlie-py-toolkit/py_toolkit_LLD.md) — `XmpStore`, `ManifestStore`, `Backend`, thumbnail builder
- [agent_LLD_rationale.md](../ouestcharlie/agent/agent_LLD_rationale.md) — agent design decisions
