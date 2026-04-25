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

### `index_partition(partition, force_extract_exif=False, generate_thumbnails=True, force_full_index=False)`

Indexes all photos directly in one folder (direct children only — subdirectories are separate partitions).

**Steps:**
1. Read existing manifest (if any) to build `existing_by_filename` dict for incremental mode.
2. List photo files in `partition` via `backend.list_files(partition, PHOTO_EXTENSIONS)`.
3. Detect photos present in the existing manifest but no longer on disk — log them and exclude from the updated manifest.
4. For each photo: if already in the manifest and `force_full_index=False`, skip it (incremental). Otherwise read or create XMP sidecar (`XmpStore.read_or_create_from_picture`). If `force_extract_exif=True`, re-extract and overwrite.
5. If `generate_thumbnails=True` and there are new photos, call `generate_partition_thumbnails` with only the newly-processed photos (or all photos when `force_full_index=True`). This is multi-threaded internally.
6. Create or update the leaf manifest at `<partition>/.ouestcharlie/manifest.json` (`_upsert_leaf_manifest`). The already-read manifest and version token are passed as `prefetched` to avoid a second backend read.
7. Atomically update the backend-wide `summary.json` (`ManifestStore.upsert_partition_in_summary`).

**Returns:** `IndexResult` (photos processed, skipped, deleted, sidecars created/skipped, errors, duration).

### `index_library(force_extract_exif=False, generate_thumbnails=True, force_full_index=False)`

Indexes the entire library under the backend root.

**Steps:**
1. BFS-walk the full directory tree from `""`. Hidden directories (name starts with `.`) are skipped — they are metadata or system folders.
2. Dispatch collected partitions to `index_partition` in parallel, capped at `_MAX_CONCURRENT_PARTITIONS = 4` concurrent workers (via `asyncio.Semaphore`). The cap is kept low because thumbnail generation is already multi-threaded; going wider would over-saturate I/O.
3. Progress is reported after each partition completes (not while it is running).
4. After all partitions are indexed, compare the discovered partition set against `summary.json`. Remove stale entries (partitions no longer on disk) from `summary.json` and delete their `.ouestcharlie/<partition>/` metadata directories via `backend.delete_dir()`.

**Returns:** `LibraryIndexResult` aggregating all per-partition `IndexResult` values plus `partitions_deleted`.

## Incremental Indexing

Whitebeard defaults to incremental mode — photos already present in the existing leaf manifest are skipped without re-reading their XMP sidecars or re-extracting EXIF. Only new photos (filenames not in the manifest) are processed.

- **Deleted photos**: photos in the manifest but not on disk are logged at INFO level and excluded from the updated manifest. Their XMP sidecars are left in place.
- **Deleted partitions**: partitions in `summary.json` but no longer on disk are removed from the summary and their `.ouestcharlie/<partition>/` metadata directories are deleted after the gather step in `index_library`.
- **EXIF changes**: changes to EXIF fields in an already-indexed photo are NOT detected in incremental mode. Use `force_extract_exif=True` together with `force_full_index=True` to refresh all metadata.
- **Thumbnail strategy**: new AVIF chunks are generated only for newly-processed photos and appended to the existing chunk list. `force_full_index=True` replaces the chunk list entirely.

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

Each `index_partition` call is independent: it writes its own `manifest.json` and then calls `upsert_partition_in_summary` to update the shared `summary.json`. The latter uses optimistic concurrency (read-modify-write with up to 5 retries on version conflict), so concurrent writes are safe — the only observable effect of parallelism is more frequent retries under high partition counts.

`LibraryIndexResult.partitions` preserves the BFS discovery order (same order as the input `partitions` list), because `asyncio.gather` returns results in submission order.

## Incremental Indexing (Detail)

By default both `index_partition` and `index_library` run in **incremental mode** (`force_full_index=False`).

At the start of each `index_partition` call, the existing leaf manifest (if any) is read and a `filename → PhotoEntry` dict is built.  For each photo file found on disk:

- **Already in manifest** → the existing `PhotoEntry` is reused without calling `_extract_one`.  No sidecar I/O, no EXIF read.  Counted in `IndexResult.photos_skipped`.
- **Not in manifest** → `_extract_one` is called as usual.  Counted in `IndexResult.photos_processed`.
- **In manifest but not on disk** → absent from the merged list; the manifest is updated without that entry (stale entries are garbage-collected).  Counted in `IndexResult.photos_deleted` and logged at INFO level.

The first run against a partition with no existing manifest behaves identically to `force_full_index=True` — all photos are processed.

**Thumbnail strategy in incremental mode:** only new photos (those not previously in the manifest) are passed to `generate_partition_thumbnails`.  The resulting new AVIF chunk is appended to the existing `thumbnail_chunks` list in the manifest.  Existing AVIF files are immutable (content-addressed) and are never modified or deleted.  If no new photos are present, thumbnails are not regenerated.

**`force_full_index=True`** skips the manifest read, re-processes every photo, and regenerates thumbnails for the full photo set (replacing existing chunks).  This is equivalent to the previous unconditional behaviour.

**`force_full_index` and `force_extract_exif` are orthogonal.**  Using `force_extract_exif=True` alone does not trigger re-indexing of already-manifest photos in incremental mode — the photo is carried over as-is.  To regenerate both the manifest entry and the XMP sidecar, use both flags together.

**Known limitation:** changes to a photo's EXIF data or XMP sidecar after the initial index are **not detected** in incremental mode.  The photo is already in the manifest and is carried over without re-reading its metadata.  Use `force_full_index=True` (and optionally `force_extract_exif=True`) to pick up metadata changes.

## Leaf Manifest Upsert (`_upsert_leaf_manifest`)

- Computes `ManifestSummary.from_photos()` from the current photo entries.
- Accepts a `prefetched: tuple[LeafManifest, VersionToken] | None` parameter. When provided (as it always is in `index_partition` after the incremental pre-scan), skips the redundant `read_leaf` call.
- Reads the existing manifest if one exists and `prefetched` is `None`, to preserve unknown fields (`_extra`) and existing thumbnail chunks.
- Thumbnail chunk merging: new chunks are computed and merged in `index_partition` before calling this helper; `_upsert_leaf_manifest` receives the final `thumbnail_chunks` list directly.
- Writes back with optimistic concurrency (`write_leaf` on existing, `create_leaf` on first index).

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
