"""Core indexing logic for Whitebeard — no MCP dependency."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Generator
from dataclasses import dataclass, field
from itertools import chain
from pathlib import PurePath

from ouestcharlie_toolkit.backend import Backend
from ouestcharlie_toolkit.lance_index import (
    PHOTO_TABLE_NAME,
    LanceIndex,
)
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.partition_summary import compute_partition_summary
from ouestcharlie_toolkit.schema import (
    METADATA_DIR,
    SCHEMA_VERSION,
    PhotoEntry,
    RootSummary,
    ThumbnailChunk,
)
from ouestcharlie_toolkit.thumbnail_builder import (
    delete_partition_thumbnails,
    generate_partition_thumbnails,
)
from ouestcharlie_toolkit.xmp import XmpStore

_log = logging.getLogger(__name__)

# Maximum number of partitions indexed concurrently. Kept low because thumbnail
# generation is already multi-threaded internally.
_MAX_CONCURRENT_PARTITIONS = 4

# Maximum number of errors to report
_TOP_ERRORS = 10

# Photo file extensions indexed by Whitebeard (case-insensitive).
PHOTO_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".jpg",
        ".jpeg",
        ".heic",
        ".heif",
        ".png",
        ".dng",
        ".cr2",
        ".cr3",
        ".nef",
        ".arw",
        ".raf",
        ".orf",
        ".rw2",
    }
)


@dataclass
class IndexResult:
    """Result of indexing a single partition."""

    partition: str
    photos_processed: int = 0
    photos_skipped: int = 0  # photos already in manifest, carried over without re-processing
    photos_deleted: int = 0  # photos in previous manifest but no longer on disk
    sidecars_created: int = 0
    sidecars_skipped: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)
    thumbnails_rebuilt: bool = False
    duration_ms: int = 0


@dataclass
class LibraryIndexResult:
    """Result of indexing an entire photo library (all partitions)."""

    partitions: list[IndexResult] = field(default_factory=list)
    total_duration_ms: int = 0  # wall-clock time for the full library run
    partitions_deleted: int = 0  # stale partitions removed from summary.json

    @property
    def total_photos_processed(self) -> int:
        return sum(r.photos_processed for r in self.partitions)

    @property
    def total_photos_skipped(self) -> int:
        return sum(r.photos_skipped for r in self.partitions)

    @property
    def total_photos_deleted(self) -> int:
        return sum(r.photos_deleted for r in self.partitions)

    @property
    def total_sidecars_created(self) -> int:
        return sum(r.sidecars_created for r in self.partitions)

    @property
    def total_errors(self) -> int:
        return sum(r.errors for r in self.partitions)

    @property
    def total_thumbnails_rebuilt(self) -> int:
        return sum(1 for r in self.partitions if r.thumbnails_rebuilt)

    @property
    def top_error_details(self) -> Generator[str]:
        for _ in range(_TOP_ERRORS):
            yield from chain.from_iterable(r.error_details for r in self.partitions)


async def index_partition(
    backend: Backend,
    partition: str,
    force_extract_exif: bool = False,
    generate_thumbnails: bool = False,
    force_full_index: bool = False,
    lance_index: LanceIndex | None = None,
) -> IndexResult:
    """Index all photos in a partition (index mode — files stay in place).

    By default (``force_full_index=False``) runs in incremental mode: photos
    already present in the leaf manifest are carried over without re-processing.
    Only photos absent from the manifest (new arrivals) go through
    ``_extract_one``.  Photos present in the previous manifest but no longer on
    disk are counted, logged, and naturally removed from the updated manifest.

    With ``force_full_index=True`` all photos are re-processed regardless of
    the existing manifest, matching the previous unconditional behaviour.

    Photos in the existing manifest that are no longer on disk are detected
    and logged; they are removed from the updated manifest.

    Thumbnails are generated incrementally: a new AVIF chunk is appended for
    newly-processed photos only; existing chunks in the manifest are preserved.
    Use ``force_full_index=True`` to regenerate thumbnails from scratch.

    After processing all photos, creates or updates the leaf manifest for the
    partition (at ``<partition>/.ouestcharlie/manifest.json``).

    Args:
        backend: Backend to read/write.
        partition: Folder path relative to backend root (e.g. "" for root,
            "Vacations/Italy 2023/" for a subfolder). Trailing slash optional.
        force_extract_exif: If True, re-extract EXIF and overwrite existing
            XMP sidecars.  If False (default), existing sidecars are reused.
            Orthogonal to ``force_full_index``.
        generate_thumbnails: If True, generate the thumbnail AVIF container
            after indexing.  Requires the image-proc binary.
            Defaults to False; the MCP agent sets this to True.
            Preview JPEGs are generated lazily on-demand by Wally HTTP.
        force_full_index: If True, re-process all photos regardless of the
            existing manifest.  If False (default), photos already present in
            the manifest are carried over without calling ``_extract_one``.
        lance_index: Pre-opened LanceIndex to reuse.  When called from
            ``index_library`` this is the shared instance created once before
            any concurrent tasks run, preventing concurrent ``create_table``
            calls that cause Lance MVCC conflicts.  Pass ``None`` (default)
            when calling ``index_partition`` directly; it will open its own.

    Returns:
        IndexResult with counts of processed, skipped, deleted, created, and
        failed photos.
    """
    _t0 = time.monotonic()
    result = IndexResult(partition=partition)
    xmp_store = XmpStore(backend)
    manifest_store = ManifestStore(backend)
    if lance_index is None:
        lance_index = await LanceIndex.open_or_create(backend, PHOTO_TABLE_NAME)

    # List only direct-child photo files — read-only, no lock needed.
    photo_files = await backend.list_files(partition, PHOTO_EXTENSIONS)
    disk_filenames: set[str] = {PurePath(f.path).name for f in photo_files}

    # Hold the partition lock for the entire read → process → write cycle so
    # that no other agent can interleave writes on the same partition.
    summary = None
    async with backend.partition_lock(partition):
        # In incremental mode, load existing photo entries from LanceDB.
        existing_by_filename: dict[str, str] = {}
        deleted_filenames: set[str] | None = None
        if not force_full_index:
            existing_rows = await lance_index.get_partition_rows(
                partition, columns=["filename", "content_hash"]
            )
            existing_by_filename = {row["filename"]: row["content_hash"] for row in existing_rows}
            deleted_filenames = existing_by_filename.keys() - disk_filenames
            result.photos_deleted = len(deleted_filenames)
            if deleted_filenames:
                _log.info(
                    "Incremental index — %d photo(s) removed from disk since last index"
                    " — partition=%r: %s",
                    len(deleted_filenames),
                    partition,
                    ", ".join(sorted(deleted_filenames)),
                )

        photo_entries: list[PhotoEntry] = []

        for file_info in photo_files:
            filename = PurePath(file_info.path).name
            if force_full_index or filename not in existing_by_filename:
                result.photos_processed += 1
                try:
                    entry, created = await _extract_one(
                        xmp_store, file_info.path, force_extract_exif
                    )
                    photo_entries.append(entry)
                    if created:
                        result.sidecars_created += 1
                    else:
                        result.sidecars_skipped += 1
                except Exception as exc:
                    _log.error(
                        "Failed to process photo — partition=%r file=%r: %s",
                        partition,
                        filename,
                        exc,
                        exc_info=True,
                    )
                    result.errors += 1
                    result.error_details.append(f"{filename}: {exc}")
            else:
                result.photos_skipped += 1

        # Collect new entries for thumbnail purposes (photos not previously in the index).
        new_entries = [e for e in photo_entries if e.filename not in existing_by_filename]

        # Generate thumbnail AVIF container.
        # Thumbnails are content-addressed (write_new), so no lock conflict.
        # Full mode: regenerate for all photos.
        # Incremental mode: generate only for new photos; existing thumbnail data is
        # preserved per-photo inside lance_index.upsert_partition.
        thumbnail_lookup: dict[str, tuple[str, int]] = {}
        if generate_thumbnails:
            if force_full_index:
                await delete_partition_thumbnails(backend, partition)
                if photo_entries:
                    try:
                        new_chunks = await generate_partition_thumbnails(
                            backend, partition, photo_entries, tier="thumbnail"
                        )
                        result.thumbnails_rebuilt = True
                        thumbnail_lookup = _chunks_to_lookup(new_chunks)
                    except Exception as exc:
                        _log.error(
                            "Thumbnail generation failed — partition=%r: %s",
                            partition,
                            exc,
                            exc_info=True,
                        )
                        result.errors += 1
                        result.error_details.append(f"thumbnails: {exc}")
            elif new_entries:
                try:
                    new_chunks = await generate_partition_thumbnails(
                        backend, partition, new_entries, tier="thumbnail"
                    )
                    result.thumbnails_rebuilt = True
                    thumbnail_lookup = _chunks_to_lookup(new_chunks)
                except Exception as exc:
                    _log.error(
                        "Thumbnail generation failed — partition=%r: %s",
                        partition,
                        exc,
                        exc_info=True,
                    )
                    result.errors += 1
                    result.error_details.append(f"thumbnails: {exc}")

        # Upsert all photo rows for this partition.
        await lance_index.upsert_partition(partition, photo_entries, thumbnail_lookup or None)

        # Delete from index photos removed from disk.
        if deleted_filenames:
            deleted_hashes = [existing_by_filename[fn] for fn in deleted_filenames]
            await lance_index.delete(partition, deleted_hashes)

        summary = await compute_partition_summary(lance_index, partition)

    # Update the backend-wide summary.json — separate root lock inside upsert.
    if summary is not None:  # Prune empty partitions
        try:
            await manifest_store.upsert_partition_in_summary(summary)
        except Exception as exc:
            _log.error(
                "Failed to update summary.json — partition=%r: %s",
                partition,
                exc,
                exc_info=True,
            )
            result.errors += 1
            result.error_details.append(f"summary.json update: {exc}")

    result.duration_ms = round((time.monotonic() - _t0) * 1000)
    return result


async def index_library(
    backend: Backend,
    force_extract_exif: bool = False,
    generate_thumbnails: bool = False,
    force_full_index: bool = False,
    on_progress: Callable[[int, int, str, int, int], Awaitable[None]] | None = None,
) -> LibraryIndexResult:
    """Index all photos in a library.

    Walks all subdirectories under the backend root and indexes each folder
    that directly contains photos. Each ``index_partition`` call writes both
    the folder's ``manifest.json`` and updates the backend-wide ``summary.json``.
    After indexing, stale partitions (present in ``summary.json`` but no longer
    on disk) are removed from the summary and their metadata directories deleted.

    Args:
        backend: Backend to read/write.
        force_extract_exif: If True, re-extract EXIF and overwrite existing
            XMP sidecars.  Passed through to ``index_partition``.
        generate_thumbnails: If True, generate thumbnail AVIF containers for
            each partition.  Passed through to ``index_partition``.
        force_full_index: If True, re-process all photos in every partition
            regardless of existing manifests.  Passed through to
            ``index_partition``.

    Returns:
        LibraryIndexResult aggregating every per-partition IndexResult.
    """
    library_result = LibraryIndexResult()
    manifest_store = ManifestStore(backend)

    # If the existing index was built with an older schema version, force a full
    # reindex so all manifests and thumbnails are regenerated to the current schema.
    try:
        existing_summary, _ = await manifest_store.read_summary()
        if existing_summary.schema_version < SCHEMA_VERSION:
            _log.info(
                "index_library — schema version %d < %d, forcing full reindex",
                existing_summary.schema_version,
                SCHEMA_VERSION,
            )
            force_full_index = True
    except FileNotFoundError:
        pass  # No existing index — first run, nothing to upgrade.
    except Exception as exc:
        _log.warning("index_library — could not read summary.json for version check: %s", exc)

    # Walk the directory tree from the backend root via BFS, collecting all
    # partitions.  Hidden directories (names starting with ".") are skipped —
    # they are system or metadata folders, not user partitions.
    partitions: list[str] = []
    queue: list[str] = [""]
    while queue:
        current = queue.pop()
        partitions.append(current)
        for subdir in await backend.list_dirs(current):
            if not PurePath(subdir).name.startswith("."):
                queue.append(subdir)

    # Create the LanceDB index once before spawning concurrent tasks.  Each
    # concurrent call to index_partition would otherwise call create_table()
    # independently; on Lance 4.x that races an Overwrite transaction against
    # in-flight Update (merge_insert) transactions and raises an MVCC conflict.
    lance_index = await LanceIndex.open_or_create(backend, PHOTO_TABLE_NAME)

    # Index partitions in parallel, capped at _MAX_CONCURRENT_PARTITIONS workers.
    # Thumbnail generation is already multi-threaded internally, so a low cap
    # avoids over-saturating I/O while still hiding per-partition latency.
    total_partitions = len(partitions)
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_PARTITIONS)
    completed = 0

    async def _index_one(partition: str) -> IndexResult:
        nonlocal completed
        async with semaphore:
            result = await index_partition(
                backend,
                partition,
                force_extract_exif,
                generate_thumbnails=generate_thumbnails,
                force_full_index=force_full_index,
                lance_index=lance_index,
            )
        completed += 1
        if on_progress is not None:
            await on_progress(
                completed,
                total_partitions,
                partition,
                result.duration_ms,
                result.photos_processed + result.photos_skipped,
            )
        return result

    _t0 = time.monotonic()
    partition_index_res = await asyncio.gather(*(_index_one(p) for p in partitions))
    # Prune empty partitions
    library_result.partitions = list(
        p
        for p in partition_index_res
        if (p.photos_processed > 0 or p.photos_skipped > 0 or p.photos_deleted > 0 or p.errors > 0)
    )

    # Remove stale partitions (in summary.json but no longer on disk).
    indexed_paths = {r.partition for r in partition_index_res}
    library_result.partitions_deleted = await _prune_deleted_partitions(
        backend, manifest_store, lance_index, indexed_paths
    )

    await lance_index.maintain()

    library_result.total_duration_ms = round((time.monotonic() - _t0) * 1000)
    return library_result


# ---------------------------------------------------------------------------
# Internal helpers — single-file processing
# ---------------------------------------------------------------------------


async def _extract_one(
    xmp_store: XmpStore,
    photo_path: str,
    force_extract_exif: bool,
) -> tuple[PhotoEntry, bool]:
    """Process a single photo.

    Returns:
        (PhotoEntry, created) where created=True if a new sidecar was written.
    """
    sidecar, version, created = await xmp_store.read_or_create_from_picture(
        photo_path, force=force_extract_exif
    )
    filename = PurePath(photo_path).name
    entry = PhotoEntry.from_sidecar(
        filename, sidecar, sidecar.content_hash or "", str(version.value)
    )
    return entry, created


def _chunks_to_lookup(chunks: list[ThumbnailChunk]) -> dict[str, tuple[str, int]]:
    """Convert ThumbnailChunk list to content_hash → (avif_hash, tile_index) map."""
    lookup: dict[str, tuple[str, int]] = {}
    for chunk in chunks:
        for i, content_hash in enumerate(chunk.grid.photo_order):
            lookup[content_hash] = (chunk.avif_hash, i)
    return lookup


# ---------------------------------------------------------------------------
# Internal helpers — deleted partition cleanup
# ---------------------------------------------------------------------------


async def _prune_deleted_partitions(
    backend: Backend,
    manifest_store: ManifestStore,
    lance_index: LanceIndex,
    indexed_paths: set[str],
) -> int:
    """Remove stale partition entries from summary.json, LanceDB,

    Compares the set of discovered partitions against the existing ``summary.json``
    and removes any partition no longer present on disk.

    Returns:
        Number of partitions removed.
    """
    try:
        existing_summary, version = await manifest_store.read_summary()
    except FileNotFoundError:
        return 0

    stale = [p for p in existing_summary.partitions if p.path not in indexed_paths and p.path != ""]
    if not stale:
        return 0

    _log.info(
        "index_library — removing %d stale partition(s): %s",
        len(stale),
        ", ".join(sorted(p.path for p in stale)),
    )

    for p in stale:
        await _delete_partition_metadata(backend, p.path)
        await lance_index.delete_partition(p.path)

    pruned = RootSummary(
        schema_version=existing_summary.schema_version,
        partitions=[p for p in existing_summary.partitions if p.path in indexed_paths],
        _extra=existing_summary._extra,
    )
    try:
        async with backend.partition_lock(""):
            await manifest_store.write_summary(pruned, version)
    except Exception as exc:
        _log.error("Failed to prune summary.json: %s", exc, exc_info=True)

    return len(stale)


async def _delete_partition_metadata(backend: Backend, partition: str) -> None:
    """Delete the .ouestcharlie/<partition>/ metadata directory recursively.

    Safety guard: refuses to delete outside the metadata tree.
    """
    suffix = partition.rstrip("/") + "/" if partition else ""
    metadata_dir = f"{METADATA_DIR}/{suffix}"
    # Safety guard: never delete outside the metadata tree.
    if not metadata_dir.startswith(METADATA_DIR + "/"):
        raise ValueError(f"Refusing to delete outside metadata dir: {metadata_dir!r}")
    try:
        await backend.delete_dir(metadata_dir)
        _log.info("Deleted stale metadata directory: %s", metadata_dir)
    except FileNotFoundError:
        pass  # Already gone — nothing to do.
    except Exception as exc:
        _log.warning("Could not delete stale metadata directory %r: %s", metadata_dir, exc)
