"""Core indexing logic for Whitebeard — no MCP dependency."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Generator
from dataclasses import dataclass, field
from itertools import chain
from pathlib import PurePath

from ouestcharlie_toolkit.backend import Backend, VersionToken
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    SCHEMA_VERSION,
    LeafManifest,
    ManifestSummary,
    PhotoEntry,
    ThumbnailChunk,
)
from ouestcharlie_toolkit.xmp import XmpStore

_log = logging.getLogger(__name__)

# Maximum number of partitions indexed concurrently. Kept low because thumbnail
# generation is already multi-threaded internally.
_MAX_CONCURRENT_PARTITIONS = 4

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
    sidecars_created: int = 0
    sidecars_skipped: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)
    thumbnails_rebuilt: bool = False
    duration_ms: int = 0
    photos_skipped: int = 0  # photos already in manifest, carried over without re-processing
    photos_deleted: int = 0  # photos in previous manifest but no longer on disk


@dataclass
class LibraryIndexResult:
    """Result of indexing an entire photo library (all partitions)."""

    partitions: list[IndexResult] = field(default_factory=list)
    total_duration_ms: int = 0  # wall-clock time for the full library run

    @property
    def total_photos(self) -> int:
        return sum(r.photos_processed for r in self.partitions)

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
    def total_photos_skipped(self) -> int:
        return sum(r.photos_skipped for r in self.partitions)

    @property
    def total_photos_deleted(self) -> int:
        return sum(r.photos_deleted for r in self.partitions)

    @property
    def error_details(self) -> Generator[str]:
        yield from chain.from_iterable(r.error_details for r in self.partitions)


async def index_partition(
    backend: Backend,
    partition: str,
    force_extract_exif: bool = False,
    generate_thumbnails: bool = False,
    force_full_index: bool = False,
) -> IndexResult:
    """Index all photos in a partition (index mode — files stay in place).

    By default (``force_full_index=False``) runs in incremental mode: photos
    already present in the leaf manifest are carried over without re-processing.
    Only photos absent from the manifest (new arrivals) go through
    ``_extract_one``.  Photos present in the previous manifest but no longer on
    disk are counted, logged, and naturally removed from the updated manifest.

    With ``force_full_index=True`` all photos are re-processed regardless of
    the existing manifest, matching the previous unconditional behaviour.

    After processing all photos, creates or updates the leaf manifest for the
    partition (at ``<partition>/.ouestcharlie/manifest.json``).

    Eventually update the root summary

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

    Returns:
        IndexResult with counts of processed, skipped, deleted, created, and
        failed photos.
    """
    _t0 = time.monotonic()
    result = IndexResult(partition=partition)
    xmp_store = XmpStore(backend)
    manifest_store = ManifestStore(backend)

    # List only direct-child photo files in the partition.
    photo_files = await backend.list_files(partition, PHOTO_EXTENSIONS)
    disk_filenames: set[str] = {PurePath(f.path).name for f in photo_files}

    # In incremental mode, load the existing manifest to determine which photos
    # are already indexed.  In force mode, skip this read entirely.
    existing_by_filename: dict[str, PhotoEntry] = {}
    existing_manifest: LeafManifest | None = None
    existing_version: VersionToken | None = None
    if force_full_index:
        pass  # Re-process everything — no manifest read needed.
    else:
        try:
            existing_manifest, existing_version = await manifest_store.read_leaf(partition)
            existing_by_filename = {e.filename: e for e in existing_manifest.photos}
            # Count and log photos that have been deleted from disk since the last index.
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
        except FileNotFoundError:
            pass  # First run — no manifest yet, treat all photos as new.

    photo_entries: list[PhotoEntry] = []

    for file_info in photo_files:
        filename = PurePath(file_info.path).name
        if force_full_index or filename not in existing_by_filename:
            result.photos_processed += 1
            try:
                entry, created = await _extract_one(xmp_store, file_info.path, force_extract_exif)
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
            photo_entries.append(existing_by_filename[filename])
            result.photos_skipped += 1

    # Collect new entries for thumbnail purposes (photos not previously in manifest).
    new_entries = [e for e in photo_entries if e.filename not in existing_by_filename]

    # Generate thumbnail AVIF container.
    # Full mode: regenerate for all photos (replaces existing chunks).
    # Incremental mode: generate only for new photos and append to existing chunks.
    thumbnail_chunks_to_write: list[ThumbnailChunk] | None = None
    if generate_thumbnails:
        if force_full_index:
            if photo_entries:
                try:
                    from ouestcharlie_toolkit.thumbnail_builder import (
                        generate_partition_thumbnails,
                    )

                    thumbnail_chunks_to_write = await generate_partition_thumbnails(
                        backend, partition, photo_entries, tier="thumbnail"
                    )
                    result.thumbnails_rebuilt = True
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
            # Incremental: thumbnail only new photos, then append chunk to existing ones.
            try:
                from ouestcharlie_toolkit.thumbnail_builder import (
                    generate_partition_thumbnails,
                )

                new_chunks = await generate_partition_thumbnails(
                    backend, partition, new_entries, tier="thumbnail"
                )
                existing_chunks = existing_manifest.thumbnail_chunks if existing_manifest else []
                thumbnail_chunks_to_write = existing_chunks + new_chunks
                result.thumbnails_rebuilt = True
            except Exception as exc:
                _log.error(
                    "Thumbnail generation failed — partition=%r: %s",
                    partition,
                    exc,
                    exc_info=True,
                )
                result.errors += 1
                result.error_details.append(f"thumbnails: {exc}")
        # else: no new photos in incremental mode → pass None → preserve existing chunks

    # Build or update the leaf manifest.
    # Pass the already-read manifest and version token to avoid a second read_leaf call.
    prefetched = (
        (existing_manifest, existing_version)
        if existing_manifest is not None and existing_version is not None
        else None
    )
    summary = await _upsert_leaf_manifest(
        manifest_store, partition, photo_entries, thumbnail_chunks_to_write, prefetched
    )

    # Update the backend-wide summary.json with this partition's new summary.
    if summary is not None:
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
    root: str = "",
    force_extract_exif: bool = False,
    generate_thumbnails: bool = False,
    force_full_index: bool = False,
    on_progress: Callable[[int, int, str, int, int], Awaitable[None]] | None = None,
) -> LibraryIndexResult:
    """Index all photos in a library.

    Walks all subdirectories under ``root`` and indexes each folder that
    directly contains photos. Each ``index_partition`` call writes both the
    folder's ``manifest.json`` and updates the backend-wide ``summary.json``.

    Args:
        backend: Backend to read/write.
        root: Library root relative to the backend root (e.g. "" for the full
            backend, "Vacations/" to scope to a subfolder).
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

    # Walk the directory tree from root via BFS, collecting all partitions.
    # Hidden directories (names starting with ".") are skipped — they are
    # system or metadata folders, not user partitions.
    partitions: list[str] = []
    queue: list[str] = [root]
    while queue:
        current = queue.pop()
        partitions.append(current)
        for subdir in await backend.list_dirs(current):
            if not PurePath(subdir).name.startswith("."):
                queue.append(subdir)

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
    library_result.partitions = list(await asyncio.gather(*(_index_one(p) for p in partitions)))
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


async def _upsert_leaf_manifest(
    manifest_store: ManifestStore,
    partition: str,
    photo_entries: list[PhotoEntry],
    thumbnail_chunks: list[ThumbnailChunk] | None = None,
    prefetched: tuple[LeafManifest, VersionToken] | None = None,
) -> ManifestSummary:
    """Create or update the leaf manifest for the partition.

    Args:
        thumbnail_chunks: List of chunks from ``generate_partition_thumbnails``,
            or ``None`` to preserve the existing value.
        prefetched: Already-read ``(LeafManifest, VersionToken)`` from an earlier
            ``read_leaf`` call.  When provided, the read is skipped and the
            supplied version token is used for the write.

    Returns:
        The ManifestSummary written into the Root Summary.
    """
    summary = ManifestSummary.from_photos(partition, photo_entries)
    manifest = LeafManifest(
        schema_version=SCHEMA_VERSION,
        partition=partition,
        photos=photo_entries,
    )
    try:
        if prefetched is not None:
            existing, version = prefetched
        else:
            existing, version = await manifest_store.read_leaf(partition)
        manifest._extra = existing._extra  # preserve unknown fields
        if thumbnail_chunks is not None:
            manifest.thumbnail_chunks = thumbnail_chunks
        else:
            manifest.thumbnail_chunks = existing.thumbnail_chunks
        await manifest_store.write_leaf(manifest, version)
    except FileNotFoundError:
        if thumbnail_chunks is not None:
            manifest.thumbnail_chunks = thumbnail_chunks
        await manifest_store.create_leaf(manifest)
    return summary
