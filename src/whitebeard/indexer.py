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
    def error_details(self) -> Generator[str]:
        yield from chain.from_iterable(r.error_details for r in self.partitions)


async def index_partition(
    backend: Backend,
    partition: str,
    force_extract_exif: bool = False,
    generate_thumbnails: bool = False,
) -> IndexResult:
    """Index all photos in a partition (index mode — files stay in place).

    For each photo directly in the partition:
    - If an XMP sidecar already exists and force_extract_exif=False, preserve
      it and include the photo in the manifest from the existing sidecar data.
    - Otherwise extract EXIF, write a new XMP sidecar, and add to the manifest.

    After processing all photos, creates or updates the leaf manifest for the
    partition (at ``<partition>/.ouestcharlie/manifest.json``).

    Eventually update the root summary

    Args:
        backend: Backend to read/write.
        partition: Folder path relative to backend root (e.g. "" for root,
            "Vacations/Italy 2023/" for a subfolder). Trailing slash optional.
        force_extract_exif: If True, re-extract EXIF and overwrite existing
            XMP sidecars.  If False (default), existing sidecars are reused.
        generate_thumbnails: If True, generate the thumbnail AVIF container
            after indexing.  Requires the image-proc binary.
            Defaults to False; the MCP agent sets this to True.
            Preview JPEGs are generated lazily on-demand by Wally HTTP.

    Returns:
        IndexResult with counts of processed, created, skipped, and failed photos.
    """
    _t0 = time.monotonic()
    result = IndexResult(partition=partition)
    xmp_store = XmpStore(backend)
    manifest_store = ManifestStore(backend)

    # List only direct-child photo files in the partition.
    photo_files = await backend.list_files(partition, PHOTO_EXTENSIONS)

    photo_entries: list[PhotoEntry] = []

    for file_info in photo_files:
        result.photos_processed += 1
        filename = PurePath(file_info.path).name
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

    # Generate thumbnail AVIF container
    thumbnail_result = None
    if generate_thumbnails and photo_entries:
        try:
            from ouestcharlie_toolkit.thumbnail_builder import (
                generate_partition_thumbnails,
            )

            thumbnail_result = await generate_partition_thumbnails(
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

    # Build or update the leaf manifest.
    summary = await _upsert_leaf_manifest(
        manifest_store, partition, photo_entries, thumbnail_result
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
            )
        completed += 1
        if on_progress is not None:
            await on_progress(
                completed,
                total_partitions,
                partition,
                result.duration_ms,
                result.photos_processed,
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
) -> ManifestSummary:
    """Create or update the leaf manifest for the partition.

    Args:
        thumbnail_chunks: List of chunks from ``generate_partition_thumbnails``,
            or ``None`` to preserve the existing value.

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
