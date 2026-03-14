"""Core indexing logic for Whitebeard — no MCP dependency."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import PurePath
from itertools import chain
from typing import Generator

_log = logging.getLogger(__name__)

from ouestcharlie_toolkit.backend import Backend
from ouestcharlie_toolkit.fields import PHOTO_FIELDS, FieldDef, FieldType
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    METADATA_DIR,
    SCHEMA_VERSION,
    LeafManifest,
    PartitionSummary,
    PhotoEntry,
    XmpSidecar,
)

# TYPE_CHECKING import for ThumbnailResult avoids circular imports at runtime.
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ouestcharlie_toolkit.thumbnail_builder import ThumbnailResult
from ouestcharlie_toolkit.xmp import XmpStore


# Photo file extensions indexed by Whitebeard (case-insensitive).
PHOTO_EXTENSIONS: frozenset[str] = frozenset({
    ".jpg", ".jpeg",
    ".heic", ".heif",
    ".png",
    ".dng", ".cr2", ".cr3", ".nef", ".arw", ".raf", ".orf", ".rw2",
})


@dataclass
class IndexResult:
    """Result of indexing a single partition."""

    partition: str
    photos_processed: int = 0
    sidecars_created: int = 0
    sidecars_skipped: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)
    summary: PartitionSummary | None = None
    thumbnails_rebuilt: bool = False


@dataclass
class LibraryIndexResult:
    """Result of indexing an entire photo library (all partitions)."""

    partitions: list[IndexResult] = field(default_factory=list)

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
    force: bool = False,
    generate_thumbnails: bool = False,
) -> IndexResult:
    """Index all photos in a partition (index mode — files stay in place).

    For each photo directly in the partition:
    - If an XMP sidecar already exists and force=False, preserve it and include
      the photo in the manifest from the existing sidecar data.
    - Otherwise extract EXIF, write a new XMP sidecar, and add to the manifest.

    After processing all photos, creates or updates the leaf manifest for the
    partition (at ``<partition>/.ouestcharlie/manifest.json``).

    Args:
        backend: Backend to read/write.
        partition: Folder path relative to backend root (e.g. "" for root,
            "Vacations/Italy 2023/" for a subfolder). Trailing slash optional.
        force: If True, re-extract EXIF and overwrite existing XMP sidecars.
        generate_thumbnails: If True, generate thumbnail and preview AVIF
            containers after indexing.  Requires the avif-grid binary.
            Defaults to False; the MCP agent sets this to True.

    Returns:
        IndexResult with counts of processed, created, skipped, and failed photos.
    """
    result = IndexResult(partition=partition)
    xmp_store = XmpStore(backend)
    manifest_store = ManifestStore(backend)

    # List all files under partition and filter to direct-child photo files.
    all_files = await backend.list_files(partition)
    photo_files = _filter_photo_files(all_files, partition)

    photo_entries: list[PhotoEntry] = []

    for file_info in photo_files:
        result.photos_processed += 1
        filename = PurePath(file_info.path).name
        try:
            entry, created = await _process_one(xmp_store, file_info.path, force)
            photo_entries.append(entry)
            if created:
                result.sidecars_created += 1
            else:
                result.sidecars_skipped += 1
        except Exception as exc:
            _log.error(
                "Failed to process photo — partition=%r file=%r: %s",
                partition, filename, exc, exc_info=True,
            )
            result.errors += 1
            result.error_details.append(f"{filename}: {exc}")

    # Generate thumbnail and preview AVIF containers.
    thumbnail_result = None
    if generate_thumbnails and photo_entries:
        try:
            from ouestcharlie_toolkit.thumbnail_builder import generate_partition_thumbnails
            thumbnail_result = await generate_partition_thumbnails(backend, partition, photo_entries)
            result.thumbnails_rebuilt = True
        except Exception as exc:
            _log.error(
                "Thumbnail generation failed — partition=%r: %s",
                partition, exc, exc_info=True,
            )
            result.errors += 1
            result.error_details.append(f"thumbnails: {exc}")

    # Build or update the leaf manifest.
    result.summary = await _upsert_leaf_manifest(
        manifest_store, partition, photo_entries, thumbnail_result
    )

    return result


async def index_library(
    backend: Backend,
    root: str = "",
    force: bool = False,
    generate_thumbnails: bool = False,
    on_progress: Callable[[int, int, str], Awaitable[None]] | None = None,
) -> LibraryIndexResult:
    """Recursively index all photos in a library and build hierarchical manifests.

    Walks all subdirectories under ``root``, indexes each folder that contains
    photos as a leaf partition, then builds parent manifests bottom-up so every
    ancestor folder has an aggregate manifest summarising its children.

    Args:
        backend: Backend to read/write.
        root: Library root relative to the backend root (e.g. "" for the full
            backend, "Vacations/" to scope to a subfolder).
        force: If True, re-extract EXIF and overwrite existing XMP sidecars.
        generate_thumbnails: If True, generate thumbnail AVIF containers for
            each partition.  Passed through to ``index_partition``.

    Returns:
        LibraryIndexResult aggregating every per-partition IndexResult.
    """
    library_result = LibraryIndexResult()
    manifest_store = ManifestStore(backend)

    # Discover all leaf partitions (directories directly containing photos).
    all_files = await backend.list_files(root)
    leaf_partitions = _discover_leaf_partitions(all_files, root)

    # Index each leaf partition, collecting summaries.
    leaf_summaries: dict[str, PartitionSummary] = {}
    sorted_partitions = sorted(leaf_partitions)
    total_partitions = len(sorted_partitions)
    for i, partition in enumerate(sorted_partitions):
        if on_progress is not None:
            await on_progress(i, total_partitions, partition)
        partition_result = await index_partition(
            backend, partition, force, generate_thumbnails=generate_thumbnails
        )
        library_result.partitions.append(partition_result)
        if partition_result.summary is not None:
            leaf_summaries[partition] = partition_result.summary

    # Build parent manifests bottom-up.
    if on_progress is not None:
        await on_progress(total_partitions, total_partitions, "building manifests")
    await _build_parent_manifests(manifest_store, leaf_summaries, root)

    return library_result


# ---------------------------------------------------------------------------
# Internal helpers — partition discovery
# ---------------------------------------------------------------------------


def _discover_leaf_partitions(files: list, root: str) -> set[str]:
    """Return the set of unique partition paths that directly contain photos.

    A partition is the immediate parent directory of a photo file.
    Paths inside metadata directories (METADATA_DIR) are excluded.
    """
    _meta_segment = f"{METADATA_DIR}/"
    leaf_partitions: set[str] = set()
    for f in files:
        if _meta_segment in f.path:
            continue
        if PurePath(f.path).suffix.lower() not in PHOTO_EXTENSIONS:
            continue
        slash_pos = f.path.rfind("/")
        if slash_pos == -1:
            # Photo at top level; partition is the root itself.
            parent = root
        else:
            parent = f.path[:slash_pos]
        leaf_partitions.add(parent)
    return leaf_partitions


def _direct_parent(partition: str) -> str:
    """Return the immediate parent of a partition path (empty string = root)."""
    if "/" not in partition:
        return ""
    return partition.rsplit("/", 1)[0]


# ---------------------------------------------------------------------------
# Internal helpers — parent manifest construction
# ---------------------------------------------------------------------------


async def _build_parent_manifests(
    manifest_store: ManifestStore,
    leaf_summaries: dict[str, PartitionSummary],
    library_root: str = "",
) -> None:
    """Build parent manifests from leaf summaries, bottom-up.

    For each ancestor folder of any leaf partition (up to ``library_root``),
    creates or updates a parent manifest whose children are the immediate
    sub-partitions at that level, with aggregated photo counts and date ranges.
    """
    if not leaf_summaries:
        return

    # all_summaries starts with leaves; parent summaries are added as computed.
    all_summaries: dict[str, PartitionSummary] = dict(leaf_summaries)

    # Collect all ancestor paths that need parent manifests.
    parent_paths: set[str] = set()
    for partition in leaf_summaries:
        current = partition
        while current != library_root:
            parent = _direct_parent(current)
            parent_paths.add(parent)
            if parent == library_root:
                break
            current = parent

    if not parent_paths:
        return

    # Process deepest paths first (most path components), root last.
    def _depth(p: str) -> int:
        return len(p.split("/")) if p else 0

    sorted_parents = sorted(parent_paths, key=_depth, reverse=True)
    # Ensure library_root is always last (may already be in the list).
    if library_root in sorted_parents:
        sorted_parents.remove(library_root)
    sorted_parents.append(library_root)

    for parent_path in sorted_parents:
        # Collect direct children of this parent from all known summaries.
        direct_children = [
            summary
            for path, summary in all_summaries.items()
            if _direct_parent(path) == parent_path and path != parent_path
        ]
        if not direct_children:
            continue

        # Compute and cache this parent's aggregate summary.
        all_summaries[parent_path] = _aggregate_summary(parent_path, direct_children)

        await manifest_store.rebuild_parent(parent_path, direct_children)


def _naive(dt: datetime) -> datetime:
    """Return a timezone-naive datetime for ordering.

    Strips tzinfo so that min()/max() can compare a mix of aware and naive
    datetimes without raising TypeError.  The original value (with or without
    tzinfo) is preserved in the PartitionSummary; only the key is stripped.
    """
    return dt.replace(tzinfo=None)


def _aggregate_summary(
    path: str,
    children: list[PartitionSummary],
    field_config: list[FieldDef] | None = None,
) -> PartitionSummary:
    """Aggregate child summaries into a single parent-level summary."""
    if field_config is None:
        field_config = PHOTO_FIELDS
    kwargs: dict = {
        "path": path,
        "photo_count": sum(c.photo_count for c in children),
    }
    for fdef in field_config:
        if fdef.summary_min_attr is None or fdef.summary_max_attr is None:
            continue
        mins  = [v for c in children if (v := getattr(c, fdef.summary_min_attr, None)) is not None]
        maxes = [v for c in children if (v := getattr(c, fdef.summary_max_attr, None)) is not None]
        if fdef.type == FieldType.DATE_RANGE:
            kwargs[fdef.summary_min_attr] = min(mins,  key=_naive) if mins  else None
            kwargs[fdef.summary_max_attr] = max(maxes, key=_naive) if maxes else None
        elif fdef.type == FieldType.INT_RANGE:
            kwargs[fdef.summary_min_attr] = min(mins)  if mins  else None
            kwargs[fdef.summary_max_attr] = max(maxes) if maxes else None
    return PartitionSummary(**kwargs)


# ---------------------------------------------------------------------------
# Internal helpers — single-file processing
# ---------------------------------------------------------------------------


def _filter_photo_files(
    files: list,  # list[FileInfo] — avoid importing FileInfo for type annotation
    partition: str,
) -> list:
    """Return only the photo FileInfo entries that are direct children of partition."""
    prefix = partition.rstrip("/") + "/" if partition else ""
    result = []
    for f in files:
        # Strip partition prefix to get the filename relative to the partition.
        rel = f.path[len(prefix):] if f.path.startswith(prefix) else f.path
        # Direct child: no directory separator in the relative part.
        if "/" in rel:
            continue
        if PurePath(rel).suffix.lower() in PHOTO_EXTENSIONS:
            result.append(f)
    return result


async def _process_one(
    xmp_store: XmpStore,
    photo_path: str,
    force: bool,
) -> tuple[PhotoEntry, bool]:
    """Process a single photo.

    Returns:
        (PhotoEntry, created) where created=True if a new sidecar was written.
    """
    sidecar, version, created = await xmp_store.read_or_create_from_picture(
        photo_path, force=force
    )
    filename = PurePath(photo_path).name
    entry = _sidecar_to_entry(filename, sidecar, sidecar.content_hash or "", str(version.value))
    return entry, created


def _sidecar_to_entry(
    filename: str,
    sidecar: XmpSidecar,
    content_hash: str,
    xmp_version_token: str,
    field_config: list[FieldDef] | None = None,
) -> PhotoEntry:
    """Convert an XmpSidecar to a PhotoEntry for the leaf manifest."""
    if field_config is None:
        field_config = PHOTO_FIELDS
    # Identity fields: no XmpSidecar equivalent, always supplied by caller
    kwargs: dict = {
        "filename": filename,
        "content_hash": content_hash,
        "metadata_version": sidecar.metadata_version,
        "xmp_version_token": xmp_version_token,
    }
    # All other fields driven by field config via sidecar_attr
    for fdef in field_config:
        if fdef.sidecar_attr is not None:
            val = getattr(sidecar, fdef.sidecar_attr, None)
            if fdef.type == FieldType.STRING_COLLECTION and val is not None:
                val = list(val)  # defensive copy (same as previous list(sidecar.tags))
            kwargs[fdef.entry_attr] = val
    return PhotoEntry(**kwargs)


def _compute_summary(
    partition: str,
    entries: list[PhotoEntry],
    field_config: list[FieldDef] | None = None,
) -> PartitionSummary:
    """Compute partition-level summary statistics from photo entries."""
    if field_config is None:
        field_config = PHOTO_FIELDS
    kwargs: dict = {"path": partition, "photo_count": len(entries)}
    for fdef in field_config:
        if fdef.summary_min_attr is None or fdef.summary_max_attr is None:
            continue
        values = [v for e in entries if (v := getattr(e, fdef.entry_attr, None)) is not None]
        if fdef.type == FieldType.DATE_RANGE:
            kwargs[fdef.summary_min_attr] = min(values, key=_naive) if values else None
            kwargs[fdef.summary_max_attr] = max(values, key=_naive) if values else None
        elif fdef.type == FieldType.INT_RANGE:
            kwargs[fdef.summary_min_attr] = min(values) if values else None
            kwargs[fdef.summary_max_attr] = max(values) if values else None
    return PartitionSummary(**kwargs)


async def _upsert_leaf_manifest(
    manifest_store: ManifestStore,
    partition: str,
    photo_entries: list[PhotoEntry],
    thumbnail_result: "ThumbnailResult | None" = None,
) -> PartitionSummary:
    """Create or update the leaf manifest for the partition.

    Returns:
        The PartitionSummary written into the manifest.
    """
    summary = _compute_summary(partition, photo_entries)
    manifest = LeafManifest(
        schema_version=SCHEMA_VERSION,
        partition=partition,
        photos=photo_entries,
        summary=summary,
    )
    if thumbnail_result is not None:
        manifest.thumbnails_hash = thumbnail_result.thumbnails_hash
        manifest.previews_hash = thumbnail_result.previews_hash
        manifest.thumbnail_grid = thumbnail_result.thumbnail_grid
        manifest.preview_grid = thumbnail_result.preview_grid
    try:
        existing, version = await manifest_store.read_leaf(partition)
        manifest._extra = existing._extra  # preserve unknown fields
        # Preserve existing thumbnail hashes if we didn't regenerate thumbnails.
        if thumbnail_result is None:
            manifest.thumbnails_hash = existing.thumbnails_hash
            manifest.previews_hash = existing.previews_hash
            manifest.thumbnail_grid = existing.thumbnail_grid
            manifest.preview_grid = existing.preview_grid
        await manifest_store.write_leaf(manifest, version)
    except FileNotFoundError:
        await manifest_store.create_leaf(manifest)
    return summary
