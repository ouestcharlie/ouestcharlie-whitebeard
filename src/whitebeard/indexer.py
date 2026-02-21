"""Core indexing logic for Whitebeard — no MCP dependency."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePath

from ouestcharlie_toolkit.backend import Backend
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    SCHEMA_VERSION,
    LeafManifest,
    PartitionSummary,
    PhotoEntry,
    XmpSidecar,
)
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


async def index_partition(
    backend: Backend,
    partition: str,
    force: bool = False,
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
            result.errors += 1
            result.error_details.append(f"{filename}: {exc}")

    # Build or update the leaf manifest.
    await _upsert_leaf_manifest(manifest_store, partition, photo_entries)

    return result


# ---------------------------------------------------------------------------
# Internal helpers
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
) -> PhotoEntry:
    """Convert an XmpSidecar to a PhotoEntry for the leaf manifest."""
    camera: str | None = None
    parts = [p for p in [sidecar.camera_make, sidecar.camera_model] if p]
    if parts:
        camera = " ".join(parts)
    return PhotoEntry(
        filename=filename,
        content_hash=content_hash,
        date_taken=sidecar.date_taken,
        camera=camera,
        gps=sidecar.gps,
        orientation=sidecar.orientation,
        tags=list(sidecar.tags),
        metadata_version=sidecar.metadata_version,
        xmp_version_token=xmp_version_token,
    )


def _compute_summary(partition: str, entries: list[PhotoEntry]) -> PartitionSummary:
    """Compute partition-level summary statistics from photo entries."""
    dates = [e.date_taken for e in entries if e.date_taken is not None]
    return PartitionSummary(
        path=partition,
        photo_count=len(entries),
        date_min=min(dates) if dates else None,
        date_max=max(dates) if dates else None,
    )


async def _upsert_leaf_manifest(
    manifest_store: ManifestStore,
    partition: str,
    photo_entries: list[PhotoEntry],
) -> None:
    """Create or update the leaf manifest for the partition."""
    summary = _compute_summary(partition, photo_entries)
    manifest = LeafManifest(
        schema_version=SCHEMA_VERSION,
        partition=partition,
        photos=photo_entries,
        summary=summary,
    )
    try:
        existing, version = await manifest_store.read_leaf(partition)
        manifest._extra = existing._extra  # preserve unknown fields
        await manifest_store.write_leaf(manifest, version)
    except FileNotFoundError:
        await manifest_store.create_leaf(manifest)
