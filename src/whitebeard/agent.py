"""Whitebeard MCP agent — photo indexer for local drives."""

from __future__ import annotations

import logging

from mcp.server.fastmcp import Context
from ouestcharlie_toolkit import report_progress
from ouestcharlie_toolkit.server import AgentBase

from .indexer import index_library, index_partition

_log = logging.getLogger(__name__)


class WhitebeardAgent(AgentBase):
    """Whitebeard: indexes an existing photo library in place.

    Receives ``WOOF_BACKEND_CONFIG`` from the environment (set by Woof before
    launching), exposes one MCP tool: ``index_partition``.
    """

    def __init__(self) -> None:
        super().__init__("whitebeard", version="0.1.0")
        self._register_tools()

    def _register_tools(self) -> None:
        mcp = self.mcp

        @mcp.tool(name="index_partition")
        async def index_partition_tool(
            ctx: Context,
            partition: str,
            force_extract_exif: bool = False,
            generate_thumbnails: bool = True,
            force_full_index: bool = False,
        ) -> dict:
            """Index all photos directly in a partition folder.

            By default runs in **incremental mode**: photos already present in
            the leaf manifest are carried over without re-processing.  Only new
            photos (absent from the manifest) are indexed.  Photos deleted from
            disk are counted, logged, and removed from the updated manifest.

            Scans ``partition`` for photo files (JPEG, HEIC, PNG, RAW).  For
            each new photo (or all photos when ``force_full_index=True``),
            extracts EXIF metadata and writes an XMP sidecar containing
            ``ouestcharlie:contentHash`` and all standard EXIF fields.  Creates
            or updates the leaf manifest at
            ``<partition>/.ouestcharlie/manifest.json``.

            Thumbnails: in incremental mode, a new AVIF chunk is generated for
            new photos only and appended to the existing chunks; existing AVIF
            files are immutable and never re-generated.

            Args:
                partition: Folder path relative to the backend root, e.g.
                    ``""`` for the root, ``"Vacations/Italy 2023/"`` for a
                    sub-folder.  Trailing slash is optional.
                force_extract_exif: Re-extract EXIF and overwrite existing
                    XMP sidecars.  Defaults to False.  Orthogonal to
                    ``force_full_index``.
                generate_thumbnails: Generate ``thumbnails.avif`` AVIF grids.
                    Defaults to True.
                force_full_index: Re-process all photos, even those already
                    present in the manifest.  Defaults to False (incremental).

            Returns:
                ``partition`` — echoed partition path.
                ``photosProcessed`` — photos indexed in this run (new or force-reindexed).
                ``photosSkipped`` — photos already in the manifest, carried over.
                ``photosDeleted`` — photos removed from disk since the last index.
                ``sidecarsCreated`` — XMP sidecars written (new or force-updated).
                ``sidecarsSkipped`` — photos whose existing sidecar was reused.
                ``thumbnailsRebuilt`` — true if a new AVIF chunk was generated.
                ``errors`` — count of photos that failed processing.
                ``errorDetails`` — list of per-photo error messages.
                ``durationMs`` — wall-clock time for this partition in milliseconds.
            """
            try:
                result = await index_partition(
                    self.backend,
                    partition,
                    force_extract_exif,
                    generate_thumbnails=generate_thumbnails,
                    force_full_index=force_full_index,
                )
            except Exception as exc:
                _log.error(
                    "index_partition failed — partition=%r: %s",
                    partition,
                    exc,
                    exc_info=True,
                )
                raise
            return {
                "partition": result.partition,
                "photosProcessed": result.photos_processed,
                "photosSkipped": result.photos_skipped,
                "photosDeleted": result.photos_deleted,
                "sidecarsCreated": result.sidecars_created,
                "sidecarsSkipped": result.sidecars_skipped,
                "thumbnailsRebuilt": result.thumbnails_rebuilt,
                "errors": result.errors,
                "errorDetails": result.error_details,
                "durationMs": result.duration_ms,
            }

        @mcp.tool(name="index_library")
        async def index_library_tool(
            ctx: Context,
            force_extract_exif: bool = False,
            generate_thumbnails: bool = True,
            force_full_index: bool = False,
        ) -> dict:
            """Recursively index all photos in the library and build manifests.

            By default runs in **incremental mode**: each partition is indexed
            incrementally (only new photos processed, deleted photos removed).
            Use ``force_full_index=True`` to re-process all photos across the
            entire library.

            Walks every subfolder under the backend root, indexes each folder
            that contains photos as a leaf partition (creating XMP sidecars and
            AVIF thumbnail grids), then builds parent manifests bottom-up so
            every ancestor folder has an aggregate manifest summarising its
            children.

            After indexing, partitions present in ``summary.json`` but no longer
            on disk are removed from the summary and their
            ``.ouestcharlie/<partition>/`` metadata directories are deleted.

            Args:
                force_extract_exif: Re-extract EXIF and overwrite existing
                    XMP sidecars.  Defaults to False.
                generate_thumbnails: Generate ``thumbnails.avif`` AVIF grids
                    for each partition.  Defaults to True.
                force_full_index: Re-process all photos in every partition,
                    even those already indexed.  Defaults to False (incremental).

            Returns:
                ``partitionsIndexed`` — number of leaf partitions processed.
                ``partitionsDeleted`` — stale partitions removed from summary.
                ``totalPhotos`` — photos indexed in this run (new or force-reindexed).
                ``totalPhotosSkipped`` — photos carried over from existing manifests.
                ``totalPhotosDeleted`` — photos removed from disk across all partitions.
                ``totalSidecarsCreated`` — XMP sidecars written.
                ``totalThumbnailsRebuilt`` — partitions where a new AVIF chunk was generated.
                ``totalErrors`` — count of photos that failed processing.
                ``errorDetails`` — list of per-photo error messages across all partitions.
                ``totalDurationMs`` — wall-clock time for the full library run in milliseconds.
            """

            async def _library_progress(
                current: int,
                total: int,
                name: str,
                duration_ms: int = 0,
                photos: int = 0,
            ) -> None:
                message = f"{name} — {photos} photos ({duration_ms}ms)" if duration_ms else name
                await report_progress(ctx, current, total, message)

            try:
                result = await index_library(
                    self.backend,
                    force_extract_exif=force_extract_exif,
                    generate_thumbnails=generate_thumbnails,
                    force_full_index=force_full_index,
                    on_progress=_library_progress,
                )
            except Exception as exc:
                _log.error("index_library failed: %s", exc, exc_info=True)
                raise
            return {
                "partitionsIndexed": len(result.partitions),
                "partitionsDeleted": result.partitions_deleted,
                "totalPhotos": result.total_photos,
                "totalPhotosSkipped": result.total_photos_skipped,
                "totalPhotosDeleted": result.total_photos_deleted,
                "totalSidecarsCreated": result.total_sidecars_created,
                "totalThumbnailsRebuilt": result.total_thumbnails_rebuilt,
                "totalErrors": result.total_errors,
                "errorDetails": list(result.error_details),
                "totalDurationMs": result.total_duration_ms,
            }
