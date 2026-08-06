"""Whitebeard MCP agent — photo indexer for local drives."""

from __future__ import annotations

import logging

from mcp.server.fastmcp import Context
from ouestcharlie_toolkit.server import AgentBase

from .indexer import index_library, index_partition_scope

_log = logging.getLogger(__name__)


class WhitebeardAgent(AgentBase):
    """Whitebeard: indexes an existing photo library in place.

    Receives ``WOOF_BACKEND_CONFIG`` from the environment (set by Woof before
    launching), exposes MCP tools ``index_library`` and
    ``index_partition_scope``.
    """

    def __init__(self) -> None:
        super().__init__("whitebeard", version="0.1.0")
        self._register_tools()

    def _register_tools(self) -> None:
        mcp = self.mcp

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

            After indexing, previously-indexed partitions no longer on disk
            are removed from the LanceDB index and their
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
                try:
                    await ctx.report_progress(progress=current, total=total, message=message)
                except Exception as exc:
                    _log.debug(
                        "Progress notification failed (client may have disconnected): %s", exc
                    )

            try:
                result = await index_library(
                    self.backend,
                    force_extract_exif=force_extract_exif,
                    generate_thumbnails=generate_thumbnails,
                    force_full_index=force_full_index,
                    on_progress=_library_progress,
                    lance_index_path=self.lance_index_path_override,
                )
            except Exception as exc:
                # TaskGroup wraps partition failures in an ExceptionGroup — unwrap the first.
                cause = exc.exceptions[0] if isinstance(exc, BaseExceptionGroup) else exc
                _log.error("index_library failed: %s", cause, exc_info=cause)
                raise cause from exc
            return {
                "partitionsIndexed": len(result.partitions),
                "partitionsDeleted": result.partitions_deleted,
                "totalPhotosProcessed": result.total_photos_processed,
                "totalPhotosSkipped": result.total_photos_skipped,
                "totalPhotosDeleted": result.total_photos_deleted,
                "totalSidecarsCreated": result.total_sidecars_created,
                "totalThumbnailsRebuilt": result.total_thumbnails_rebuilt,
                "totalErrors": result.total_errors,
                "topErrorDetails": list(result.top_error_details),
                "totalDurationMs": result.total_duration_ms,
            }

        @mcp.tool(name="index_partition_scope")
        async def index_partition_scope_tool(
            ctx: Context,
            partition_scope: list[str],
            force_extract_exif: bool = False,
            generate_thumbnails: bool = True,
            force_full_index: bool = False,
        ) -> dict:
            """Index an explicit list of partition folders.

            By default runs in **incremental mode**: each partition is indexed
            incrementally (only new photos processed, deleted photos removed).
            Use ``force_full_index=True`` to re-process all photos in every
            listed partition.

            Each entry in ``partition_scope`` is indexed independently as a
            leaf partition (direct-child photos only, no descendants — same
            semantics as ``index_partition``). Unlike ``index_library``, does
            not walk the directory tree and does not prune stale partitions —
            only the given entries are touched.

            Args:
                partition_scope: Folder paths to index, e.g.
                    ``["2024/2024-07", "2024/2024-08"]``.
                force_extract_exif: Re-extract EXIF and overwrite existing
                    XMP sidecars.  Defaults to False.
                generate_thumbnails: Generate ``thumbnails.avif`` AVIF grids
                    for each partition.  Defaults to True.
                force_full_index: Re-process all photos in every listed
                    partition, even those already indexed.  Defaults to False
                    (incremental).

            Returns:
                ``partitionsIndexed`` — number of partitions processed.
                ``totalPhotos`` — photos indexed in this run (new or force-reindexed).
                ``totalPhotosSkipped`` — photos carried over from existing manifests.
                ``totalPhotosDeleted`` — photos removed from disk across all partitions.
                ``totalSidecarsCreated`` — XMP sidecars written.
                ``totalThumbnailsRebuilt`` — partitions where a new AVIF chunk was generated.
                ``totalErrors`` — count of photos that failed processing.
                ``errorDetails`` — list of per-photo error messages across all partitions.
                ``totalDurationMs`` — wall-clock time for the run in milliseconds.
            """

            async def _scope_progress(
                current: int,
                total: int,
                name: str,
                duration_ms: int = 0,
                photos: int = 0,
            ) -> None:
                message = f"{name} — {photos} photos ({duration_ms}ms)" if duration_ms else name
                try:
                    await ctx.report_progress(progress=current, total=total, message=message)
                except Exception as exc:
                    _log.debug(
                        "Progress notification failed (client may have disconnected): %s", exc
                    )

            try:
                result = await index_partition_scope(
                    self.backend,
                    partition_scope,
                    force_extract_exif=force_extract_exif,
                    generate_thumbnails=generate_thumbnails,
                    force_full_index=force_full_index,
                    on_progress=_scope_progress,
                    lance_index_path=self.lance_index_path_override,
                )
            except Exception as exc:
                cause = exc.exceptions[0] if isinstance(exc, BaseExceptionGroup) else exc
                _log.error("index_partition_scope failed: %s", cause, exc_info=cause)
                raise cause from exc
            return {
                "partitionsIndexed": len(result.partitions),
                "totalPhotosProcessed": result.total_photos_processed,
                "totalPhotosSkipped": result.total_photos_skipped,
                "totalPhotosDeleted": result.total_photos_deleted,
                "totalSidecarsCreated": result.total_sidecars_created,
                "totalThumbnailsRebuilt": result.total_thumbnails_rebuilt,
                "totalErrors": result.total_errors,
                "topErrorDetails": list(result.top_error_details),
                "totalDurationMs": result.total_duration_ms,
            }
