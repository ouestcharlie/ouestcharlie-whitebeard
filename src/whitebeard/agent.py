"""Whitebeard MCP agent — photo indexer for local drives."""

from __future__ import annotations

from ouestcharlie_toolkit.server import AgentBase

from .indexer import index_library, index_partition


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

        @mcp.tool()
        async def index_partition_tool(
            partition: str,
            force: bool = False,
        ) -> dict:
            """Index all photos directly in a partition folder.

            Scans ``partition`` for photo files (JPEG, HEIC, PNG, RAW).  For
            each photo without an existing XMP sidecar (or all photos when
            ``force=True``), extracts EXIF metadata and writes an XMP sidecar
            containing ``ouestcharlie:contentHash`` and all standard EXIF
            fields.  Creates or updates the leaf manifest at
            ``<partition>/.ouestcharlie/manifest.json``, and generates
            ``thumbnails.avif`` (256 px tiles) and ``previews.avif`` (1440 px
            tiles) in the same metadata directory.

            Args:
                partition: Folder path relative to the backend root, e.g.
                    ``""`` for the root, ``"Vacations/Italy 2023/"`` for a
                    sub-folder.  Trailing slash is optional.
                force: Re-extract EXIF and overwrite existing XMP sidecars.

            Returns:
                ``partition`` — echoed partition path.
                ``photosProcessed`` — total photos found.
                ``sidecarsCreated`` — XMP sidecars written (new or force-updated).
                ``sidecarsSkipped`` — photos whose existing sidecar was reused.
                ``thumbnailsRebuilt`` — true if AVIF grids were (re-)generated.
                ``errors`` — count of photos that failed processing.
                ``errorDetails`` — list of per-photo error messages.
            """
            result = await index_partition(
                self.backend, partition, force=force, generate_thumbnails=True
            )
            return {
                "partition": result.partition,
                "photosProcessed": result.photos_processed,
                "sidecarsCreated": result.sidecars_created,
                "sidecarsSkipped": result.sidecars_skipped,
                "thumbnailsRebuilt": result.thumbnails_rebuilt,
                "errors": result.errors,
                "errorDetails": result.error_details,
            }

        @mcp.tool()
        async def index_library_tool(
            root: str = "",
            force: bool = False,
        ) -> dict:
            """Recursively index all photos in the library and build manifests.

            Walks every subfolder under ``root``, indexes each folder that
            contains photos as a leaf partition (creating XMP sidecars and AVIF
            thumbnail grids), then builds parent manifests bottom-up so every
            ancestor folder has an aggregate manifest summarising its children.

            Args:
                root: Library root relative to the backend root.  Defaults to
                    ``""`` (the entire backend).
                force: Re-extract EXIF and overwrite existing XMP sidecars.

            Returns:
                ``partitionsIndexed`` — number of leaf partitions processed.
                ``totalPhotos`` — total photos across all partitions.
                ``totalSidecarsCreated`` — XMP sidecars written.
                ``totalThumbnailsRebuilt`` — partitions whose AVIF grids were regenerated.
                ``totalErrors`` — count of photos that failed processing.
                ``errorDetails`` — list of per-photo error messages across all partitions.
            """
            result = await index_library(self.backend, root=root, force=force, generate_thumbnails=True)
            return {
                "partitionsIndexed": len(result.partitions),
                "totalPhotos": result.total_photos,
                "totalSidecarsCreated": result.total_sidecars_created,
                "totalThumbnailsRebuilt": result.total_thumbnails_rebuilt,
                "totalErrors": result.total_errors,
                "errorDetails": list(result.error_details),
            }
