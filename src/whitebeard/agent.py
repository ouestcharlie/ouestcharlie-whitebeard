"""Whitebeard MCP agent — photo indexer for local drives."""

from __future__ import annotations

from ouestcharlie_toolkit.server import AgentBase

from .indexer import index_partition


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
            fields.  Finally creates or updates the partition's leaf manifest at
            ``<partition>/.ouestcharlie/manifest.json``.

            Args:
                partition: Folder path relative to the backend root, e.g.
                    ``""`` for the root, ``"Vacations/Italy 2023/"`` for a
                    sub-folder.  Trailing slash is optional.
                force: Re-extract EXIF and overwrite existing XMP sidecars.

            Returns:
                Summary dict with ``photosProcessed``, ``sidecarsCreated``,
                ``sidecarsSkipped``, and ``errors``.
            """
            result = await index_partition(self.backend, partition, force=force)
            return {
                "partition": result.partition,
                "photosProcessed": result.photos_processed,
                "sidecarsCreated": result.sidecars_created,
                "sidecarsSkipped": result.sidecars_skipped,
                "errors": result.errors,
            }
