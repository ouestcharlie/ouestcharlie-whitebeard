"""Tests for Whitebeard's purge_metadata MCP tool."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ouestcharlie_toolkit.schema import METADATA_DIR

from whitebeard.agent import WhitebeardAgent


def _make_agent(root: Path, monkeypatch: pytest.MonkeyPatch) -> WhitebeardAgent:
    monkeypatch.setenv(
        "WOOF_BACKEND_CONFIG",
        json.dumps({"name": "testlib", "type": "filesystem", "path": str(root)}),
    )
    return WhitebeardAgent()


def _purge_tool(agent: WhitebeardAgent):
    return agent.mcp._tool_manager.get_tool("purge_metadata").fn


@pytest.mark.asyncio
async def test_purge_metadata_deletes_dir_keeps_sidecars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Derived metadata lives under .ouestcharlie/; XMP sidecars live alongside
    # the originals, outside it.
    meta = tmp_path / METADATA_DIR
    (meta / "2024").mkdir(parents=True)
    (meta / "summary.json").write_text("{}")
    (meta / "2024" / "thumbnails.avif").write_bytes(b"avif")
    photo = tmp_path / "photo.jpg"
    photo.write_bytes(b"jpeg")
    sidecar = tmp_path / "photo.xmp"
    sidecar.write_text("<xmp/>")

    agent = _make_agent(tmp_path, monkeypatch)
    tool_fn = _purge_tool(agent)
    result = await tool_fn(ctx=None)

    assert result == {"metadataDir": METADATA_DIR, "existed": True}
    assert not meta.exists()
    assert photo.exists()  # originals untouched
    assert sidecar.exists()  # XMP sidecars never deleted


@pytest.mark.asyncio
async def test_purge_metadata_idempotent_when_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = _make_agent(tmp_path, monkeypatch)
    tool_fn = _purge_tool(agent)
    result = await tool_fn(ctx=None)

    assert result == {"metadataDir": METADATA_DIR, "existed": False}
