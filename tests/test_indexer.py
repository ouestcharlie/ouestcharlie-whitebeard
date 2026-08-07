"""Tests for Whitebeard indexer — core indexing logic."""

from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.lance_index import PHOTO_TABLE_NAME, LanceIndex
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import LOWEST_SCHEMA_VERSION, SCHEMA_VERSION, RootSummary
from ouestcharlie_toolkit.xmp import parse_xmp

from whitebeard.indexer import (
    _MAX_CONCURRENT_PARTITIONS,
    IndexResult,
    LibraryIndexResult,
    index_library,
    index_partition,
    index_partition_scope,
)

_SAMPLE_JPG = Path(__file__).parent / "sample-images" / "001.jpg"

# Minimal valid JPEG (SOI + JFIF APP0 + EOI) — no EXIF data.
_MINIMAL_JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xd9"


def _unique_jpeg(i: int) -> bytes:
    """Unique minimal JPEG bytes so each file gets a distinct content hash."""
    return _MINIMAL_JPEG + bytes([i % 256])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmpdir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture()
def backend_with_sample(tmpdir: Path) -> LocalBackend:
    """Backend rooted at a temp dir that contains 001.jpg."""
    shutil.copy(_SAMPLE_JPG, tmpdir / "001.jpg")
    return LocalBackend(root=tmpdir)


@pytest.fixture()
def backend_with_minimal(tmpdir: Path) -> LocalBackend:
    """Backend rooted at a temp dir that contains a minimal JPEG (no EXIF)."""
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    return LocalBackend(root=tmpdir)


def _write_sample_video(path: Path, *, frames: int = 15) -> None:
    """Synthesize a tiny mpeg4 MP4 for tests (mpeg4 ships in every ffmpeg build)."""
    import av
    from PIL import Image as PILImage

    with av.open(str(path), "w") as container:
        vstream = container.add_stream("mpeg4", rate=30)
        vstream.width, vstream.height = 64, 48
        vstream.pix_fmt = "yuv420p"
        for i in range(frames):
            img = PILImage.new("RGB", (64, 48), ((i * 15) % 256, 40, 90))
            container.mux(vstream.encode(av.VideoFrame.from_image(img)))
        container.mux(vstream.encode())


@pytest.fixture()
def backend_with_video(tmpdir: Path) -> LocalBackend:
    """Backend rooted at a temp dir that contains one MP4 video."""
    _write_sample_video(tmpdir / "clip.mp4")
    return LocalBackend(root=tmpdir)


# ---------------------------------------------------------------------------
# index_partition — sidecar creation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_creates_xmp_sidecar(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """index_partition writes an XMP sidecar next to the photo."""
    await index_partition(backend_with_sample, "")
    # New sidecars use the full-extension convention (Darktable/digiKam-compatible, OEC-25).
    assert (tmpdir / "001.jpg.xmp").exists()


@pytest.mark.asyncio
async def test_index_sidecar_has_content_hash(
    backend_with_sample: LocalBackend, tmpdir: Path
) -> None:
    """The created XMP sidecar contains an ouestcharlie:contentHash."""
    await index_partition(backend_with_sample, "")
    sidecar = parse_xmp((tmpdir / "001.jpg.xmp").read_text(encoding="utf-8"))
    assert sidecar.content_hash is not None
    assert len(sidecar.content_hash) == 22


@pytest.mark.asyncio
async def test_index_sidecar_has_camera_fields(
    backend_with_sample: LocalBackend, tmpdir: Path
) -> None:
    """The created XMP sidecar contains make/model extracted from EXIF."""
    await index_partition(backend_with_sample, "")
    sidecar = parse_xmp((tmpdir / "001.jpg.xmp").read_text(encoding="utf-8"))
    assert sidecar.camera_make is not None
    assert sidecar.camera_model is not None


# ---------------------------------------------------------------------------
# index_partition — LanceDB index
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_manifest_photo_entry(backend_with_sample: LocalBackend) -> None:
    """The LanceDB index contains a photo entry with the correct filename and hash."""
    await index_partition(backend_with_sample, "")
    lance_index = await LanceIndex.open(backend_with_sample, PHOTO_TABLE_NAME)
    rows = [r async for r in lance_index.get_partition_rows("")]
    assert len(rows) == 1
    assert rows[0]["filename"] == "001.jpg"
    assert len(rows[0]["content_hash"]) == 22


@pytest.mark.asyncio
async def test_index_video_creates_sidecar_with_media_type(
    backend_with_video: LocalBackend, tmpdir: Path
) -> None:
    """A video file is discovered and gets a sidecar marked media_type=video."""
    await index_partition(backend_with_video, "")
    sidecar = parse_xmp((tmpdir / "clip.mp4.xmp").read_text(encoding="utf-8"))
    assert sidecar.media_type == "video"
    assert sidecar.video_codec == "mpeg4"
    assert sidecar.duration_seconds is not None
    assert len(sidecar.content_hash) == 22


@pytest.mark.asyncio
async def test_index_video_lance_row(backend_with_video: LocalBackend) -> None:
    """The video is indexed into LanceDB with mediaType surfaced as a searchable field."""
    await index_partition(backend_with_video, "")
    lance_index = await LanceIndex.open(backend_with_video, PHOTO_TABLE_NAME)
    rows = [r async for r in lance_index.get_partition_rows("")]
    assert len(rows) == 1
    assert rows[0]["filename"] == "clip.mp4"
    assert rows[0]["media_type"] == "video"


@pytest.mark.asyncio
async def test_index_video_cover_frame_thumbnail(
    backend_with_video: LocalBackend, tmpdir: Path
) -> None:
    """Video-only partition: the cover frame is tiled into an AVIF thumbnail chunk."""
    result = await index_partition(backend_with_video, "", generate_thumbnails=True)
    assert result.errors == 0
    assert result.thumbnails_rebuilt
    assert list((tmpdir / ".ouestcharlie").glob("thumbnails-*.avif"))
    # The video row carries the tile location, like a photo.
    lance_index = await LanceIndex.open(backend_with_video, PHOTO_TABLE_NAME)
    rows = [r async for r in lance_index.get_partition_rows("")]
    assert rows[0]["thumbnail_avif_hash"] is not None


@pytest.mark.asyncio
async def test_index_mixed_photo_and_video(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """A partition with both a photo and a video indexes and thumbnails both."""
    _write_sample_video(tmpdir / "clip.mp4")
    result = await index_partition(backend_with_sample, "", generate_thumbnails=True)
    assert result.errors == 0
    assert result.photos_processed == 2
    lance_index = await LanceIndex.open(backend_with_sample, PHOTO_TABLE_NAME)
    rows = [x async for x in lance_index.get_partition_rows("")]
    media = {r["filename"]: r["media_type"] for r in rows}
    assert media == {"001.jpg": "photo", "clip.mp4": "video"}
    # Both media produced tiles (they may share one grid chunk).
    assert all(r["thumbnail_avif_hash"] is not None for r in rows)
    assert list((tmpdir / ".ouestcharlie").glob("thumbnails-*.avif"))


@pytest.mark.asyncio
async def test_index_partition_alone_does_not_write_summary(
    backend_with_sample: LocalBackend, tmpdir: Path
) -> None:
    """indexer.index_partition() is pure LanceDB/XMP logic — it never touches summary.json.

    summary.json is written once per session by whichever caller owns the
    session (index_library, or the index_partition MCP tool wrapper in
    agent.py) — not by this lower-level function, so it can be run
    concurrently across partitions without contending on a shared file.
    """
    await index_partition(backend_with_sample, "")
    assert not (tmpdir / ".ouestcharlie" / "summary.json").exists()


@pytest.mark.asyncio
async def test_index_manifest_rating_in_lance_index(tmpdir: Path) -> None:
    """Rating extracted from EXIF/XMP ends up in the LanceDB row (aggregated at query time)."""
    from ouestcharlie_toolkit.schema import PhotoEntry, XmpSidecar

    (tmpdir / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "b.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "c.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    ratings = [2, 5, 4]
    call_count = 0

    async def fake_process(xmp_store, photo_path, force_extract_exif):
        nonlocal call_count
        r = ratings[call_count]
        call_count += 1
        sidecar = XmpSidecar(content_hash=f"KfAbc123A2nBcR8xYvLm1P{call_count}", rating=r)
        entry = PhotoEntry.from_sidecar(
            photo_path.split("/")[-1], sidecar, sidecar.content_hash, "1"
        )
        return entry, True

    with patch("whitebeard.indexer._extract_one", side_effect=fake_process):
        await index_partition(backend, "")

    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    rows = [r async for r in lance_index.get_partition_rows("")]
    assert sorted(r["rating"] for r in rows) == [2, 4, 5]


# ---------------------------------------------------------------------------
# index_partition — skip / force behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_skips_existing_sidecar(
    backend_with_sample: LocalBackend, tmpdir: Path
) -> None:
    """Without force, an existing XMP sidecar is not overwritten."""
    sentinel = "<!-- sentinel -->"
    xmp_path = tmpdir / "001.xmp"
    xmp_path.write_text(sentinel, encoding="utf-8")

    result = await index_partition(backend_with_sample, "")

    assert xmp_path.read_text(encoding="utf-8") == sentinel
    assert result.sidecars_skipped == 1
    assert result.sidecars_created == 0


@pytest.mark.asyncio
async def test_index_force_overwrites_sidecar(
    backend_with_sample: LocalBackend, tmpdir: Path
) -> None:
    """With force=True, an existing XMP sidecar is replaced with fresh EXIF data."""
    # First index run creates the sidecar (full-extension convention, OEC-25).
    await index_partition(backend_with_sample, "")

    # Overwrite the sidecar with a sentinel.
    xmp_path = tmpdir / "001.jpg.xmp"
    xmp_path.write_text("<!-- overwritten -->", encoding="utf-8")

    # force_full_index=True is required alongside force_extract_exif so that the photo
    # is re-processed rather than carried over from the existing manifest.
    result = await index_partition(
        backend_with_sample, "", force_extract_exif=True, force_full_index=True
    )

    assert result.sidecars_created == 1
    assert result.sidecars_skipped == 0
    # Content should be the fresh XMP, not the sentinel.
    new_content = xmp_path.read_text(encoding="utf-8")
    assert "<!-- overwritten -->" not in new_content
    assert "ouestcharlie" in new_content


# ---------------------------------------------------------------------------
# index_partition — file filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_ignores_non_photo_files(tmpdir: Path) -> None:
    """Non-photo files (.txt, .md, .json) in a partition are ignored."""
    (tmpdir / "notes.txt").write_text("hello")
    (tmpdir / "README.md").write_text("# readme")
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_partition(backend, "")

    assert result.photos_processed == 1  # only photo.jpg


@pytest.mark.asyncio
async def test_index_ignores_subdirectory_photos(tmpdir: Path) -> None:
    """Photos in subdirectories are NOT indexed as part of the parent partition."""
    subdir = tmpdir / "subdir"
    subdir.mkdir()
    (subdir / "deep.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "top.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_partition(backend, "")

    assert result.photos_processed == 1  # only top.jpg
    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    rows = [r async for r in lance_index.get_partition_rows("")]
    assert len(rows) == 1
    assert rows[0]["filename"] == "top.jpg"


@pytest.mark.asyncio
async def test_index_result_counts(backend_with_sample: LocalBackend) -> None:
    """IndexResult counts are accurate for a clean index run."""
    result = await index_partition(backend_with_sample, "")
    assert isinstance(result, IndexResult)
    assert result.photos_processed == 1
    assert result.sidecars_created == 1
    assert result.sidecars_skipped == 0
    assert result.errors == 0


# ---------------------------------------------------------------------------
# index_partition — sub-folder partition
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_sub_partition(tmpdir: Path) -> None:
    """index_partition works correctly for a non-root partition."""
    sub = tmpdir / "Vacations" / "Italy"
    sub.mkdir(parents=True)
    shutil.copy(_SAMPLE_JPG, sub / "001.jpg")
    backend = LocalBackend(root=tmpdir)

    result = await index_partition(backend, "Vacations/Italy")

    assert result.photos_processed == 1
    assert result.sidecars_created == 1
    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    rows = [r async for r in lance_index.get_partition_rows("Vacations/Italy")]
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# index_library — recursive indexing and parent manifests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_library_single_partition(tmpdir: Path) -> None:
    """index_library with photos only at root indexes exactly one partition."""
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_library(backend)

    assert isinstance(result, LibraryIndexResult)
    assert len(result.partitions) == 1
    assert result.total_photos_processed == 1


@pytest.mark.asyncio
async def test_index_library_one_partition_empty(tmpdir: Path) -> None:
    """index_library with one empty partition pruned."""
    (tmpdir / "2024" / "2024-07").mkdir(parents=True)
    (tmpdir / "2024" / "2024-07" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "2024" / "2024-08").mkdir(parents=True)  # Empty
    backend = LocalBackend(root=tmpdir)

    result = await index_library(backend)

    assert isinstance(result, LibraryIndexResult)
    assert len(result.partitions) == 1
    assert result.total_photos_processed == 1


@pytest.mark.asyncio
async def test_index_library_one_partition_empty_update(tmpdir: Path) -> None:
    """index_library with one empty partition pruned."""
    (tmpdir / "2024" / "2024-07").mkdir(parents=True)
    (tmpdir / "2024" / "2024-07" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "2024" / "2024-08").mkdir(parents=True)  # Empty
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)
    # second run — should not duplicate
    result = await index_library(backend)

    assert isinstance(result, LibraryIndexResult)
    assert len(result.partitions) == 1
    assert result.total_photos_processed == 0
    assert result.total_photos_skipped == 1


@pytest.mark.asyncio
async def test_index_library_one_partition_empty_parent_update(tmpdir: Path) -> None:
    """index_library with one empty partition pruned."""
    # Directories
    (tmpdir / "2024" / "2024-07").mkdir(parents=True)
    # Images
    (tmpdir / "2024" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "2024" / "2024-07" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_library(backend)
    assert len(result.partitions) == 2
    assert result.total_photos_processed == 2
    assert result.total_photos_skipped == 0
    assert result.total_photos_deleted == 0
    assert result.total_errors == 0

    # delete and clear partition "2024" but keep sub-partitions
    (tmpdir / "2024" / "a.jpg").unlink()

    # second run — process deletion
    result = await index_library(backend)

    assert isinstance(result, LibraryIndexResult)
    assert len(result.partitions) == 2.0  # Logging deletion
    assert result.partitions_deleted == 0  # Parent is kept
    assert result.total_photos_processed == 0
    assert result.total_photos_skipped == 1
    assert result.total_photos_deleted == 1
    assert result.total_errors == 0

    # third run — nothing to do
    result = await index_library(backend)

    assert isinstance(result, LibraryIndexResult)
    assert len(result.partitions) == 1.0
    assert result.partitions_deleted == 0
    assert result.total_photos_processed == 0
    assert result.total_photos_skipped == 1
    assert result.total_photos_deleted == 0
    assert result.total_errors == 0


@pytest.mark.asyncio
async def test_index_library_writes_thin_summary_json(tmpdir: Path) -> None:
    """index_library writes summary.json once, as the thin schema-version marker."""
    (tmpdir / "2024" / "2024-07").mkdir(parents=True)
    (tmpdir / "2024" / "2024-07" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)

    summary_file = tmpdir / ".ouestcharlie" / "summary.json"
    assert summary_file.exists()
    data = json.loads(summary_file.read_text())
    assert data["schemaVersion"] == SCHEMA_VERSION
    assert "partitions" not in data


@pytest.mark.asyncio
async def test_index_library_reindex_keeps_lance_rows_current(tmpdir: Path) -> None:
    """Re-indexing a partition does not duplicate its LanceDB rows."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_unique_jpeg(0))
    (tmpdir / "A" / "p2.jpg").write_bytes(_unique_jpeg(1))
    backend = LocalBackend(root=tmpdir)

    await index_partition(backend, "A")
    # second run — should not duplicate
    await index_partition(backend, "A")

    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    rows = [r async for r in lance_index.get_partition_rows("A")]
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_index_library_indexes_all_partitions(tmpdir: Path) -> None:
    """index_library indexes every partition into LanceDB."""
    (tmpdir / "2024" / "2024-07").mkdir(parents=True)
    (tmpdir / "2024" / "2024-08").mkdir(parents=True)
    (tmpdir / "2024" / "2024-07" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "2024" / "2024-08" / "b.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_library(backend)

    assert result.total_photos_processed == 2
    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    paths = await lance_index.list_partitions()
    assert "2024/2024-07" in paths
    assert "2024/2024-08" in paths


@pytest.mark.asyncio
async def test_index_library_lance_rating_per_partition(tmpdir: Path) -> None:
    """Rating values land in the correct partition's LanceDB rows."""
    from ouestcharlie_toolkit.schema import XmpSidecar
    from ouestcharlie_toolkit.xmp import serialize_xmp

    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "A" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B").mkdir()
    (tmpdir / "B" / "p3.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    for photo, rating in [("A/p1.jpg", 2), ("A/p2.jpg", 4), ("B/p3.jpg", 5)]:
        sidecar = XmpSidecar(content_hash=f"KfAbc123A2nBcR8xYvLm1P{rating}", rating=rating)
        xmp_file = tmpdir / photo.replace(".jpg", ".xmp")
        xmp_file.write_text(serialize_xmp(sidecar), encoding="utf-8")

    await index_library(backend)

    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    ratings_a = sorted([r["rating"] async for r in lance_index.get_partition_rows("A")])
    ratings_b = sorted([r["rating"] async for r in lance_index.get_partition_rows("B")])
    assert ratings_a == [2, 4]
    assert ratings_b == [5]


@pytest.mark.asyncio
async def test_index_library_lance_photo_count_per_partition(tmpdir: Path) -> None:
    """LanceDB row count per partition matches the actual photo count on disk."""
    (tmpdir / "A").mkdir()
    (tmpdir / "B").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG + b"1")
    (tmpdir / "A" / "p2.jpg").write_bytes(_MINIMAL_JPEG + b"2")
    (tmpdir / "B" / "p3.jpg").write_bytes(_MINIMAL_JPEG + b"1")  # Same as p1.jpg
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)

    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    rows_a = [r async for r in lance_index.get_partition_rows("A")]
    rows_b = [r async for r in lance_index.get_partition_rows("B")]
    assert len(rows_a) == 2
    assert len(rows_b) == 1


def test_top_error_details_caps_at_ten() -> None:
    """top_error_details yields at most _TOP_ERRORS (10) messages across all partitions."""
    result = LibraryIndexResult(
        partitions=[
            IndexResult("p1", errors=7, error_details=[f"p1-err-{i}" for i in range(7)]),
            IndexResult("p2", errors=7, error_details=[f"p2-err-{i}" for i in range(7)]),
        ]
    )
    details = list(result.top_error_details)
    assert len(details) == 10
    # First errors come from p1 (in order), then p2 up to the cap.
    assert details[:7] == [f"p1-err-{i}" for i in range(7)]
    assert details[7:] == ["p2-err-0", "p2-err-1", "p2-err-2"]


def test_top_error_details_fewer_than_ten() -> None:
    """top_error_details yields all errors when total count is below the cap."""
    result = LibraryIndexResult(
        partitions=[
            IndexResult("p1", errors=2, error_details=["e1", "e2"]),
            IndexResult("p2", errors=1, error_details=["e3"]),
        ]
    )
    assert list(result.top_error_details) == ["e1", "e2", "e3"]


def test_top_error_details_no_errors() -> None:
    """top_error_details yields nothing when there are no errors."""
    result = LibraryIndexResult(partitions=[IndexResult("p1")])
    assert list(result.top_error_details) == []


@pytest.mark.asyncio
async def test_index_library_result_totals(tmpdir: Path) -> None:
    """LibraryIndexResult aggregates counts across all partitions."""
    (tmpdir / "A").mkdir()
    (tmpdir / "B").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_library(backend)

    assert result.total_photos_processed == 2
    assert result.total_sidecars_created == 2
    assert result.total_errors == 0


@pytest.mark.asyncio
async def test_index_library_idempotent(tmpdir: Path) -> None:
    """Running index_library twice with the same library is idempotent."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)
    result2 = await index_library(backend)

    # In incremental mode, the second run carries over already-indexed photos.
    assert result2.total_photos_processed == 0  # no newly processed photos
    assert result2.total_photos_skipped == 1  # one photo carried over from manifest
    assert result2.total_sidecars_created == 0  # sidecar already exists
    assert result2.total_errors == 0


# ---------------------------------------------------------------------------
# Logging behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_partition_logs_error_on_photo_failure(tmpdir: Path, caplog) -> None:
    """When _process_one raises, an ERROR with exc_info is logged."""
    (tmpdir / "broken.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    with (
        patch("whitebeard.indexer._extract_one", side_effect=RuntimeError("simulated failure")),
        caplog.at_level(logging.ERROR, logger="whitebeard.indexer"),
    ):
        result = await index_partition(backend, "")

    assert result.errors == 1
    assert any("simulated failure" in msg for msg in caplog.messages)
    assert any(r.levelno == logging.ERROR for r in caplog.records)
    assert any(r.exc_info is not None for r in caplog.records)


# ---------------------------------------------------------------------------
# Mixed-timezone datetime handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_mixed_timezone_photos(tmpdir: Path) -> None:
    """index_partition succeeds when photos have a mix of aware and naive datetimes.

    Regression test: min()/max() over a mixed list raises TypeError without the
    _naive() key function.
    """
    from datetime import timedelta, timezone

    from ouestcharlie_toolkit.backend import VersionToken
    from ouestcharlie_toolkit.schema import PhotoEntry, XmpSidecar

    naive_dt = datetime(2024, 7, 1, 12, 0, 0)
    aware_dt = datetime(2024, 7, 2, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))

    (tmpdir / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "b.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    call_count = 0

    async def fake_process(xmp_store, photo_path, force_extract_exif):
        nonlocal call_count
        call_count += 1
        dt = naive_dt if call_count == 1 else aware_dt
        sidecar = XmpSidecar(content_hash="KfAbc123A2nBcR8xYvLm1P", date_taken=dt)
        token = VersionToken(value=1)
        entry = PhotoEntry.from_sidecar(
            photo_path.split("/")[-1], sidecar, sidecar.content_hash, str(token.value)
        )
        return entry, True

    with patch("whitebeard.indexer._extract_one", side_effect=fake_process):
        result = await index_partition(backend, "")

    assert result.errors == 0


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_partition_duration_ms(backend_with_sample: LocalBackend) -> None:
    """IndexResult.duration_ms is a non-negative integer after indexing."""
    result = await index_partition(backend_with_sample, "")
    assert isinstance(result.duration_ms, int)
    assert result.duration_ms >= 0


@pytest.mark.asyncio
async def test_index_library_total_duration_ms(tmpdir: Path) -> None:
    """LibraryIndexResult.total_duration_ms is wall-clock time, not the sum of partition times."""
    (tmpdir / "A").mkdir()
    (tmpdir / "B").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_library(backend)

    assert isinstance(result.total_duration_ms, int)
    assert result.total_duration_ms >= 0
    # Wall-clock must be <= sum of partition times (parallelism can only help).
    assert result.total_duration_ms <= sum(p.duration_ms for p in result.partitions) + 50


# ---------------------------------------------------------------------------
# Parallel indexing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_library_concurrency_capped(tmpdir: Path) -> None:
    """No more than _MAX_CONCURRENT_PARTITIONS partitions run at the same time."""

    n = _MAX_CONCURRENT_PARTITIONS + 2  # more partitions than the cap
    for i in range(n):
        (tmpdir / f"P{i}").mkdir()
        (tmpdir / f"P{i}" / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    active: list[int] = []
    peak: list[int] = []

    original_index_partition = index_partition

    async def tracked_index_partition(b, partition, *args, **kwargs):
        active.append(1)
        peak.append(len(active))
        try:
            return await original_index_partition(b, partition, *args, **kwargs)
        finally:
            active.pop()

    with patch("whitebeard.indexer.index_partition", side_effect=tracked_index_partition):
        await index_library(backend)

    assert max(peak) <= _MAX_CONCURRENT_PARTITIONS


# ---------------------------------------------------------------------------
# Schema version upgrade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_library_forces_full_reindex_on_schema_upgrade(tmpdir: Path) -> None:
    """When summary.json has an older schemaVersion, index_library forces a full reindex."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    # First index — creates manifests with the current SCHEMA_VERSION.
    await index_library(backend)

    # Downgrade below the lowest supported version to simulate a too-old index.
    store = ManifestStore(backend)
    _current_summary, version = await store.read_summary()
    stale = RootSummary(schema_version=LOWEST_SCHEMA_VERSION - 1)
    await store.write_summary(stale, version)

    # Second run (incremental by default) — must detect stale version and force full reindex.
    result = await index_library(backend)

    assert result.total_photos_processed > 0  # all photos re-processed, none skipped
    assert result.total_photos_skipped == 0


@pytest.mark.asyncio
async def test_index_library_no_reindex_within_compatible_window(tmpdir: Path) -> None:
    """A version within [LOWEST, SCHEMA_VERSION) is used in place — no forced reindex."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)

    # Simulate an index one version behind but still within the compatible window.
    store = ManifestStore(backend)
    _current, version = await store.read_summary()
    await store.write_summary(RootSummary(schema_version=LOWEST_SCHEMA_VERSION), version)

    result = await index_library(backend)  # must stay incremental, not full-reindex

    assert result.total_photos_processed == 0
    assert result.total_photos_skipped > 0
    # A successful run rewrites the summary to the current version.
    after, _ = await store.read_summary()
    assert after.schema_version == SCHEMA_VERSION


@pytest.mark.asyncio
async def test_index_library_no_forced_reindex_when_schema_current(tmpdir: Path) -> None:
    """When the schema version is current, a second run stays incremental."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)
    result = await index_library(backend)  # incremental — schema version matches

    assert result.total_photos_processed == 0  # no re-processing
    assert result.total_photos_skipped > 0  # photo carried over from manifest


@pytest.mark.asyncio
async def test_index_library_raises_when_schema_newer(tmpdir: Path) -> None:
    """When summary.json has a newer schemaVersion, index_library refuses to run."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)

    store = ManifestStore(backend)
    _current_summary, version = await store.read_summary()
    newer = RootSummary(schema_version=SCHEMA_VERSION + 1)
    await store.write_summary(newer, version)

    with pytest.raises(ValueError, match="newer"):
        await index_library(backend)

    # summary.json must be left untouched (still the newer version, not overwritten).
    unchanged_summary, _ = await store.read_summary()
    assert unchanged_summary.schema_version == SCHEMA_VERSION + 1


@pytest.mark.asyncio
async def test_index_library_progress_callback_called_for_each_partition(tmpdir: Path) -> None:
    """on_progress is called exactly once per partition."""
    for name in ("A", "B", "C"):
        (tmpdir / name).mkdir()
        (tmpdir / name / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    calls: list[tuple] = []

    async def on_progress(done, total, partition, duration_ms, photos):
        calls.append((done, total, partition))

    await index_library(backend, on_progress=on_progress)

    # One call per partition (root "" + A + B + C = 4 partitions)
    assert len(calls) == 4
    # 'total' is always the same; 'done' counts up to total
    totals = {total for _, total, _ in calls}
    assert totals == {4}
    assert sorted(done for done, _, _ in calls) == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Incremental indexing (force_full_index)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_result_has_photos_skipped_and_deleted_fields(
    backend_with_minimal: LocalBackend,
) -> None:
    """IndexResult has photos_skipped and photos_deleted fields, both 0 on first run."""
    result = await index_partition(backend_with_minimal, "")
    assert result.photos_skipped == 0
    assert result.photos_deleted == 0
    assert result.errors == 0


@pytest.mark.asyncio
async def test_incremental_first_run_processes_all(tmpdir: Path) -> None:
    """Without an existing manifest, incremental mode processes all photos."""
    (tmpdir / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "b.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_partition(backend, "")  # force_full_index=False is the default

    assert result.photos_processed == 2
    assert result.photos_skipped == 0
    assert result.photos_deleted == 0
    assert result.errors == 0


@pytest.mark.asyncio
async def test_incremental_skips_already_indexed_photos(tmpdir: Path) -> None:
    """In incremental mode, photos already in the manifest are not re-processed."""
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    # First run: creates the manifest.
    result1 = await index_partition(backend, "")
    assert result1.photos_processed == 1
    assert result1.photos_skipped == 0

    call_count = 0
    from whitebeard.indexer import _extract_one as real_extract_one

    async def counting_extract(xmp_store, photo_path, force_extract_exif):
        nonlocal call_count
        call_count += 1
        return await real_extract_one(xmp_store, photo_path, force_extract_exif)

    with patch("whitebeard.indexer._extract_one", side_effect=counting_extract):
        result2 = await index_partition(backend, "")

    assert result2.photos_processed == 0
    assert result2.photos_skipped == 1
    assert result2.photos_deleted == 0
    assert result2.errors == 0
    assert call_count == 0  # _extract_one was never called


@pytest.mark.asyncio
async def test_incremental_processes_new_photos_in_existing_partition(tmpdir: Path) -> None:
    """In incremental mode, only photos absent from the manifest are processed."""
    (tmpdir / "existing.jpg").write_bytes(_unique_jpeg(0))
    backend = LocalBackend(root=tmpdir)

    # First run: index existing.jpg.
    result1 = await index_partition(backend, "")
    assert result1.photos_processed == 1

    # Add a new photo.
    (tmpdir / "new.jpg").write_bytes(_unique_jpeg(1))

    result2 = await index_partition(backend, "")

    assert result2.photos_processed == 1  # only new.jpg
    assert result2.photos_skipped == 1  # existing.jpg carried over
    assert result2.photos_deleted == 0
    assert result2.errors == 0

    # Both photos must appear in the index.
    lance_index_obj = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    rows = [r async for r in lance_index_obj.get_partition_rows("")]
    filenames = {r["filename"] for r in rows}
    assert "existing.jpg" in filenames
    assert "new.jpg" in filenames


@pytest.mark.asyncio
async def test_incremental_removes_deleted_photos_from_manifest(
    tmpdir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Photos deleted from disk are counted, logged, and removed from the index."""
    (tmpdir / "keep.jpg").write_bytes(_unique_jpeg(0))
    (tmpdir / "delete.jpg").write_bytes(_unique_jpeg(1))
    backend = LocalBackend(root=tmpdir)

    # First run: both photos indexed.
    await index_partition(backend, "")

    # Remove one photo from disk.
    (tmpdir / "delete.jpg").unlink()

    with caplog.at_level(logging.INFO, logger="whitebeard.indexer"):
        result = await index_partition(backend, "")

    assert result.photos_deleted == 1
    assert result.photos_skipped == 1  # keep.jpg carried over
    assert result.photos_processed == 0

    # "delete.jpg" must appear in the INFO log.
    assert any("delete.jpg" in record.message for record in caplog.records)

    # Index must only contain keep.jpg.
    lance_index_obj = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    rows = [r async for r in lance_index_obj.get_partition_rows("")]
    filenames = {r["filename"] for r in rows}
    assert "keep.jpg" in filenames
    assert "delete.jpg" not in filenames


@pytest.mark.asyncio
async def test_force_full_index_reprocesses_all_photos(tmpdir: Path) -> None:
    """force_full_index=True re-processes all photos even if manifest is current."""
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_partition(backend, "")  # first run

    call_count = 0
    from whitebeard.indexer import _extract_one as real_extract_one

    async def counting_extract(xmp_store, photo_path, force_extract_exif):
        nonlocal call_count
        call_count += 1
        return await real_extract_one(xmp_store, photo_path, force_extract_exif)

    with patch("whitebeard.indexer._extract_one", side_effect=counting_extract):
        result = await index_partition(backend, "", force_full_index=True)

    assert result.photos_processed == 1
    assert result.photos_skipped == 0
    assert result.photos_deleted == 0
    assert call_count == 1  # _extract_one was called despite existing manifest


@pytest.mark.asyncio
async def test_index_library_incremental_skips_already_indexed(tmpdir: Path) -> None:
    """Second library run in incremental mode skips already-indexed photos."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)  # initial run

    result = await index_library(backend)  # incremental second run

    partition_a = next(r for r in result.partitions if r.partition == "A")
    assert partition_a.photos_skipped == 1
    assert partition_a.photos_processed == 0
    assert result.total_photos_skipped >= 1


@pytest.mark.asyncio
async def test_index_library_force_full_index_reprocesses_all(tmpdir: Path) -> None:
    """index_library with force_full_index=True reprocesses all photos."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)  # initial run

    result = await index_library(backend, force_full_index=True)

    for r in result.partitions:
        assert r.photos_skipped == 0


@pytest.mark.asyncio
async def test_incremental_skips_thumbnail_when_no_change(tmpdir: Path) -> None:
    """In incremental mode, thumbnails are not regenerated when no new photos exist."""
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_partition(backend, "")  # first run

    thumbnail_call_count = 0

    async def counting_thumbnails(b, partition, entries, tier):
        nonlocal thumbnail_call_count
        thumbnail_call_count += 1
        return []

    with patch(
        "whitebeard.indexer.generate_partition_thumbnails",
        side_effect=counting_thumbnails,
    ):
        result = await index_partition(backend, "", generate_thumbnails=True)

    assert result.thumbnails_rebuilt is False
    assert thumbnail_call_count == 0


@pytest.mark.asyncio
async def test_incremental_generates_new_thumbnail_chunk_when_photo_added(
    tmpdir: Path,
) -> None:
    """In incremental mode, a new thumbnail chunk is generated for new photos only."""
    (tmpdir / "existing.jpg").write_bytes(_unique_jpeg(0))
    backend = LocalBackend(root=tmpdir)

    await index_partition(backend, "")  # first run

    (tmpdir / "new.jpg").write_bytes(_unique_jpeg(1))  # add a new photo

    thumbnail_call_args: list = []

    async def capturing_thumbnails(b, partition, entries, tier):
        thumbnail_call_args.append([e.filename for e in entries])
        return []

    with patch(
        "whitebeard.indexer.generate_partition_thumbnails",
        side_effect=capturing_thumbnails,
    ):
        result = await index_partition(backend, "", generate_thumbnails=True)

    assert result.thumbnails_rebuilt is True
    assert len(thumbnail_call_args) == 1
    # Only the new photo must be passed to the thumbnail builder.
    assert thumbnail_call_args[0] == ["new.jpg"]


# ---------------------------------------------------------------------------
# Deleted partition cleanup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_library_removes_stale_partition_from_lance(tmpdir: Path) -> None:
    """After a partition directory is deleted, the next library run removes it from LanceDB."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B").mkdir()
    (tmpdir / "B" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)

    # Delete partition B from disk.
    shutil.rmtree(tmpdir / "B")

    result = await index_library(backend)

    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    assert "B" not in await lance_index.list_partitions()
    assert result.partitions_deleted == 1


@pytest.mark.asyncio
async def test_index_library_deletes_stale_partition_metadata_files(tmpdir: Path) -> None:
    """After a partition directory is deleted, the next library run deletes its metadata dir."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)

    # Confirm metadata dir was created.
    metadata_dir = tmpdir / ".ouestcharlie" / "A"
    assert metadata_dir.exists()

    # Delete partition A from disk.
    shutil.rmtree(tmpdir / "A")

    await index_library(backend)

    assert not metadata_dir.exists()


@pytest.mark.asyncio
async def test_index_library_does_not_touch_active_partitions(tmpdir: Path) -> None:
    """Active partitions are left intact when a different partition is deleted."""
    (tmpdir / "Keep").mkdir()
    (tmpdir / "Keep" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "Gone").mkdir()
    (tmpdir / "Gone" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)
    shutil.rmtree(tmpdir / "Gone")
    await index_library(backend)

    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    assert "Keep" in await lance_index.list_partitions()


@pytest.mark.asyncio
async def test_index_library_partitions_deleted_count(tmpdir: Path) -> None:
    """partitions_deleted reflects the number of stale partitions removed."""
    for name in ("X", "Y", "Z"):
        (tmpdir / name).mkdir()
        (tmpdir / name / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)

    shutil.rmtree(tmpdir / "X")
    shutil.rmtree(tmpdir / "Z")

    result = await index_library(backend)

    assert result.partitions_deleted == 2


# ---------------------------------------------------------------------------
# index_partition_scope — explicit multi-folder indexing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_partition_scope_indexes_only_listed_partitions(tmpdir: Path) -> None:
    """Only the given partition_scope entries are indexed, not the whole tree."""
    (tmpdir / "2024" / "2024-07").mkdir(parents=True)
    (tmpdir / "2024" / "2024-07" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "2024" / "2024-08").mkdir(parents=True)
    (tmpdir / "2024" / "2024-08" / "b.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "2024" / "2024-09").mkdir(parents=True)
    (tmpdir / "2024" / "2024-09" / "c.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)
    await ManifestStore(backend).write_full_summary(RootSummary(schema_version=SCHEMA_VERSION))

    result = await index_partition_scope(backend, ["2024/2024-07", "2024/2024-08"])

    assert isinstance(result, LibraryIndexResult)
    assert len(result.partitions) == 2
    assert result.total_photos_processed == 2
    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    partitions = await lance_index.list_partitions()
    assert "2024/2024-07" in partitions
    assert "2024/2024-08" in partitions
    assert "2024/2024-09" not in partitions


@pytest.mark.asyncio
async def test_index_partition_scope_does_not_prune_out_of_scope_partitions(
    tmpdir: Path,
) -> None:
    """Partitions outside the scope are left untouched, even if deleted from disk."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B").mkdir()
    (tmpdir / "B" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)
    shutil.rmtree(tmpdir / "B")

    # Only re-index A — B is gone from disk but out of scope, so must not be pruned.
    await index_partition_scope(backend, ["A"])

    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    assert "B" in await lance_index.list_partitions()


@pytest.mark.asyncio
async def test_index_partition_scope_does_not_write_summary(tmpdir: Path) -> None:
    """A scoped run must not write summary.json — it only checks it, never updates it."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)
    store = ManifestStore(backend)
    await store.write_full_summary(RootSummary(schema_version=SCHEMA_VERSION))
    before, _ = await store.read_summary()

    await index_partition_scope(backend, ["A"])

    after, _ = await store.read_summary()
    assert after.schema_version == before.schema_version
    assert after.last_indexed_at == before.last_indexed_at


@pytest.mark.asyncio
async def test_index_partition_scope_raises_when_summary_missing(tmpdir: Path) -> None:
    """Without a prior full index (no summary.json), scoped indexing refuses to run."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    with pytest.raises(ValueError, match="full index"):
        await index_partition_scope(backend, ["A"])

    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME, create_if_missing=True)
    assert await lance_index.list_partitions() == set()


@pytest.mark.asyncio
async def test_index_partition_scope_raises_when_schema_too_old(tmpdir: Path) -> None:
    """A version below the lowest supported means scoped indexing must refuse."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)
    store = ManifestStore(backend)
    await store.write_full_summary(RootSummary(schema_version=LOWEST_SCHEMA_VERSION - 1))

    with pytest.raises(ValueError, match="older"):
        await index_partition_scope(backend, ["A"])

    unchanged, _ = await store.read_summary()
    assert unchanged.schema_version == LOWEST_SCHEMA_VERSION - 1
    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME, create_if_missing=True)
    assert await lance_index.list_partitions() == set()


@pytest.mark.asyncio
async def test_index_partition_scope_accepts_compatible_window(tmpdir: Path) -> None:
    """A version within [LOWEST, SCHEMA_VERSION] is accepted by a scoped run."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)
    await index_library(backend)  # establishes a real index + manifests

    # Downgrade the marker to the lowest supported version — still in the window.
    store = ManifestStore(backend)
    _current, version = await store.read_summary()
    await store.write_summary(RootSummary(schema_version=LOWEST_SCHEMA_VERSION), version)

    result = await index_partition_scope(backend, ["A"])  # must not raise
    assert result.partitions


@pytest.mark.asyncio
async def test_index_partition_scope_raises_when_schema_newer(tmpdir: Path) -> None:
    """A newer-than-supported schema version means scoped indexing must refuse."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)
    store = ManifestStore(backend)
    await store.write_full_summary(RootSummary(schema_version=SCHEMA_VERSION + 1))

    with pytest.raises(ValueError, match="newer"):
        await index_partition_scope(backend, ["A"])

    unchanged, _ = await store.read_summary()
    assert unchanged.schema_version == SCHEMA_VERSION + 1
    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME, create_if_missing=True)
    assert await lance_index.list_partitions() == set()


@pytest.mark.asyncio
async def test_index_partition_scope_progress_callback_called_for_each_partition(
    tmpdir: Path,
) -> None:
    for name in ("A", "B"):
        (tmpdir / name).mkdir()
        (tmpdir / name / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)
    await ManifestStore(backend).write_full_summary(RootSummary(schema_version=SCHEMA_VERSION))

    calls = []

    async def on_progress(current, total, name, duration_ms, photos):
        calls.append((current, total, name))

    await index_partition_scope(backend, ["A", "B"], on_progress=on_progress)

    assert len(calls) == 2
    assert {c[2] for c in calls} == {"A", "B"}
    assert all(c[1] == 2 for c in calls)
