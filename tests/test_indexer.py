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
from ouestcharlie_toolkit.manifest import ManifestStore, ManifestSummary
from ouestcharlie_toolkit.schema import METADATA_DIR
from ouestcharlie_toolkit.xmp import parse_xmp

from whitebeard.indexer import (
    _MAX_CONCURRENT_PARTITIONS,
    IndexResult,
    LibraryIndexResult,
    index_library,
    index_partition,
)

_SAMPLE_JPG = Path(__file__).parent / "sample-images" / "001.jpg"

# Minimal valid JPEG (SOI + JFIF APP0 + EOI) — no EXIF data.
_MINIMAL_JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xd9"


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


# ---------------------------------------------------------------------------
# index_partition — sidecar creation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_creates_xmp_sidecar(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """index_partition writes an XMP sidecar next to the photo."""
    await index_partition(backend_with_sample, "")
    assert (tmpdir / "001.xmp").exists()


@pytest.mark.asyncio
async def test_index_sidecar_has_content_hash(
    backend_with_sample: LocalBackend, tmpdir: Path
) -> None:
    """The created XMP sidecar contains an ouestcharlie:contentHash."""
    await index_partition(backend_with_sample, "")
    sidecar = parse_xmp((tmpdir / "001.xmp").read_text(encoding="utf-8"))
    assert sidecar.content_hash is not None
    assert len(sidecar.content_hash) == 22


@pytest.mark.asyncio
async def test_index_sidecar_has_camera_fields(
    backend_with_sample: LocalBackend, tmpdir: Path
) -> None:
    """The created XMP sidecar contains make/model extracted from EXIF."""
    await index_partition(backend_with_sample, "")
    sidecar = parse_xmp((tmpdir / "001.xmp").read_text(encoding="utf-8"))
    assert sidecar.camera_make is not None
    assert sidecar.camera_model is not None


# ---------------------------------------------------------------------------
# index_partition — leaf manifest
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_creates_leaf_manifest(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """index_partition writes the leaf manifest at .ouestcharlie/manifest.json."""
    await index_partition(backend_with_sample, "")
    manifest_file = tmpdir / METADATA_DIR / "manifest.json"
    assert manifest_file.exists()


@pytest.mark.asyncio
async def test_index_manifest_photo_entry(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """The leaf manifest contains a photo entry with the correct filename and hash."""
    await index_partition(backend_with_sample, "")
    manifest_file = tmpdir / METADATA_DIR / "manifest.json"
    data = json.loads(manifest_file.read_text(encoding="utf-8"))
    assert len(data["photos"]) == 1
    entry = data["photos"][0]
    assert entry["filename"] == "001.jpg"
    assert len(entry["contentHash"]) == 22


@pytest.mark.asyncio
async def test_index_manifest_summary(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """The leaf manifest summary reflects the photo count."""
    await index_partition(backend_with_sample, "")

    manifestStore = ManifestStore(backend_with_sample)
    manifest, _ = await manifestStore.read_leaf("")
    summary = ManifestSummary.from_photos("", manifest.photos)
    assert summary.photo_count == 1


@pytest.mark.asyncio
async def test_index_manifest_has_date(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """The manifest summary has a date range when the photo has EXIF date."""
    await index_partition(backend_with_sample, "")

    manifestStore = ManifestStore(backend_with_sample)
    manifest, _ = await manifestStore.read_leaf("")
    summary = ManifestSummary.from_photos("", manifest.photos)

    # 001.jpg has EXIF DateTimeOriginal
    assert "dateTaken" in summary._stats
    assert "min" in summary._stats["dateTaken"]
    assert "max" in summary._stats["dateTaken"]


@pytest.mark.asyncio
async def test_index_manifest_summary_rating_range(tmpdir: Path) -> None:
    """Leaf manifest summary has ratingMin/ratingMax when photos have ratings."""
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
        sidecar = XmpSidecar(content_hash=f"sha256:{'0' * 63}{call_count}", rating=r)
        entry = PhotoEntry.from_sidecar(
            photo_path.split("/")[-1], sidecar, sidecar.content_hash, "1"
        )
        return entry, True

    with patch("whitebeard.indexer._extract_one", side_effect=fake_process):
        await index_partition(backend, "")

    manifestStore = ManifestStore(backend)
    manifest, _ = await manifestStore.read_leaf("")
    summary = ManifestSummary.from_photos("", manifest.photos)
    assert summary._stats["rating"]["min"] == 2
    assert summary._stats["rating"]["max"] == 5


@pytest.mark.asyncio
async def test_index_manifest_summary_no_rating_when_unrated(tmpdir: Path) -> None:
    """ratingMin/ratingMax are absent from the summary when no photo has a rating."""
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_partition(backend, "")

    manifestStore = ManifestStore(backend)
    manifest, _ = await manifestStore.read_leaf("")
    summary = ManifestSummary.from_photos("", manifest.photos)

    assert "rating" not in summary._stats


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
    # First index run creates the sidecar.
    await index_partition(backend_with_sample, "")

    # Overwrite the sidecar with a sentinel.
    xmp_path = tmpdir / "001.xmp"
    # original_content = xmp_path.read_text(encoding="utf-8")
    xmp_path.write_text("<!-- overwritten -->", encoding="utf-8")

    result = await index_partition(backend_with_sample, "", force_extract_exif=True)

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
    data = json.loads((tmpdir / METADATA_DIR / "manifest.json").read_text())
    assert len(data["photos"]) == 1
    assert data["photos"][0]["filename"] == "top.jpg"


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
    manifest_file = tmpdir / METADATA_DIR / "Vacations" / "Italy" / "manifest.json"
    assert manifest_file.exists()
    data = json.loads(manifest_file.read_text())
    assert data["partition"] == "Vacations/Italy"


# ---------------------------------------------------------------------------
# index_library — recursive indexing and parent manifests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_library_single_partition(tmpdir: Path) -> None:
    """index_library with photos only at root creates a leaf manifest, no parent."""
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_library(backend)

    assert isinstance(result, LibraryIndexResult)
    assert len(result.partitions) == 1
    assert result.total_photos == 1
    # Leaf manifest exists.
    assert (tmpdir / METADATA_DIR / "manifest.json").exists()
    # No parent manifest (nothing to summarise above a single leaf at root).
    # The root IS the leaf, so no deeper parent manifest is needed.


@pytest.mark.asyncio
async def test_index_partition_writes_summary_json(tmpdir: Path) -> None:
    """index_partition creates summary.json at the backend root."""
    (tmpdir / "2024" / "2024-07").mkdir(parents=True)
    (tmpdir / "2024" / "2024-07" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_partition(backend, "2024/2024-07")

    summary_file = tmpdir / ".ouestcharlie" / "summary.json"
    assert summary_file.exists()
    data = json.loads(summary_file.read_text())
    assert len(data["partitions"]) == 1
    assert data["partitions"][0]["path"] == "2024/2024-07"
    assert data["partitions"][0]["photoCount"] == 1


@pytest.mark.asyncio
async def test_index_partition_updates_existing_summary(tmpdir: Path) -> None:
    """Re-indexing a partition replaces its entry in summary.json."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "A" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_partition(backend, "A")
    await index_partition(backend, "A")  # second run — should not duplicate

    data = json.loads((tmpdir / ".ouestcharlie" / "summary.json").read_text())
    assert len(data["partitions"]) == 1
    assert data["partitions"][0]["photoCount"] == 2


@pytest.mark.asyncio
async def test_index_library_writes_summary_json(tmpdir: Path) -> None:
    """index_library produces summary.json listing all indexed partitions."""
    (tmpdir / "2024" / "2024-07").mkdir(parents=True)
    (tmpdir / "2024" / "2024-08").mkdir(parents=True)
    (tmpdir / "2024" / "2024-07" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "2024" / "2024-08" / "b.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_library(backend)

    assert result.total_photos == 2
    # All traversed directories get a manifest (including intermediate ones).
    assert (tmpdir / METADATA_DIR / "2024" / "2024-07" / "manifest.json").exists()
    assert (tmpdir / METADATA_DIR / "2024" / "2024-08" / "manifest.json").exists()
    assert (tmpdir / METADATA_DIR / "2024" / "manifest.json").exists()
    # summary.json lists all indexed partitions (photo-bearing and intermediate).
    data = json.loads((tmpdir / ".ouestcharlie" / "summary.json").read_text())
    paths = {p["path"] for p in data["partitions"]}
    assert "2024/2024-07" in paths
    assert "2024/2024-08" in paths


@pytest.mark.asyncio
async def test_index_library_all_dirs_get_manifest(tmpdir: Path) -> None:
    """All traversed directories get a manifest, including intermediate ones."""
    (tmpdir / "2024" / "July" / "Vacation").mkdir(parents=True)
    shutil.copy(_SAMPLE_JPG, tmpdir / "2024" / "July" / "Vacation" / "001.jpg")
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)

    # Every directory in the tree gets a manifest.json.
    assert (tmpdir / METADATA_DIR / "2024" / "July" / "Vacation" / "manifest.json").exists()
    assert (tmpdir / METADATA_DIR / "2024" / "July" / "manifest.json").exists()
    assert (tmpdir / METADATA_DIR / "2024" / "manifest.json").exists()
    # summary.json exists at root.
    assert (tmpdir / METADATA_DIR / "summary.json").exists()


@pytest.mark.asyncio
async def test_index_library_summary_rating_range(tmpdir: Path) -> None:
    """summary.json contains per-partition rating ranges."""
    from ouestcharlie_toolkit.schema import XmpSidecar
    from ouestcharlie_toolkit.xmp import serialize_xmp

    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "A" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B").mkdir()
    (tmpdir / "B" / "p3.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    for photo, rating in [("A/p1.jpg", 2), ("A/p2.jpg", 4), ("B/p3.jpg", 5)]:
        sidecar = XmpSidecar(content_hash=f"sha256:{'0' * 63}{rating}", rating=rating)
        xmp_file = tmpdir / photo.replace(".jpg", ".xmp")
        xmp_file.write_text(serialize_xmp(sidecar), encoding="utf-8")

    await index_library(backend)

    data = json.loads((tmpdir / ".ouestcharlie" / "summary.json").read_text())
    part_a = next(p for p in data["partitions"] if p["path"] == "A")
    part_b = next(p for p in data["partitions"] if p["path"] == "B")
    assert part_a["rating"]["min"] == 2
    assert part_a["rating"]["max"] == 4
    assert part_b["rating"]["min"] == 5
    assert part_b["rating"]["max"] == 5


@pytest.mark.asyncio
async def test_index_library_summary_photo_count(tmpdir: Path) -> None:
    """summary.json photoCount per partition matches actual photo count."""
    (tmpdir / "A").mkdir()
    (tmpdir / "B").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "A" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B" / "p3.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    await index_library(backend)

    data = json.loads((tmpdir / ".ouestcharlie" / "summary.json").read_text())
    counts = {p["path"]: p["photoCount"] for p in data["partitions"]}
    assert counts["A"] == 2
    assert counts["B"] == 1


@pytest.mark.asyncio
async def test_index_library_result_totals(tmpdir: Path) -> None:
    """LibraryIndexResult aggregates counts across all partitions."""
    (tmpdir / "A").mkdir()
    (tmpdir / "B").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=tmpdir)

    result = await index_library(backend)

    assert result.total_photos == 2
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

    assert result2.total_photos == 1
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

    from ouestcharlie_toolkit.schema import PhotoEntry, VersionToken, XmpSidecar

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
        sidecar = XmpSidecar(content_hash=f"sha256:{'0' * 64}", date_taken=dt)
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
