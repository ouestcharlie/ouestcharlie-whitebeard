"""Tests for Whitebeard indexer — core indexing logic."""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.schema import METADATA_DIR, manifest_path
from ouestcharlie_toolkit.xmp import parse_xmp, xmp_path_for

from whitebeard.indexer import (
    PHOTO_EXTENSIONS,
    IndexResult,
    LibraryIndexResult,
    index_library,
    index_partition,
)

# Sample JPEG from the py-toolkit test suite.
_SAMPLE_JPG = (
    Path(__file__).parent.parent.parent
    / "ouestcharlie-py-toolkit"
    / "tests"
    / "sample-images"
    / "001.jpg"
)

# Minimal valid JPEG (SOI + JFIF APP0 + EOI) — no EXIF data.
_MINIMAL_JPEG = (
    b"\xff\xd8"
    b"\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    b"\xff\xd9"
)


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
    return LocalBackend(root=str(tmpdir))


@pytest.fixture()
def backend_with_minimal(tmpdir: Path) -> LocalBackend:
    """Backend rooted at a temp dir that contains a minimal JPEG (no EXIF)."""
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    return LocalBackend(root=str(tmpdir))


# ---------------------------------------------------------------------------
# index_partition — sidecar creation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_creates_xmp_sidecar(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """index_partition writes an XMP sidecar next to the photo."""
    await index_partition(backend_with_sample, "")
    assert (tmpdir / "001.xmp").exists()


@pytest.mark.asyncio
async def test_index_sidecar_has_content_hash(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """The created XMP sidecar contains an ouestcharlie:contentHash."""
    await index_partition(backend_with_sample, "")
    sidecar = parse_xmp((tmpdir / "001.xmp").read_text(encoding="utf-8"))
    assert sidecar.content_hash is not None
    assert sidecar.content_hash.startswith("sha256:")


@pytest.mark.asyncio
async def test_index_sidecar_has_camera_fields(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
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
    assert entry["contentHash"].startswith("sha256:")


@pytest.mark.asyncio
async def test_index_manifest_summary(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """The leaf manifest summary reflects the photo count."""
    await index_partition(backend_with_sample, "")
    data = json.loads((tmpdir / METADATA_DIR / "manifest.json").read_text())
    assert data["summary"]["photoCount"] == 1


@pytest.mark.asyncio
async def test_index_manifest_has_date(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """The manifest summary has a date range when the photo has EXIF date."""
    await index_partition(backend_with_sample, "")
    data = json.loads((tmpdir / METADATA_DIR / "manifest.json").read_text())
    # 001.jpg has EXIF DateTimeOriginal
    assert "dateTaken" in data["summary"]
    assert "min" in data["summary"]["dateTaken"]
    assert "max" in data["summary"]["dateTaken"]


@pytest.mark.asyncio
async def test_index_manifest_summary_rating_range(tmpdir: Path) -> None:
    """Leaf manifest summary has ratingMin/ratingMax when photos have ratings."""
    from ouestcharlie_toolkit.schema import XmpSidecar, VersionToken
    from whitebeard.indexer import _sidecar_to_entry

    (tmpdir / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "b.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "c.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

    ratings = [2, 5, 4]
    call_count = 0

    async def fake_process(xmp_store, photo_path, force_extract_exif):
        nonlocal call_count
        r = ratings[call_count]
        call_count += 1
        sidecar = XmpSidecar(content_hash=f"sha256:{'0' * 63}{call_count}", rating=r)
        entry = _sidecar_to_entry(photo_path.split("/")[-1], sidecar, sidecar.content_hash, "1")
        return entry, True

    with patch("whitebeard.indexer._extract_one", side_effect=fake_process):
        result = await index_partition(backend, "")

    assert result.summary is not None
    assert result.summary.rating["min"] == 2
    assert result.summary.rating["max"] == 5

    data = json.loads((tmpdir / METADATA_DIR / "manifest.json").read_text())
    assert data["summary"]["rating"]["min"] == 2
    assert data["summary"]["rating"]["max"] == 5


@pytest.mark.asyncio
async def test_index_manifest_summary_no_rating_when_unrated(tmpdir: Path) -> None:
    """ratingMin/ratingMax are absent from the summary when no photo has a rating."""
    (tmpdir / "photo.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

    await index_partition(backend, "")

    data = json.loads((tmpdir / METADATA_DIR / "manifest.json").read_text())
    assert "ratingMin" not in data["summary"]
    assert "ratingMax" not in data["summary"]


# ---------------------------------------------------------------------------
# index_partition — skip / force behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_skips_existing_sidecar(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """Without force, an existing XMP sidecar is not overwritten."""
    sentinel = "<!-- sentinel -->"
    xmp_path = tmpdir / "001.xmp"
    xmp_path.write_text(sentinel, encoding="utf-8")

    result = await index_partition(backend_with_sample, "")

    assert xmp_path.read_text(encoding="utf-8") == sentinel
    assert result.sidecars_skipped == 1
    assert result.sidecars_created == 0


@pytest.mark.asyncio
async def test_index_force_overwrites_sidecar(backend_with_sample: LocalBackend, tmpdir: Path) -> None:
    """With force=True, an existing XMP sidecar is replaced with fresh EXIF data."""
    # First index run creates the sidecar.
    await index_partition(backend_with_sample, "")

    # Overwrite the sidecar with a sentinel.
    xmp_path = tmpdir / "001.xmp"
    original_content = xmp_path.read_text(encoding="utf-8")
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
    backend = LocalBackend(root=str(tmpdir))

    result = await index_partition(backend, "")

    assert result.photos_processed == 1  # only photo.jpg


@pytest.mark.asyncio
async def test_index_ignores_subdirectory_photos(tmpdir: Path) -> None:
    """Photos in subdirectories are NOT indexed as part of the parent partition."""
    subdir = tmpdir / "subdir"
    subdir.mkdir()
    (subdir / "deep.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "top.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

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
    backend = LocalBackend(root=str(tmpdir))

    result = await index_partition(backend, "Vacations/Italy")

    assert result.photos_processed == 1
    assert result.sidecars_created == 1
    manifest_file = tmpdir / "Vacations" / "Italy" / METADATA_DIR / "manifest.json"
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
    backend = LocalBackend(root=str(tmpdir))

    result = await index_library(backend)

    assert isinstance(result, LibraryIndexResult)
    assert len(result.partitions) == 1
    assert result.total_photos == 1
    # Leaf manifest exists.
    assert (tmpdir / METADATA_DIR / "manifest.json").exists()
    # No parent manifest (nothing to summarise above a single leaf at root).
    # The root IS the leaf, so no deeper parent manifest is needed.


@pytest.mark.asyncio
async def test_index_library_builds_parent_manifest(tmpdir: Path) -> None:
    """Two leaf partitions under a common parent produce a parent manifest."""
    (tmpdir / "2024" / "2024-07").mkdir(parents=True)
    (tmpdir / "2024" / "2024-08").mkdir(parents=True)
    (tmpdir / "2024" / "2024-07" / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "2024" / "2024-08" / "b.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

    result = await index_library(backend)

    assert result.total_photos == 2
    assert len(result.partitions) == 2

    # Leaf manifests.
    assert (tmpdir / "2024" / "2024-07" / METADATA_DIR / "manifest.json").exists()
    assert (tmpdir / "2024" / "2024-08" / METADATA_DIR / "manifest.json").exists()

    # Parent manifest for 2024/.
    parent_file = tmpdir / "2024" / METADATA_DIR / "manifest.json"
    assert parent_file.exists()
    parent_data = json.loads(parent_file.read_text())
    assert len(parent_data["children"]) == 2
    child_paths = {c["path"] for c in parent_data["children"]}
    assert "2024/2024-07" in child_paths
    assert "2024/2024-08" in child_paths

    # Root parent manifest.
    root_file = tmpdir / METADATA_DIR / "manifest.json"
    assert root_file.exists()
    root_data = json.loads(root_file.read_text())
    assert any(c["path"] == "2024" for c in root_data["children"])


@pytest.mark.asyncio
async def test_index_library_three_levels(tmpdir: Path) -> None:
    """Three-level hierarchy: root → year → month → photos."""
    (tmpdir / "2024" / "July" / "Vacation").mkdir(parents=True)
    shutil.copy(_SAMPLE_JPG, tmpdir / "2024" / "July" / "Vacation" / "001.jpg")
    backend = LocalBackend(root=str(tmpdir))

    await index_library(backend)

    # Leaf at 2024/July/Vacation.
    assert (tmpdir / "2024" / "July" / "Vacation" / METADATA_DIR / "manifest.json").exists()
    # Parent at 2024/July.
    assert (tmpdir / "2024" / "July" / METADATA_DIR / "manifest.json").exists()
    # Parent at 2024.
    assert (tmpdir / "2024" / METADATA_DIR / "manifest.json").exists()
    # Root parent.
    assert (tmpdir / METADATA_DIR / "manifest.json").exists()


@pytest.mark.asyncio
async def test_index_library_parent_rating_range(tmpdir: Path) -> None:
    """Parent manifest ratingMin/ratingMax aggregate across child partitions."""
    from ouestcharlie_toolkit.schema import XmpSidecar, VersionToken
    from whitebeard.indexer import _sidecar_to_entry
    from ouestcharlie_toolkit.xmp import serialize_xmp

    # Partition A: photos rated 2 and 4 → ratingMin=2, ratingMax=4
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "A" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    # Partition B: photo rated 5 → ratingMin=5, ratingMax=5
    (tmpdir / "B").mkdir()
    (tmpdir / "B" / "p3.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

    # Write XMP sidecars with known ratings before indexing.
    for photo, rating in [("A/p1.jpg", 2), ("A/p2.jpg", 4), ("B/p3.jpg", 5)]:
        sidecar = XmpSidecar(
            content_hash=f"sha256:{'0' * 63}{rating}",
            rating=rating,
        )
        xmp_path = tmpdir / photo.replace(".jpg", ".xmp")
        xmp_path.write_text(serialize_xmp(sidecar), encoding="utf-8")

    await index_library(backend)

    root_data = json.loads((tmpdir / METADATA_DIR / "manifest.json").read_text())
    child_a = next(c for c in root_data["children"] if c["path"] == "A")
    child_b = next(c for c in root_data["children"] if c["path"] == "B")

    assert child_a["rating"]["min"] == 2
    assert child_a["rating"]["max"] == 4
    assert child_b["rating"]["min"] == 5
    assert child_b["rating"]["max"] == 5

    # Root-level aggregation: min/max across all children entries.
    all_mins = [c["rating"]["min"] for c in root_data["children"] if "rating" in c]
    all_maxes = [c["rating"]["max"] for c in root_data["children"] if "rating" in c]
    assert min(all_mins) == 2
    assert max(all_maxes) == 5


@pytest.mark.asyncio
async def test_index_library_parent_photo_count(tmpdir: Path) -> None:
    """Parent manifest photoCount aggregates all children."""
    (tmpdir / "A").mkdir()
    (tmpdir / "B").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "A" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B" / "p3.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

    await index_library(backend)

    root_data = json.loads((tmpdir / METADATA_DIR / "manifest.json").read_text())
    total = sum(c["photoCount"] for c in root_data["children"])
    assert total == 3


@pytest.mark.asyncio
async def test_index_library_result_totals(tmpdir: Path) -> None:
    """LibraryIndexResult aggregates counts across all partitions."""
    (tmpdir / "A").mkdir()
    (tmpdir / "B").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

    result = await index_library(backend)

    assert result.total_photos == 2
    assert result.total_sidecars_created == 2
    assert result.total_errors == 0


@pytest.mark.asyncio
async def test_index_library_idempotent(tmpdir: Path) -> None:
    """Running index_library twice with the same library is idempotent."""
    (tmpdir / "A").mkdir()
    (tmpdir / "A" / "p.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

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
    backend = LocalBackend(root=str(tmpdir))

    with patch(
        "whitebeard.indexer._extract_one",
        side_effect=RuntimeError("simulated failure"),
    ):
        with caplog.at_level(logging.ERROR, logger="whitebeard.indexer"):
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
    from datetime import timezone, timedelta
    from ouestcharlie_toolkit.schema import XmpSidecar, VersionToken

    naive_dt = datetime(2024, 7, 1, 12, 0, 0)
    aware_dt = datetime(2024, 7, 2, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))

    (tmpdir / "a.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "b.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

    call_count = 0

    async def fake_process(xmp_store, photo_path, force_extract_exif):
        nonlocal call_count
        call_count += 1
        dt = naive_dt if call_count == 1 else aware_dt
        sidecar = XmpSidecar(content_hash=f"sha256:{'0' * 64}", date_taken=dt)
        token = VersionToken(value=1)
        from whitebeard.indexer import _sidecar_to_entry
        entry = _sidecar_to_entry(photo_path.split("/")[-1], sidecar, sidecar.content_hash, str(token.value))
        return entry, True

    with patch("whitebeard.indexer._extract_one", side_effect=fake_process):
        result = await index_partition(backend, "")

    assert result.errors == 0
    assert result.summary is not None
    assert result.summary.dateTaken is not None
    assert result.summary.dateTaken["min"] is not None
    assert result.summary.dateTaken["max"] is not None


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
    """LibraryIndexResult.total_duration_ms sums durations across partitions."""
    (tmpdir / "A").mkdir()
    (tmpdir / "B").mkdir()
    (tmpdir / "A" / "p1.jpg").write_bytes(_MINIMAL_JPEG)
    (tmpdir / "B" / "p2.jpg").write_bytes(_MINIMAL_JPEG)
    backend = LocalBackend(root=str(tmpdir))

    result = await index_library(backend)

    assert result.total_duration_ms == sum(p.duration_ms for p in result.partitions)
    assert result.total_duration_ms >= 0
