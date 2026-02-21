"""Tests for Whitebeard indexer — core indexing logic."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest

from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.schema import METADATA_DIR, manifest_path
from ouestcharlie_toolkit.xmp import parse_xmp, xmp_path_for

from whitebeard.indexer import PHOTO_EXTENSIONS, IndexResult, index_partition

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
    """The manifest summary has dateMin/dateMax when the photo has EXIF date."""
    await index_partition(backend_with_sample, "")
    data = json.loads((tmpdir / METADATA_DIR / "manifest.json").read_text())
    # 001.jpg has EXIF DateTimeOriginal
    assert "dateMin" in data["summary"]
    assert "dateMax" in data["summary"]


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

    result = await index_partition(backend_with_sample, "", force=True)

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
