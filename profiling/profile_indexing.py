"""Standalone profiling script for index_partition.

Configuration is read from profiling/.env:
    BACKEND_ROOT=<path to photo library root>
    PARTITION=<partition path relative to backend root>

Usage:
    .venv/bin/python profiling/profile_indexing.py

Output: prints step-level summary to stdout, saves full cProfile to
profiling/results/profile_indexing_<partition_slug>.txt.
"""

import asyncio
import cProfile
import io
import pstats
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path, PurePath

from dotenv import dotenv_values
from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.xmp import XmpStore

from whitebeard.indexer import (
    PHOTO_EXTENSIONS,
    _extract_one,
    _upsert_leaf_manifest,
)


class TimingBackend:
    """Wraps a Backend and accumulates time spent in each method."""

    def __init__(self, inner):
        self._inner = inner
        self.totals: dict[str, float] = defaultdict(float)
        self.counts: dict[str, int] = defaultdict(int)

    def _t(self, name: str):
        class _ctx:
            def __enter__(ctx):
                ctx._t0 = time.perf_counter()

            def __exit__(ctx, *_):
                self.totals[name] += time.perf_counter() - ctx._t0
                self.counts[name] += 1

        return _ctx()

    # ── Forwarded methods ─────────────────────────────────────────────────────

    async def list_dirs(self, prefix: str):
        with self._t("list_dirs"):
            return await self._inner.list_dirs(prefix)

    async def list_files(self, prefix: str, suffixes=None):
        with self._t("list_files"):
            return await self._inner.list_files(prefix, suffixes)

    async def read(self, path: str):
        with self._t("read"):
            return await self._inner.read(path)

    async def exists(self, path: str) -> bool:
        with self._t("exists"):
            return await self._inner.exists(path)

    async def delete(self, path: str) -> None:
        with self._t("delete"):
            return await self._inner.delete(path)

    async def write_new(self, path: str, data: bytes):
        with self._t("write_new"):
            return await self._inner.write_new(path, data)

    async def write_conditional(self, path: str, data: bytes, expected_version):
        with self._t("write_conditional"):
            return await self._inner.write_conditional(path, data, expected_version)

    # Pass through any attributes not intercepted (e.g. root path)
    def __getattr__(self, name):
        return getattr(self._inner, name)

    def report(self) -> str:
        lines = ["  Backend I/O breakdown:"]
        for op in ("list_files", "read", "write_new", "write_conditional", "exists"):
            ms = self.totals[op] * 1000
            n = self.counts[op]
            if n:
                lines.append(f"    {op:<20} {ms:7.1f} ms  ({n}×  avg {ms / n:.1f} ms)")
        total_ms = sum(self.totals.values()) * 1000
        lines.append(f"    {'TOTAL':<20} {total_ms:7.1f} ms")
        return "\n".join(lines)


async def profile_steps(backend_root: str, partition: str) -> None:
    inner = LocalBackend(backend_root)
    backend = TimingBackend(inner)
    xmp_store = XmpStore(backend)
    manifest_store = ManifestStore(backend)

    # ── Step 1: Discovery ────────────────────────────────────────────────────
    t0 = time.perf_counter()
    photo_files = await backend.list_files(partition, PHOTO_EXTENSIONS)
    t_discovery = time.perf_counter() - t0
    n = len(photo_files)
    print(f"Discovery:  {t_discovery * 1000:6.1f} ms  ({n} photos)")

    # ── Step 2: EXIF extraction + XMP write (force) ──────────────────────────
    backend.totals.clear()
    backend.counts.clear()
    photo_entries = []
    t_exif_total = 0.0
    per_photo = []
    for fi in photo_files:
        t = time.perf_counter()
        try:
            entry, _ = await _extract_one(xmp_store, fi.path, force_extract_exif=True)
            photo_entries.append(entry)
        except Exception as exc:
            print(f"  ERROR {PurePath(fi.path).name}: {exc}")
        elapsed = time.perf_counter() - t
        t_exif_total += elapsed
        per_photo.append((PurePath(fi.path).name, elapsed * 1000))

    print(f"EXIF+XMP:   {t_exif_total * 1000:6.1f} ms  total")
    if n:
        print(f"            {t_exif_total * 1000 / n:6.1f} ms  avg/photo")
    per_photo.sort(key=lambda x: -x[1])
    print("  Slowest 5 photos:")
    for name, ms in per_photo[:5]:
        print(f"    {ms:6.1f} ms  {name}")
    print(backend.report())

    # ── Step 3: Thumbnail generation ─────────────────────────────────────────
    from ouestcharlie_toolkit.thumbnail_builder import generate_partition_thumbnails

    backend.totals.clear()
    backend.counts.clear()
    t0 = time.perf_counter()
    thumbnail_result = await generate_partition_thumbnails(
        backend, partition, photo_entries, tier="thumbnail"
    )
    t_thumbnails = time.perf_counter() - t0
    print(f"Thumbnails: {t_thumbnails * 1000:6.1f} ms")
    print(backend.report())

    # ── Step 4: Manifest write ────────────────────────────────────────────────
    backend.totals.clear()
    backend.counts.clear()
    t0 = time.perf_counter()
    summary = await _upsert_leaf_manifest(
        manifest_store, partition, photo_entries, thumbnail_result
    )
    t_manifest = time.perf_counter() - t0
    print(f"Manifest:   {t_manifest * 1000:6.1f} ms")
    print(backend.report())

    # ── Step 5: Summary.json update ──────────────────────────────────────────
    backend.totals.clear()
    backend.counts.clear()
    t0 = time.perf_counter()
    await manifest_store.upsert_partition_in_summary(summary)
    t_summary = time.perf_counter() - t0
    print(f"Summary:    {t_summary * 1000:6.1f} ms")
    print(backend.report())

    total = t_discovery + t_exif_total + t_thumbnails + t_manifest + t_summary
    print(f"\nTotal:      {total * 1000:6.1f} ms")


def main() -> None:
    env_file = Path(__file__).parent / ".env"
    env = dotenv_values(env_file)
    backend_root = env.get("BACKEND_ROOT", "")
    partition = env.get("PARTITION", "")
    if not backend_root or not partition:
        print(f"Set BACKEND_ROOT and PARTITION in {env_file}")
        sys.exit(1)

    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)
    slug = partition.replace("/", "_").replace(" ", "-")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = results_dir / f"profile_indexing_{slug}_{ts}.txt"

    header = (
        f"=== Step-level timing (force EXIF) ===\nBackend: {backend_root}  Partition: {partition}\n"
    )
    print(f"\n{header}")
    asyncio.run(profile_steps(backend_root, partition))

    print("\n\n=== cProfile (top 40 by own time) ===\n")
    pr = cProfile.Profile()
    pr.enable()
    asyncio.run(profile_steps(backend_root, partition))
    pr.disable()

    s = io.StringIO()
    pstats.Stats(pr, stream=s).sort_stats("tottime").print_stats(40)
    cprofile_output = s.getvalue()
    print(cprofile_output)

    # ── Save full output to file ──────────────────────────────────────────────
    # Re-capture stdout by re-running with a StringIO mirror isn't practical,
    # so we write the cProfile dump (which is the verbose part) plus a note.
    with open(out_path, "w") as f:
        f.write(header + "\n")
        f.write("(See terminal for step-level timing with backend breakdown)\n\n")
        f.write("=== cProfile (full, sorted by own time) ===\n\n")
        s2 = io.StringIO()
        pstats.Stats(pr, stream=s2).sort_stats("tottime").print_stats()
        f.write(s2.getvalue())
        f.write("\n\n=== cProfile (sorted by cumulative time) ===\n\n")
        s3 = io.StringIO()
        pstats.Stats(pr, stream=s3).sort_stats("cumulative").print_stats()
        f.write(s3.getvalue())

    print(f"\nFull profile saved to: {out_path.relative_to(Path(__file__).parent.parent)}")


if __name__ == "__main__":
    main()
