# Whitebeard: Photo library indexer

Whitebeard is the **indexing agent** for OuEstCharlie. It operates in **index mode**:
scan an existing local photo library in place (no files moved), create XMP sidecars
with `ouestcharlie:` fields, and write leaf manifests.

> **More about OuEstCharlie on the [OuEstCharlie Blog](https://ouestcharlie.github.io/ouestcharlie/)**

MCP tools: `index_partition` and `index_library`, both incremental by default (`force_full_index=False`).

## Project Layout

```
src/whitebeard/
  indexer.py    — core logic (no MCP dependency, independently testable)
  agent.py      — WhitebeardAgent(AgentBase) registering MCP tools
  __main__.py   — entry point: python -m whitebeard
tests/
  test_indexer.py
```

## Installation

### From PyPI (recommended)

```bash
pip install whitebeard
```

### From source (development)

Requires the sibling `ouestcharlie-py-toolkit` repo and a built `image-proc` binary (see its README_DEV.md):

```bash
uv venv
uv sync
```

## Running Tests

**Always use `.venv/bin/python -m pytest`:**

```bash
.venv/bin/python -m pytest tests/ -v
```

## Incremental vs Force Indexing

By default, both `index_partition` and `index_library` run in **incremental mode**: photos already present in the partition manifest are carried over without re-processing. Only photos missing from the manifest are indexed. Photos deleted from disk are automatically removed from the manifest on the next run.

To re-process all photos regardless of their manifest state, pass `force_full_index=True`. To also regenerate XMP sidecars specifically, add `force_extract_exif=True`.

## MCP Inspector

Use `mcp dev` from the repo root with a backend config pointing at a local photo folder:

```bash
WOOF_BACKEND_CONFIG='{"type":"filesystem","root":"/path/to/photos"}' \
    mcp dev src/whitebeard/__main__.py
```

> **Note:** The default MCP Inspector timeout is too low for full-library indexing runs. Increase it in the Inspector settings before calling `index_partition`.

## Context

| Repository | Purpose |
|------------|---------|
| [ouestcharlie](https://github.com/ouestcharlie/ouestcharlie/) | Architecture docs, HLR/HLD, MCP interface |
| [ouestcharlie-woof](https://github.com/ouestcharlie/ouestcharlie-woof/) | Woof controller |
| [ouestcharlie-py-toolkit](https://github.com/ouestcharlie/ouestcharlie-py-toolkit) | Python toolkit for agents |
| [**ouestcharlie-whitebeard** *This repo*](https://github.com/ouestcharlie/ouestcharlie-whitebeard) | Indexing agent |
| [ouestcharlie-wally](https://github.com/ouestcharlie/ouestcharlie-wally) | Search/consumption agent |