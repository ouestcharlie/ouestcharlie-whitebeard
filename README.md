# Whitebeard: Photo library indexer

Whitebeard is the **indexing agent** for OuEstCharlie. It operates in **index mode**:
scan an existing local photo library in place (no files moved), create XMP sidecars
with `ouestcharlie:` fields, and write leaf manifests.

MCP tool exposed: `index_partition(partition, force=False)`.

## Project Layout

```
src/whitebeard/
  indexer.py    — core logic (no MCP dependency, independently testable)
  agent.py      — WhitebeardAgent(AgentBase) registering MCP tools
  __main__.py   — entry point: python -m whitebeard
tests/
  test_indexer.py
```

## Running Tests

Whitebeard shares the same venv problem as the py-toolkit (rawpy has no macOS x86_64 wheel).

**Venv setup (first time):**
```bash
cd /Users/antoinehue/Code/charlie/ouestcharlie-whitebeard
uv venv
.venv/bin/pip install -e ../ouestcharlie-py-toolkit
.venv/bin/pip install -e ".[dev]"
```

**Running tests:**
```bash
.venv/bin/python -m pytest tests/ -v
```

## Context

| Repository | Purpose |
|------------|---------|
| [ouestcharlie](https://github.com/ouestcharlie/ouestcharlie/) | Architecture docs, HLR/HLD, MCP interface |
| [ouestcharlie-woof](https://github.com/ouestcharlie/ouestcharlie-woof/) | Woof controller |
| [ouestcharlie-py-toolkit](https://github.com/ouestcharlie/ouestcharlie-py-toolkit) | Python toolkit for agents |
| [**ouestcharlie-whitebeard** *This repo*](https://github.com/ouestcharlie/ouestcharlie-whitebeard) | Indexing agent |
| [ouestcharlie-wally](https://github.com/ouestcharlie/ouestcharlie-wally) | Search/consumption agent |