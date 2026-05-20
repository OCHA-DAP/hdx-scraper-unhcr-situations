# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-unhcr-situations** collects refugees and asylum seekers data from [UNHCR Situations](https://data.unhcr.org/en/regions) and creates one dataset in HDX. It is run every week.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.unhcr_situations
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_unhcr_situations.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline flows through several stages in `__main__.py`:

1. **`get_data_from_hdx`** — Reads the existing dataset from HDX to get previous data.

2. **`get_data_from_unhcr`** — Calls the UNHCR API to get current refugee/asylum-seeker data.

3. **`generate_dataset`** — Constructs an HDX `Dataset` object and attaches a resource. Returns `dataset`.

### Key design points

- **One dataset**: the scraper creates/updates a single HDX dataset.
- **`Retrieve`** (`hdx-python-utilities`) abstracts HTTP downloads and supports save/replay via `save=True`/`use_saved=True` — used in tests to replay fixture data from `tests/fixtures/input/`.
- **Static config inside the package**: `config/` lives under `src/hdx/scraper/unhcr_situations/config/` so it is installed with the package and located via `script_dir_plus_file`.

### Config files

- `src/hdx/scraper/unhcr_situations/config/project_configuration.yaml` — API URL and dataset description template
- `src/hdx/scraper/unhcr_situations/config/hdx_dataset_static.yaml` — Static HDX metadata applied to the dataset (license, methodology, source, etc.)

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-unhcr-situations` entry.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
