# Changelog

## [0.5.0](https://github.com/tnware/source-customer-thermometer/compare/v0.4.0...v0.5.0) (2026-05-13)


### ⚠ BREAKING CHANGES

* Docker image path, Python package import, and connector definitionId all change. No external consumers yet (only configured locally), so blast radius is the local Airbyte connection that hasn't been wired up.

### Features

* rename connector to source-customer-thermometer ([6bc8353](https://github.com/tnware/source-customer-thermometer/commit/6bc83538d8ef78754c7fff58d36131986a8c5b35))

## [Unreleased]

> **Repo + package renamed to `source-customer-thermometer`** (was
> `source-ct-api`). New Docker image path:
> `ghcr.io/tnware/source-customer-thermometer`. The old path
> (`ghcr.io/tnware/source-ct-api:0.4.0` and earlier) remains pullable
> for anything pinned to it. Release-please will format this into a
> proper entry on the next release cut.

## [0.4.0] — 2026-05-13

### Fixed
- `metadata.yaml` `dockerRepository` corrected from
  `ghcr.io/powercts/source-ct-api` to `ghcr.io/tnware/source-ct-api`
  to match the actual GHCR location (the GHA workflow uses
  `github.repository_owner` which resolves to `tnware`). Published
  v0.3.0 images landed at the correct path; only the metadata pointer
  was wrong.
- README references updated to the correct GHCR path and the correct
  GitHub repo URL.

## [0.3.0] — 2026-05-13

### Added
- **Pytest suite** (21 tests) covering `_validate_start_date` (valid /
  future-rejected / garbage-rejected / None-rejected), `_map_fields`
  (full row, boolean coercions, response_id zero/missing/garbage,
  `_int` helper), `get_updated_state` cursor logic, and `read_records`
  (happy path, empty response, **10k cap raises**, non-XML raises).
- CI gate: `image.yml` now runs `pytest` in a `test` job that the
  `build` job depends on. Failing tests block the image push.
- `[project.optional-dependencies] dev = ["pytest>=7.0", "responses>=0.23"]`
  in pyproject.toml.

### Changed
- **Enriched `thermometer_responses.json` schema**: every field has a
  `description`; `response_date` uses `format: date-time` +
  `airbyte_type: timestamp_with_timezone` so Airbyte's typed
  destination creates a `TIMESTAMPTZ` Postgres column instead of `TEXT`.
- Documented the custom-field semantics: `custom_1 → ticket_ref`,
  `custom_2 → technician_name`, `custom_3 → ticket_subject`.

## [0.2.0] — 2026-05-13

### Changed
- **Raise on API 10k record cap** instead of logging a warning and
  silently dropping records. The previous behavior could produce wrong
  KPI numbers if a sync window contained more than 10,000 responses;
  the new behavior fails loudly. README documents the recovery path.
- `start_date` is now validated at construction time. Future dates and
  unparseable values are rejected with a clear error.
- XML parse errors are now wrapped in a `RuntimeError` with a clear
  message instead of bubbling up as opaque `ParseError`.
- Bumped `dockerRepository` in metadata.yaml to
  `ghcr.io/powercts/source-ct-api` (corrected to `tnware/source-ct-api` in v0.4.0).

## [0.1.0] — earlier
- Initial extraction from the PowerCTS reporting monorepo.
- Single stream `thermometer_responses`, incremental by `response_date`.
