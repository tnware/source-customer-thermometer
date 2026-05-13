# Changelog

## [Unreleased]

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
  `ghcr.io/powercts/source-ct-api`.

## [0.1.0] — earlier
- Initial extraction from the PowerCTS reporting monorepo.
- Single stream `thermometer_responses`, incremental by `response_date`.
