# source-ct-api

Airbyte custom source connector for the [CustomerThermometer](https://www.customerthermometer.com/) CSAT API.

## Streams

| Stream                  | Sync mode               | Notes                                                                                  |
| ----------------------- | ----------------------- | -------------------------------------------------------------------------------------- |
| `thermometer_responses` | incremental + full      | Survey responses. Incremental by `response_date`. Single API call per sync.            |

## Authentication

API key, found in **CT admin panel → Account Settings → API**.

## Configuration

| Field        | Required | Description                                                                                                   |
| ------------ | -------- | ------------------------------------------------------------------------------------------------------------- |
| `api_key`    | yes      | CustomerThermometer API key. Marked as Airbyte secret.                                                        |
| `start_date` | yes      | `YYYY-MM-DD`. Earliest response_date to fetch on the first sync. Validated; rejected if in the future or malformed. |

## API behavior worth knowing

The CustomerThermometer API caps each call at **10,000 records** and does
not (as of this writing) support pagination — no offset, no cursor token,
no `next_page` link. This is upstream behavior, not something the
connector can work around.

### How this connector handles the cap

If a single sync window returns >= 10,000 records, the connector **raises
and fails the sync**. The alternative (silently emitting only the first
10,000) would produce wrong KPI numbers downstream.

### How to recover from a hit cap

Most likely scenario: your first sync covers a long enough history to
exceed the cap.

1. Lower `start_date` so the window is smaller (e.g. one quarter at a
   time). Sync, let the cursor advance.
2. Once caught up, ongoing incremental syncs should stay well under the
   cap (a CSAT response stream is rarely high-volume).

If you regularly hit the cap on a steady-state incremental sync (i.e.
more than 10,000 responses between syncs), you'd need to sync more
frequently or push CustomerThermometer to support pagination.

## Build + push

```bash
docker build -t ghcr.io/tnware/source-ct-api:dev .
```

CI in `.github/workflows/image.yml` builds and pushes to ghcr.io on
push to main and on tags.

## Local testing

```bash
# Spec
python -m source_ct_api spec

# Connection check
python -m source_ct_api check --config /path/to/secrets/config.json

# Catalog discovery
python -m source_ct_api discover --config /path/to/secrets/config.json

# Read records
python -m source_ct_api read \
  --config /path/to/secrets/config.json \
  --catalog /path/to/integration_tests/configured_catalog.json
```

Example `config.json`:

```json
{
  "api_key": "...",
  "start_date": "2025-07-01"
}
```

## Pointing Airbyte at this connector

In the Airbyte UI:

1. **Settings → Sources → New connector**
2. **Add a new Docker connector**
3. Image name: `ghcr.io/tnware/source-ct-api`
4. Image tag: a published tag (see [Releases](https://github.com/tnware/source-ct-api/releases))

## Releases & commit style

Releases are automated by [release-please](https://github.com/googleapis/release-please).
Commit to `main` using [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add new Foo endpoint        → minor bump (0.4.0 → 0.5.0)
fix:  handle null api_key         → patch bump (0.4.0 → 0.4.1)
chore: bump action versions       → no release (hidden from changelog)
feat!: drop deprecated cursor     → major bump (note the !)
```

Release-please watches `main` and maintains a rolling **Release PR** that:
- Bumps `pyproject.toml` `version` and `metadata.yaml` `dockerImageTag`
  in lockstep
- Appends a categorized entry to `CHANGELOG.md`
- When merged, cuts a `vX.Y.Z` tag and a GitHub Release. The
  `image.yml` workflow picks up the tag and builds + pushes the
  versioned image.

Don't hand-edit versions or the CHANGELOG — release-please owns those.

## CHANGELOG

See [CHANGELOG.md](CHANGELOG.md).

## License

MIT — see [LICENSE](LICENSE).
