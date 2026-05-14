# source-customer-thermometer

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

The CustomerThermometer API has no pagination — no offset, no cursor
token, no `next_page` link. The connector works around this by walking
the `fromDate` / `toDate` window in 7-day slices, one API call per
slice, advancing from `start_date` (or the last cursor value, on
incremental syncs) up to today.

CSAT response volumes are low enough that a weekly slice stays well
under any practical per-call cap. Each slice is independent, so syncs
resume cleanly from wherever the cursor last landed.

## Build + push

```bash
docker build -t ghcr.io/tnware/source-customer-thermometer:dev .
```

CI in `.github/workflows/image.yml` builds and pushes to ghcr.io on
push to main and on tags.

## Local testing

```bash
# Spec
python -m source_customer_thermometer spec

# Connection check
python -m source_customer_thermometer check --config /path/to/secrets/config.json

# Catalog discovery
python -m source_customer_thermometer discover --config /path/to/secrets/config.json

# Read records
python -m source_customer_thermometer read \
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
3. Image name: `ghcr.io/tnware/source-customer-thermometer`
4. Image tag: a published tag (see [Releases](https://github.com/tnware/source-customer-thermometer/releases))

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
