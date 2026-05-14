"""Stream implementation for the CustomerThermometer Airbyte connector.

Single stream:

- ``ThermometerResponses`` — incremental by ``response_date`` cursor.
  The CT API has no pagination (no offset, no cursor token, no next-page
  link). The connector works around this by walking the ``fromDate`` /
  ``toDate`` window in 7-day slices, one API call per slice. CSAT volumes
  per week are low enough that no per-call cap is a realistic concern.

Output field names match the destination column names expected by the
downstream warehouse so the staging models need no field-name translation.
"""

import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from typing import Any, Iterable, Mapping, Optional

import requests

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

log = logging.getLogger(__name__)

_CT_API_URL = "https://app.customerthermometer.com/api.php"
_WINDOW_DAYS = 7
_PER_CHUNK_LIMIT = 10_000
_HTTP_TIMEOUT_SECONDS = 60


def _today() -> date:
    """Wrapper around ``date.today()`` so tests can pin the wall clock."""
    return date.today()


def _validate_start_date(value: Any) -> str:
    """Coerce + validate a start_date config value.

    Accepts ``YYYY-MM-DD``. Rejects future dates and unparseable strings
    with a clear error so the sync fails at config time instead of mid-fetch.
    Returns the date as ``YYYY-MM-DD``.
    """
    if value is None:
        raise ValueError("start_date is required")
    try:
        parsed = date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"start_date must be YYYY-MM-DD, got {value!r}: {exc}"
        ) from exc
    if parsed > _today():
        raise ValueError(
            f"start_date {parsed.isoformat()} is in the future"
        )
    return parsed.isoformat()


class ThermometerResponses(Stream):
    """CSAT survey responses, ingested incrementally by response_date."""

    primary_key = "response_id"
    cursor_field = "response_date"

    def __init__(self, config: Mapping[str, Any]):
        super().__init__()
        self._api_key = config["api_key"]
        self._start_date = _validate_start_date(config["start_date"])

    @property
    def source_defined_cursor(self) -> bool:
        return True

    @property
    def supported_sync_modes(self):
        return [SyncMode.incremental, SyncMode.full_refresh]

    def get_updated_state(
        self,
        current_stream_state: Mapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        current = current_stream_state.get(self.cursor_field, "")
        latest = str(latest_record.get(self.cursor_field, "") or "")
        # ISO-8601 strings sort lexicographically, so string max is correct.
        return {self.cursor_field: max(current, latest)}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        state = stream_state or {}
        cursor_value = state.get(self.cursor_field, self._start_date)

        window_start = date.fromisoformat(str(cursor_value)[:10])
        today = _today()

        while window_start <= today:
            window_end = min(
                window_start + timedelta(days=_WINDOW_DAYS - 1), today
            )

            log.info(
                "ThermometerResponses: fetching %s to %s",
                window_start.isoformat(),
                window_end.isoformat(),
            )

            params = {
                "apiKey": self._api_key,
                "getMethod": "getBlastResults",
                "fromDate": window_start.isoformat(),
                "toDate": window_end.isoformat(),
                "limit": str(_PER_CHUNK_LIMIT),
            }

            resp = requests.get(
                _CT_API_URL, params=params, timeout=_HTTP_TIMEOUT_SECONDS
            )
            resp.raise_for_status()

            if resp.text.strip():
                try:
                    root = ET.fromstring(resp.text)
                except ET.ParseError as exc:
                    raise RuntimeError(
                        f"CustomerThermometer returned non-XML response: {exc}"
                    ) from exc

                for item in root.findall("thermometer_blast_response"):
                    raw = {child.tag: child.text for child in item}
                    record = _map_fields(raw)
                    if record:
                        yield record

            window_start = window_end + timedelta(days=1)

    def get_json_schema(self) -> Mapping[str, Any]:
        schema_path = os.path.join(
            os.path.dirname(__file__), "schemas", "thermometer_responses.json"
        )
        with open(schema_path) as f:
            return json.load(f)


def _map_fields(raw: dict) -> Optional[dict]:
    """Map raw XML field names to destination column names.

    Returns None if response_id is missing or zero (defensive: a row
    without a stable primary key can't be deduplicated downstream).
    """
    try:
        response_id = int(raw.get("response_id") or 0)
    except (ValueError, TypeError):
        return None
    if not response_id:
        return None

    def _int(val):
        try:
            return int(val) if val else None
        except (ValueError, TypeError):
            return None

    return {
        "response_id":      response_id,
        "response_date":    raw.get("response_date"),
        "response":         raw.get("response"),
        "temperature_id":   _int(raw.get("temperature_id")),
        "recipient_email":  raw.get("recipient"),       # API tag is 'recipient'
        "first_name":       raw.get("first_name"),
        "last_name":        raw.get("last_name"),
        "company":          raw.get("company"),
        # CustomerThermometer exposes twelve free-form custom fields per
        # blast (custom1..custom12 on sendEmail; emitted as
        # custom_1..custom_12 in the getBlastResults XML). Their
        # semantics are configured per-account in the CT admin UI, so
        # the connector passes them through raw. Downstream consumers
        # are responsible for naming them.
        "custom_1":         raw.get("custom_1"),
        "custom_2":         raw.get("custom_2"),
        "custom_3":         raw.get("custom_3"),
        "custom_4":         raw.get("custom_4"),
        "custom_5":         raw.get("custom_5"),
        "custom_6":         raw.get("custom_6"),
        "custom_7":         raw.get("custom_7"),
        "custom_8":         raw.get("custom_8"),
        "custom_9":         raw.get("custom_9"),
        "custom_10":        raw.get("custom_10"),
        "custom_11":        raw.get("custom_11"),
        "custom_12":        raw.get("custom_12"),
        "comment":          raw.get("comment"),
        "blast_id":         _int(raw.get("blast_id")),
        "thermometer_id":   _int(raw.get("thermometer_id")),
        "response_bounced": raw.get("response_bounced") == "1",
        "comment_hidden":   raw.get("comment_hidden") == "1",
    }
