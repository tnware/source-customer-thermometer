"""Tests for the CustomerThermometer connector — pure logic + mocked HTTP."""

from datetime import date, timedelta
from unittest.mock import patch

import pytest
import responses

from airbyte_cdk.models import SyncMode

from source_customer_thermometer.streams import (
    ThermometerResponses,
    _map_fields,
    _validate_start_date,
    _CT_API_URL,
    _WINDOW_DAYS,
)


CONFIG = {"api_key": "ct-key", "start_date": "2025-01-01"}


# ----------------------------------------------------------------------
# _validate_start_date
# ----------------------------------------------------------------------

class TestValidateStartDate:
    def test_accepts_valid_iso_date(self):
        assert _validate_start_date("2025-01-01") == "2025-01-01"

    def test_strips_time_component(self):
        # Be lenient: if a caller passes "2025-01-01T00:00:00", accept it.
        assert _validate_start_date("2025-01-01T12:00:00") == "2025-01-01"

    def test_rejects_future_date(self):
        future = (date.today() + timedelta(days=30)).isoformat()
        with pytest.raises(ValueError, match="in the future"):
            _validate_start_date(future)

    @pytest.mark.parametrize("value", ["not-a-date", "2025/01/01", "abcdefghij"])
    def test_rejects_garbage(self, value):
        with pytest.raises(ValueError):
            _validate_start_date(value)

    def test_rejects_none(self):
        with pytest.raises(ValueError):
            _validate_start_date(None)


# ----------------------------------------------------------------------
# _map_fields — field mapping correctness
# ----------------------------------------------------------------------

class TestMapFields:
    def test_full_record_maps_correctly(self):
        raw = {
            "response_id": "3001",
            "response_date": "2026-01-15 11:00:00",
            "response": "Gold",
            "temperature_id": "1",
            "recipient": "cust@x.com",
            "first_name": "Cust",
            "last_name": "Omer",
            "company": "Acme",
            "custom_1": "5001",
            "custom_2": "Jane Doe",
            "custom_3": "Ticket subject",
            "comment": "Great service!",
            "blast_id": "42",
            "thermometer_id": "7",
            "response_bounced": "0",
            "comment_hidden": "0",
        }
        out = _map_fields(raw)
        assert out["response_id"] == 3001
        assert out["response"] == "Gold"
        assert out["temperature_id"] == 1
        assert out["recipient_email"] == "cust@x.com"
        assert out["custom_1"] == "5001"
        assert out["custom_2"] == "Jane Doe"
        assert out["custom_3"] == "Ticket subject"
        assert out["blast_id"] == 42
        assert out["thermometer_id"] == 7
        assert out["response_bounced"] is False
        assert out["comment_hidden"] is False

    def test_response_bounced_true_when_one(self):
        out = _map_fields({"response_id": "1", "response_bounced": "1"})
        assert out["response_bounced"] is True

    def test_comment_hidden_true_when_one(self):
        out = _map_fields({"response_id": "1", "comment_hidden": "1"})
        assert out["comment_hidden"] is True

    def test_returns_none_when_response_id_zero(self):
        assert _map_fields({"response_id": "0"}) is None

    def test_returns_none_when_response_id_missing(self):
        assert _map_fields({}) is None

    def test_returns_none_when_response_id_unparseable(self):
        assert _map_fields({"response_id": "abc"}) is None

    def test_int_helper_returns_none_for_empty_string(self):
        out = _map_fields({"response_id": "1", "temperature_id": ""})
        assert out["temperature_id"] is None

    def test_int_helper_returns_none_for_garbage(self):
        out = _map_fields({"response_id": "1", "blast_id": "not-a-number"})
        assert out["blast_id"] is None


# ----------------------------------------------------------------------
# get_updated_state
# ----------------------------------------------------------------------

class TestGetUpdatedState:
    @pytest.fixture
    def stream(self):
        return ThermometerResponses(CONFIG)

    def test_picks_later_iso_date(self, stream):
        out = stream.get_updated_state(
            current_stream_state={"response_date": "2026-01-15"},
            latest_record={"response_date": "2026-01-16"},
        )
        assert out == {"response_date": "2026-01-16"}

    def test_keeps_existing_when_latest_is_empty(self, stream):
        out = stream.get_updated_state(
            current_stream_state={"response_date": "2026-01-15"},
            latest_record={},
        )
        assert out == {"response_date": "2026-01-15"}


# ----------------------------------------------------------------------
# read_records — chunked date-window walk
# ----------------------------------------------------------------------

def _xml_response(items: list[dict]) -> str:
    parts = ["<response>"]
    for item in items:
        parts.append("<thermometer_blast_response>")
        for k, v in item.items():
            parts.append(f"<{k}>{v}</{k}>")
        parts.append("</thermometer_blast_response>")
    parts.append("</response>")
    return "".join(parts)


class TestReadRecords:
    @responses.activate
    @patch("source_customer_thermometer.streams._today")
    def test_yields_mapped_records(self, mock_today):
        # One 7-day chunk: 2025-01-01 → 2025-01-07
        mock_today.return_value = date(2025, 1, _WINDOW_DAYS)
        responses.add(
            responses.GET,
            _CT_API_URL,
            body=_xml_response([
                {"response_id": 1, "response": "Gold", "temperature_id": 1},
                {"response_id": 2, "response": "Green", "temperature_id": 2},
            ]),
            status=200,
            content_type="application/xml",
        )

        stream = ThermometerResponses(CONFIG)
        records = list(stream.read_records(SyncMode.full_refresh))

        assert len(records) == 2
        assert records[0]["response_id"] == 1
        assert records[1]["response"] == "Green"
        assert len(responses.calls) == 1

    @responses.activate
    @patch("source_customer_thermometer.streams._today")
    def test_walks_multiple_chunks(self, mock_today):
        # 3 weekly chunks: 2025-01-01..07, 08..14, 15..21
        mock_today.return_value = date(2025, 1, 21)

        call_count = {"n": 0}

        def cb(request):
            call_count["n"] += 1
            body = _xml_response([{"response_id": call_count["n"]}])
            return (200, {"Content-Type": "application/xml"}, body)

        responses.add_callback(responses.GET, _CT_API_URL, callback=cb)

        stream = ThermometerResponses(CONFIG)
        records = list(stream.read_records(SyncMode.full_refresh))

        assert call_count["n"] == 3
        assert [r["response_id"] for r in records] == [1, 2, 3]

    @responses.activate
    @patch("source_customer_thermometer.streams._today")
    def test_resumes_from_cursor_state(self, mock_today):
        # Cursor sits mid-history; we should only fetch from there forward.
        mock_today.return_value = date(2025, 1, 14)

        captured = []

        def cb(request):
            captured.append(request.params.get("fromDate"))
            return (200, {"Content-Type": "application/xml"}, "")

        responses.add_callback(responses.GET, _CT_API_URL, callback=cb)

        stream = ThermometerResponses(CONFIG)
        list(stream.read_records(
            SyncMode.incremental,
            stream_state={"response_date": "2025-01-10 09:00:00"},
        ))

        # First window starts at the cursor date, not start_date.
        assert captured[0] == "2025-01-10"

    @responses.activate
    @patch("source_customer_thermometer.streams._today")
    def test_empty_response_yields_nothing(self, mock_today):
        mock_today.return_value = date(2025, 1, _WINDOW_DAYS)
        responses.add(
            responses.GET,
            _CT_API_URL,
            body="",
            status=200,
            content_type="application/xml",
        )
        stream = ThermometerResponses(CONFIG)
        assert list(stream.read_records(SyncMode.full_refresh)) == []

    @responses.activate
    @patch("source_customer_thermometer.streams._today")
    def test_raises_on_non_xml_response(self, mock_today):
        mock_today.return_value = date(2025, 1, _WINDOW_DAYS)
        responses.add(
            responses.GET,
            _CT_API_URL,
            body="<<<not xml>>>",
            status=200,
            content_type="application/xml",
        )
        stream = ThermometerResponses(CONFIG)
        with pytest.raises(RuntimeError, match="non-XML"):
            list(stream.read_records(SyncMode.full_refresh))
