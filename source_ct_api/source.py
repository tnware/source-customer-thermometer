"""AbstractSource implementation for the CustomerThermometer connector."""

import logging
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from typing import Any, List, Mapping, Tuple

import requests

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .streams import ThermometerResponses, _CT_API_URL

log = logging.getLogger(__name__)


class SourceCtApi(AbstractSource):
    """Airbyte source connector for the CustomerThermometer API."""

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        params = {
            "apiKey": config["api_key"],
            "getMethod": "getBlastResults",
            "fromDate": yesterday,
            "toDate": yesterday,
            "limit": "1",
        }
        try:
            resp = requests.get(_CT_API_URL, params=params, timeout=15)
            resp.raise_for_status()
            if resp.text.strip():
                ET.fromstring(resp.text)
            return True, None
        except requests.exceptions.HTTPError as exc:
            return False, f"HTTP {exc.response.status_code}: {exc.response.text[:200]}"
        except ET.ParseError as exc:
            return False, f"XML parse error: {exc}"
        except Exception as exc:
            return False, str(exc)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [ThermometerResponses(config)]
