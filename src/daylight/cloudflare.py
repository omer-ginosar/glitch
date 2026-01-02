from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Dict, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


class CloudflareAPIError(RuntimeError):
    pass


def _format_rfc3339(value: datetime) -> str:
    formatted = value.isoformat()
    if formatted.endswith("+00:00"):
        return formatted.replace("+00:00", "Z")
    return formatted


@dataclass(frozen=True)
class CloudflareClient:
    account_id: str
    api_token: str
    per_page: int = 1000
    direction: str = "asc"
    hide_user_logs: bool = False
    timeout_seconds: int = 30
    base_url: str = "https://api.cloudflare.com/client/v4"

    def iter_audit_logs(self, *, since: datetime, before: datetime) -> Iterable[Dict[str, Any]]:
        page = 1
        while True:
            payload = self._get_audit_logs_page(since=since, before=before, page=page)
            result = payload.get("result", [])
            if not isinstance(result, list):
                raise CloudflareAPIError("Unexpected audit log response shape: result is not a list")
            for item in result:
                if isinstance(item, dict):
                    yield item
                else:
                    raise CloudflareAPIError("Unexpected audit log item type")
            if len(result) < self.per_page:
                break
            page += 1

    def _get_audit_logs_page(self, *, since: datetime, before: datetime,
                             page: int) -> Dict[str, Any]:
        params = {
            "page": page,
            "per_page": self.per_page,
            "direction": self.direction,
            "since": _format_rfc3339(since),
            "before": _format_rfc3339(before),
        }
        if self.hide_user_logs:
            params["hide_user_logs"] = "true"
        path = f"/accounts/{self.account_id}/audit_logs"
        return self._get_json(path, params)

    def _get_json(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        query = urlencode(params)
        url = f"{self.base_url}{path}?{query}"
        request = Request(url)
        request.add_header("Authorization", f"Bearer {self.api_token}")
        request.add_header("Accept", "application/json")
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            message = exc.read().decode("utf-8", errors="ignore")
            raise CloudflareAPIError(f"HTTP {exc.code}: {message}") from exc
        except URLError as exc:
            raise CloudflareAPIError(f"Request failed: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise CloudflareAPIError("Failed to decode JSON response") from exc

        if not isinstance(payload, dict):
            raise CloudflareAPIError("Unexpected response payload type")
        if not payload.get("success", False):
            errors = payload.get("errors", [])
            raise CloudflareAPIError(f"Cloudflare API error: {errors}")
        return payload
