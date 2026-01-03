import json
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import pytest

from glitch.cloudflare import CloudflareAPIError, CloudflareClient


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_iter_audit_logs_paginates(monkeypatch):
    payloads = [
        {"success": True, "result": [{"id": "1"}, {"id": "2"}]},
        {"success": True, "result": [{"id": "3"}]},
    ]
    requests = []

    def urlopen_stub(request, timeout):
        requests.append(request)
        return _FakeResponse(payloads.pop(0))

    monkeypatch.setattr("glitch.cloudflare.urlopen", urlopen_stub)

    client = CloudflareClient(
        account_id="acct",
        api_token="token",
        per_page=2,
        direction="asc",
    )
    results = list(
        client.iter_audit_logs(
            since=datetime(2026, 1, 2, tzinfo=timezone.utc),
            before=datetime(2026, 1, 3, tzinfo=timezone.utc),
        )
    )
    assert [item["id"] for item in results] == ["1", "2", "3"]
    assert requests[0].get_header("Authorization") == "Bearer token"

    first_url = urlparse(requests[0].full_url)
    first_query = parse_qs(first_url.query)
    assert first_query["page"] == ["1"]
    assert first_query["per_page"] == ["2"]


def test_error_response_raises(monkeypatch):
    payload = {"success": False, "errors": [{"message": "nope"}]}

    def urlopen_stub(request, timeout):
        return _FakeResponse(payload)

    monkeypatch.setattr("glitch.cloudflare.urlopen", urlopen_stub)

    client = CloudflareClient(account_id="acct", api_token="token")
    with pytest.raises(CloudflareAPIError):
        list(
            client.iter_audit_logs(
                since=datetime(2026, 1, 2, tzinfo=timezone.utc),
                before=datetime(2026, 1, 3, tzinfo=timezone.utc),
            )
        )
