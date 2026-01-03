from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


class SchemaError(ValueError):
    pass


def _parse_rfc3339(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise SchemaError(f"Invalid RFC3339 timestamp: {value}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _normalize_action_result(value: Any) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return None
    return str(value)


@dataclass(frozen=True)
class AuditLogRecord:
    event_id: str
    timestamp: datetime
    account_id: str
    actor_email: str | None
    actor_type: str | None
    action_type: str | None
    action_result: str | None
    resource_type: str | None
    resource_id: str | None
    ip_address: str | None
    metadata: Mapping[str, Any]
    ingestion_time: datetime
    raw_event: Mapping[str, Any]

    @classmethod
    def from_cloudflare_event(
        cls,
        event: Mapping[str, Any],
        *,
        account_id: str,
        ingestion_time: datetime,
    ) -> "AuditLogRecord":
        if not isinstance(event, Mapping):
            raise SchemaError("Audit log event must be a mapping")

        event_id = _coerce_str(event.get("id"))
        if not event_id:
            raise SchemaError("Missing required field: id")

        raw_timestamp = event.get("when")
        if not isinstance(raw_timestamp, str) or not raw_timestamp:
            raise SchemaError("Missing required field: when")
        timestamp = _parse_rfc3339(raw_timestamp)

        actor = event.get("actor") or {}
        action = event.get("action") or {}
        resource = event.get("resource") or {}
        metadata = event.get("metadata") or {}
        if not isinstance(metadata, Mapping):
            metadata = {}

        return cls(
            event_id=event_id,
            timestamp=timestamp,
            account_id=account_id,
            actor_email=_coerce_str(actor.get("email")),
            actor_type=_coerce_str(actor.get("type")),
            action_type=_coerce_str(action.get("type")),
            action_result=_normalize_action_result(action.get("result")),
            resource_type=_coerce_str(resource.get("type")),
            resource_id=_coerce_str(resource.get("id")),
            ip_address=_coerce_str(actor.get("ip")),
            metadata=metadata,
            ingestion_time=ingestion_time,
            raw_event=event,
        )
