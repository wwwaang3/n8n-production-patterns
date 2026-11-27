"""Deterministic idempotent webhook reconciliation & DLQ state machine.

Lightweight Python reference companion for n8n/webhook pipelines requiring
strict deduplication, bounded retries, and replayable dead-letter queues.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class EventState(str, Enum):
    RECEIVED = "RECEIVED"
    VALIDATED = "VALIDATED"
    IN_PROGRESS = "IN_PROGRESS"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    COMPLETED = "COMPLETED"
    DUPLICATE_SKIPPED = "DUPLICATE_SKIPPED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    DEAD_LETTERED = "DEAD_LETTERED"


@dataclass
class AuditEntry:
    timestamp: str
    from_state: Optional[EventState]
    to_state: EventState
    detail: str = ""


@dataclass
class EventRecord:
    idempotency_key: str
    source: str
    payload: Dict[str, Any]
    state: EventState = EventState.RECEIVED
    attempts: int = 0
    last_error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    audit_log: List[AuditEntry] = field(default_factory=list)

    def transition(self, next_state: EventState, detail: str = "") -> None:
        self.audit_log.append(
            AuditEntry(
                timestamp=datetime.now(timezone.utc).isoformat(),
                from_state=self.state,
                to_state=next_state,
                detail=detail,
            )
        )
        self.state = next_state


def compute_idempotency_key(source: str, external_id: str, event_type: str) -> str:
    """Compute a deterministic SHA-256 idempotency key from canonical event identity."""
    canonical = f"{source.strip().lower()}::{event_type.strip().lower()}::{external_id.strip()}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_required_fields(payload: Dict[str, Any], required: List[str]) -> List[str]:
    """Return missing or empty required field names."""
    missing = []
    for key in required:
        val = payload.get(key)
        if val is None or (isinstance(val, str) and not val.strip()):
            missing.append(key)
    return missing


class IdempotentReconciliationEngine:
    """Pluggable reconciliation engine with deterministic dedupe and DLQ replay."""

    def __init__(self, max_retries: int = 3) -> None:
        self.max_retries = max_retries
        self.records: Dict[str, EventRecord] = {}
        self.dlq: List[Dict[str, Any]] = []

    def process_event(
        self,
        source: str,
        event_type: str,
        external_id: str,
        payload: Dict[str, Any],
        required_fields: List[str],
        handler: Callable[[Dict[str, Any]], Dict[str, Any]],
    ) -> EventRecord:
        key = compute_idempotency_key(source, external_id, event_type)

        existing = self.records.get(key)
        if existing and existing.state == EventState.COMPLETED:
            existing.transition(EventState.DUPLICATE_SKIPPED, "Replay detected; side-effect skipped")
            existing.state = EventState.COMPLETED
            return existing

        record = existing or EventRecord(idempotency_key=key, source=source, payload=payload)
        self.records[key] = record

        missing = validate_required_fields(payload, required_fields)
        if missing:
            record.last_error = f"Missing required fields: {', '.join(missing)}"
            record.transition(EventState.VALIDATION_FAILED, record.last_error)
            self._push_dlq(record, reason="validation_error")
            return record

        record.transition(EventState.VALIDATED, "Required fields verified")

        while record.attempts < self.max_retries:
            record.attempts += 1
            record.transition(EventState.IN_PROGRESS, f"Attempt {record.attempts}/{self.max_retries}")
            try:
                record.result = handler(payload)
                record.last_error = None
                record.transition(EventState.COMPLETED, "Downstream write succeeded")
                return record
            except Exception as exc:
                record.last_error = f"{type(exc).__name__}: {exc}"
                if record.attempts < self.max_retries:
                    record.transition(EventState.RETRY_SCHEDULED, record.last_error)
                else:
                    record.transition(EventState.DEAD_LETTERED, f"Exhausted retries: {record.last_error}")
                    self._push_dlq(record, reason="max_retries_exceeded")
                    return record

        return record

    def replay_dlq(
        self,
        idempotency_key: str,
        handler: Callable[[Dict[str, Any]], Dict[str, Any]],
        patched_payload: Optional[Dict[str, Any]] = None,
    ) -> EventRecord:
        """Deterministically replay a dead-lettered event after upstream recovery."""
        record = self.records[idempotency_key]
        if patched_payload is not None:
            record.payload = patched_payload
        record.attempts = 0
        record.transition(EventState.IN_PROGRESS, "Manual DLQ replay initiated")
        record.result = handler(record.payload)
        record.last_error = None
        record.transition(EventState.COMPLETED, "DLQ replay succeeded")
        self.dlq = [item for item in self.dlq if item["idempotency_key"] != idempotency_key]
        return record

    def _push_dlq(self, record: EventRecord, reason: str) -> None:
        self.dlq.append(
            {
                "idempotency_key": record.idempotency_key,
                "source": record.source,
                "reason": reason,
                "error": record.last_error,
                "attempts": record.attempts,
                "payload_json": json.dumps(record.payload, sort_keys=True),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
