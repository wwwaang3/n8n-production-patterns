import unittest

from service.idempotency_engine import (
    EventState,
    IdempotentReconciliationEngine,
    compute_idempotency_key,
)


class TestIdempotencyEngine(unittest.TestCase):
    def test_deterministic_key_normalization(self):
        k1 = compute_idempotency_key("Gumroad", "SALE_9981", "Sale.Completed")
        k2 = compute_idempotency_key("  gumroad ", "SALE_9981", "sale.completed ")
        self.assertEqual(k1, k2)

    def test_duplicate_webhook_is_strict_noop(self):
        engine = IdempotentReconciliationEngine(max_retries=3)
        calls = []

        def write_airtable(payload):
            calls.append(payload["sale_id"])
            return {"airtable_record_id": "rec_001"}

        payload = {"sale_id": "G-1001", "email": "buyer@example.com", "price": 4900}
        r1 = engine.process_event("gumroad", "sale", "G-1001", payload, ["sale_id", "email"], write_airtable)
        r2 = engine.process_event("gumroad", "sale", "G-1001", payload, ["sale_id", "email"], write_airtable)

        self.assertEqual(r1.state, EventState.COMPLETED)
        self.assertEqual(r2.state, EventState.COMPLETED)
        self.assertEqual(len(calls), 1)
        self.assertTrue(any(entry.to_state == EventState.DUPLICATE_SKIPPED for entry in r2.audit_log))

    def test_transient_429_retries_then_succeeds(self):
        engine = IdempotentReconciliationEngine(max_retries=3)
        state = {"count": 0}

        def flaky_handler(payload):
            state["count"] += 1
            if state["count"] < 3:
                raise RuntimeError("HTTP 429 Too Many Requests")
            return {"status": "synced", "attempt": state["count"]}

        rec = engine.process_event(
            "close_crm", "lead.created", "LEAD-42",
            {"lead_id": "LEAD-42", "email": "ops@client.com"},
            ["lead_id", "email"], flaky_handler,
        )
        self.assertEqual(rec.state, EventState.COMPLETED)
        self.assertEqual(rec.attempts, 3)
        self.assertEqual(len(engine.dlq), 0)

    def test_exhausted_retries_route_to_dlq_and_replay_cleanly(self):
        engine = IdempotentReconciliationEngine(max_retries=2)

        def broken_handler(_payload):
            raise ConnectionError("Downstream timeout")

        rec = engine.process_event(
            "gumroad", "sale", "G-9999",
            {"sale_id": "G-9999", "email": "vip@client.com"},
            ["sale_id", "email"], broken_handler,
        )
        self.assertEqual(rec.state, EventState.DEAD_LETTERED)
        self.assertEqual(len(engine.dlq), 1)

        replayed = engine.replay_dlq(rec.idempotency_key, lambda p: {"replayed": p["sale_id"]})
        self.assertEqual(replayed.state, EventState.COMPLETED)
        self.assertEqual(len(engine.dlq), 0)

    def test_missing_required_fields_fail_fast_to_dlq(self):
        engine = IdempotentReconciliationEngine(max_retries=3)
        called = False

        def handler(_payload):
            nonlocal called
            called = True
            return {}

        rec = engine.process_event(
            "gumroad", "sale", "G-BAD",
            {"sale_id": "G-BAD", "email": "   "},
            ["sale_id", "email"], handler,
        )
        self.assertFalse(called)
        self.assertEqual(rec.state, EventState.VALIDATION_FAILED)
        self.assertEqual(len(engine.dlq), 1)


if __name__ == "__main__":
    unittest.main()
