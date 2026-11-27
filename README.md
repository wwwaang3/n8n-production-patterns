# Production n8n & Webhook Automation Patterns (Idempotency, Retries & Dead-Letter Queues)

**Maintained by Lingkefa Intelligent Technology (Ningbo) Co., Ltd. — AMD Edge AI Ecosystem Partner**

Most automation demos only build the *happy path*. In real client operations (e-commerce sales sync, AI lead routing, ERP/CRM webhooks), APIs time out, return `429 Too Many Requests`, deliver duplicate webhooks, or emit malformed LLM JSON.

This repository packages battle-tested, importable **n8n workflow blueprints** and a **Python idempotent reconciliation reference service** engineered around three non-negotiable reliability rules:

1. **Deterministic Idempotency Keys** — Every inbound event is hashed/keyed before any side effect (`UPSERT` rather than blind `INSERT`), guaranteeing `<0.1%` duplicate records across webhook replays and retries.
2. **Bounded Exponential Backoff + LLM Schema Guard** — Upstream `429`/`5xx` calls retry with backoff; AI Agent / LLM outputs pass strict JSON schema validation with deterministic fallback before touching downstream CRMs or Airtable.
3. **Structured Dead-Letter Queue (DLQ) & Alerting** — Any unrecoverable payload is persisted with full error context (`workflow_id`, `execution_id`, `failed_node`, `idempotency_key`, `raw_payload`) and alerted so operations teams can replay cleanly without data loss.

## Repository Structure

```text
n8n-production-patterns/
├── workflows/
│   ├── 01_gumroad_to_airtable_idempotent.json   # Webhook validation + idempotent Airtable upsert + DLQ
│   ├── 02_ai_lead_triage_schema_guard.json      # AI lead enrichment with strict JSON guard + fallback
│   └── 03_global_error_handler_dlq.json         # Reusable n8n Error Trigger -> DLQ log + Ops Alert
├── service/
│   └── idempotency_engine.py                    # 8-state reconciliation & retry/DLQ state machine (Python 3.9+)
├── tests/
│   └── test_idempotency_engine.py               # Zero-dependency unittest suite (5/5 passing)
└── README.md
```

## Node Naming Convention

Every workflow uses explicit stage prefixes so another engineer can audit a 25-node workflow in under 60 seconds:

| Prefix | Purpose |
| :--- | :--- |
| `[TRIGGER]` | Webhook / Schedule / Error entry point |
| `[VALIDATE]` | Signature verification, required-field & schema guard |
| `[IDEMPOTENCY]` | Deterministic key lookup / lock check |
| `[TRANSFORM]` | Pure data mapping & normalization |
| `[AI]` | LLM classification / extraction with JSON guard |
| `[WRITE]` | Idempotent API / CRM / Airtable / PostgreSQL upsert |
| `[DLQ]` | Dead-letter persistence + human-readable alert |

## Quick Verification

```bash
python3 -m unittest discover -s tests -v
```

## Contact & Custom Engagements

Need reliable self-hosted n8n workflows, on-premise document OCR/RPA, or custom Python/TypeScript automation?

- **Lead Contact**: Rufa Wang — Co-Founder, Lingkefa Intelligent Technology (Ningbo) Co., Ltd.
- **Email**: `wwwaang3@gmail.com`
- **WhatsApp / Tel**: `+86 187 5831 1950`
