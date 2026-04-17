# Enterprise Lineage MVP

A Python starter codebase for an enterprise data lineage MVP aligned to the recommended scope:

## Covered MVP sources
- BigQuery / Dataplex sample lineage
- Azure / Purview-integrated sample lineage
- On-prem warehouse sample lineage
- Custom batch/script flow via OpenLineage-style event
- SQL file ingestion to derived lineage events
- Runtime log ingestion to derived lineage events
- OpenAPI spec ingestion to derived lineage events

## Covered MVP capabilities
- Canonical dataset / process / run model
- Canonical ID generation
- Upstream / downstream lineage traversal
- Evidence store in SQLite
- Confidence scoring
- FastAPI service
- Streamlit lightweight UI
- Publish-out integration to Purview-style export JSON

## Repo structure
- `app/models.py` - canonical models and request/response schemas
- `app/database.py` - SQLite persistence and seed helpers
- `app/services/` - ID generation, scoring, graph traversal, publishing
- `app/adapters/` - sample source adapters and derived-file ingestion adapters
- `app/parsers/` - SQLGlot-backed SQL, log, and OpenAPI parsers
- `app/api.py` - FastAPI endpoints
- `app/ui/streamlit_app.py` - lightweight UI
- `data/samples/` - sample source payloads
- `tests/` - lightweight tests

## Quick start
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m app.seed_data
uvicorn app.api:app --reload
```

Open another terminal for the UI:
```bash
streamlit run app/ui/streamlit_app.py
```

API docs:
- http://127.0.0.1:8000/docs

## Example success scenarios
1. Search for `customer_master` across tools.
2. View cross-platform path from on-prem warehouse to BigQuery through a custom batch flow.
3. Ingest an OpenLineage-style event and see the resulting edge plus evidence.
4. Run downstream impact analysis for an on-prem dataset feeding cloud data products.
5. Automatically ingest SQL files, logs, and OpenAPI specs and convert them into lineage events.
6. Bulk ingest a whole directory of SQL files, logs, or API specs in one call.

## Notes
This is an MVP starter, so it uses:
- SQLite instead of a production graph database
- NetworkX for in-memory traversal from persisted edges
- Sample adapters and payloads instead of live cloud authentication

Production upgrades would typically replace:
- SQLite -> graph DB + operational RDBMS
- sample adapters -> actual Purview / Dataplex / warehouse connectors
- JSON export -> real publish-back API integration

## Additional file-ingestion APIs
```bash
curl -X POST http://127.0.0.1:8000/api/v1/ingest/sql \
  -H 'Content-Type: application/json' \
  -d '{"path": "data/ingest_samples/sql/customer_publish.sql"}'

curl -X POST http://127.0.0.1:8000/api/v1/ingest/logs \
  -H 'Content-Type: application/json' \
  -d '{"path": "data/ingest_samples/logs/customer_api_bridge.log"}'

curl -X POST http://127.0.0.1:8000/api/v1/ingest/api-spec \
  -H 'Content-Type: application/json' \
  -d '{"path": "data/ingest_samples/apis/customer_publish_api.yaml"}'
```

These endpoints parse files, convert them into normalized lineage relationships, and persist the resulting nodes, edges, and evidence.

## Bulk-ingestion APIs
```bash
curl -X POST http://127.0.0.1:8000/api/v1/ingest/sql/bulk \
  -H "Content-Type: application/json" \
  -d "{\"directory\": "data/ingest_samples/sql"}"

curl -X POST http://127.0.0.1:8000/api/v1/ingest/logs/bulk \
  -H "Content-Type: application/json" \
  -d "{\"directory\": "data/ingest_samples/logs"}"

curl -X POST http://127.0.0.1:8000/api/v1/ingest/api-spec/bulk \
  -H "Content-Type: application/json" \
  -d "{\"directory\": "data/ingest_samples/apis"}"
```

The SQL ingestion path now uses SQLGlot so lineage extraction is more reliable for INSERT/CREATE-AS patterns and field mapping inference than the earlier regex-only implementation.
