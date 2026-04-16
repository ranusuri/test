# Enterprise Lineage MVP

A Python starter codebase for an enterprise data lineage MVP aligned to the recommended scope:

## Covered MVP sources
- BigQuery / Dataplex sample lineage
- Azure / Purview-integrated sample lineage
- On-prem warehouse sample lineage
- Custom batch/script flow via OpenLineage-style event

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
- `app/adapters/` - sample source adapters and OpenLineage ingestion
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

## Notes
This is an MVP starter, so it uses:
- SQLite instead of a production graph database
- NetworkX for in-memory traversal from persisted edges
- Sample adapters and payloads instead of live cloud authentication

Production upgrades would typically replace:
- SQLite -> graph DB + operational RDBMS
- sample adapters -> actual Purview / Dataplex / warehouse connectors
- JSON export -> real publish-back API integration
