from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from app.adapters.api_spec_adapter import ingest_api_spec
from app.adapters.log_file_adapter import ingest_log_file
from app.adapters.openlineage_adapter import ingest_event
from app.adapters.sql_file_adapter import ingest_sql_file
from app.database import edge_key, get_evidence_by_edge, get_node, init_db, search_nodes
from app.models import DirectoryIngestRequest, FileIngestRequest, OpenLineageEvent, PublishRequest, SearchResponse
from app.services.bulk_ingestion import bulk_ingest_api_specs, bulk_ingest_logs, bulk_ingest_sql
from app.services.graph_service import impact_analysis, lineage
from app.services.publish_service import publish_asset_to_purview_export

app = FastAPI(title='Enterprise Lineage MVP', version='0.1.0')
init_db()


@app.get('/health')
def health():
    return {'status': 'ok'}


@app.get('/api/v1/assets', response_model=SearchResponse)
def search_assets(query: str = Query(..., min_length=1)):
    rows = search_nodes(query)
    return SearchResponse(results=[dict(row) for row in rows])


@app.get('/api/v1/lineage/upstream')
def get_upstream(assetId: str, depth: int = 6):
    if not get_node(assetId):
        raise HTTPException(status_code=404, detail='Asset not found')
    return lineage(assetId, direction='upstream', depth=depth)


@app.get('/api/v1/lineage/downstream')
def get_downstream(assetId: str, depth: int = 6):
    if not get_node(assetId):
        raise HTTPException(status_code=404, detail='Asset not found')
    return lineage(assetId, direction='downstream', depth=depth)


@app.get('/api/v1/impact')
def get_impact(assetId: str, depth: int = 6):
    if not get_node(assetId):
        raise HTTPException(status_code=404, detail='Asset not found')
    return impact_analysis(assetId, depth=depth)


@app.post('/api/v1/lineage/events')
def register_lineage_event(event: OpenLineageEvent):
    return ingest_event(event)


@app.post('/api/v1/ingest/sql')
def ingest_sql(request: FileIngestRequest):
    return {'status': 'accepted', 'details': ingest_sql_file(request.path)}


@app.post('/api/v1/ingest/logs')
def ingest_logs(request: FileIngestRequest):
    return {'status': 'accepted', 'details': ingest_log_file(request.path)}


@app.post('/api/v1/ingest/api-spec')
def ingest_api_spec_file(request: FileIngestRequest):
    return {'status': 'accepted', 'details': ingest_api_spec(request.path)}


@app.get('/api/v1/evidence')
def get_evidence(fromAsset: str, toAsset: str, edgeType: str, processId: str | None = None):
    k = edge_key(fromAsset, toAsset, edgeType, processId)
    return {'edge_key': k, 'evidence': [dict(row) for row in get_evidence_by_edge(k)]}


@app.post('/api/v1/publish')
def publish_asset(request: PublishRequest):
    if request.target != 'purview_export':
        raise HTTPException(status_code=400, detail='Unsupported publish target')
    try:
        return publish_asset_to_purview_export(request.asset_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/v1/ingest/sql/bulk')
def ingest_sql_bulk(request: DirectoryIngestRequest):
    return {'status': 'accepted', 'details': bulk_ingest_sql(request.directory)}


@app.post('/api/v1/ingest/logs/bulk')
def ingest_logs_bulk(request: DirectoryIngestRequest):
    return {'status': 'accepted', 'details': bulk_ingest_logs(request.directory)}


@app.post('/api/v1/ingest/api-spec/bulk')
def ingest_api_spec_bulk(request: DirectoryIngestRequest):
    return {'status': 'accepted', 'details': bulk_ingest_api_specs(request.directory)}
