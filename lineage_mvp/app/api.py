from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from app.adapters.openlineage_adapter import ingest_event
from app.database import edge_key, get_evidence_by_edge, get_node, init_db, search_nodes
from app.models import OpenLineageEvent, PublishRequest, SearchResponse
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
