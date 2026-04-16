from __future__ import annotations

import json
from pathlib import Path

from app.config import EXPORT_DIR
from app.database import get_all_related_edges, get_node
from app.models import PublishResponse


def publish_asset_to_purview_export(asset_id: str) -> PublishResponse:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    node = get_node(asset_id)
    if not node:
        raise ValueError(f'Asset not found: {asset_id}')
    edges = get_all_related_edges(asset_id)
    payload = {
        'asset': dict(node),
        'relationships': [dict(edge) for edge in edges],
        'target': 'purview_export',
    }
    export_file = EXPORT_DIR / f"{asset_id.split('/')[-1]}_purview_export.json"
    with open(export_file, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2)
    return PublishResponse(
        target='purview_export',
        export_file=str(export_file),
        published_asset_count=1,
        published_edge_count=len(edges),
    )
