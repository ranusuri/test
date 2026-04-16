from __future__ import annotations

from collections import deque
from typing import Dict, List
import networkx as nx

from app.database import get_edges, get_node
from app.models import LineageHop, LineageResponse, Node


def build_graph() -> nx.DiGraph:
    graph = nx.DiGraph()
    for row in get_edges():
        graph.add_edge(
            row['from_canonical_id'],
            row['to_canonical_id'],
            edge_type=row['edge_type'],
            process_canonical_id=row['process_canonical_id'],
            confidence_score=row['confidence_score'],
        )
    return graph



def _row_to_node(row) -> Node:
    return Node(**dict(row))



def lineage(asset_id: str, direction: str = 'upstream', depth: int = 6) -> LineageResponse:
    graph = build_graph()
    hops: List[LineageHop] = []
    visited = {asset_id}
    queue = deque([(asset_id, 0)])

    while queue:
        current, current_depth = queue.popleft()
        if current_depth >= depth:
            continue
        neighbors = graph.predecessors(current) if direction == 'upstream' else graph.successors(current)
        for neighbor in neighbors:
            if neighbor in visited:
                continue
            visited.add(neighbor)
            edge = graph.get_edge_data(neighbor, current) if direction == 'upstream' else graph.get_edge_data(current, neighbor)
            row = get_node(neighbor)
            if row:
                hops.append(
                    LineageHop(
                        node=_row_to_node(row),
                        via_edge_type=edge['edge_type'],
                        process_canonical_id=edge.get('process_canonical_id'),
                        confidence_score=edge.get('confidence_score'),
                    )
                )
                queue.append((neighbor, current_depth + 1))

    return LineageResponse(asset_id=asset_id, direction=direction, hops=hops)



def impact_analysis(asset_id: str, depth: int = 6) -> Dict:
    downstream = lineage(asset_id, direction='downstream', depth=depth)
    return {
        'asset_id': asset_id,
        'impacted_asset_count': len(downstream.hops),
        'impacted_assets': [hop.node.canonical_id for hop in downstream.hops],
    }
