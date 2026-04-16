from __future__ import annotations

from app.adapters.common import load_json
from app.config import SAMPLES_DIR
from app.database import edge_key, insert_evidence, upsert_aliases, upsert_edges, upsert_nodes
from app.models import AliasRecord, Edge, Evidence, Node
from app.services.id_service import canonical_dataset_id, canonical_process_id
from app.services.scoring import compute_confidence


def ingest_sample() -> None:
    sample = load_json(SAMPLES_DIR / 'onprem_lineage.json')
    source_system = 'onprem_sql'
    process_id = canonical_process_id('onprem', 'prod', 'teradata', 'sql_job', 'customer_stage_extract')
    nodes = [Node(canonical_id=process_id, node_type='PROCESS', display_name='customer_stage_extract', platform='onprem', environment='prod', source_system=source_system, domain='customer360', owner_team='edw-team')]
    aliases = []
    edges = []
    evidence = []

    for ds in sample['datasets']:
        cid = canonical_dataset_id('onprem', ds['environment'], ds['system'], ds['container'], ds['object'])
        nodes.append(Node(canonical_id=cid, node_type='DATASET', display_name=ds['object'], platform='onprem', environment=ds['environment'], source_system=source_system, domain=ds.get('domain'), owner_team=ds.get('owner_team')))
        aliases.append(AliasRecord(source_system=source_system, native_id=ds['native_id'], fq_name=ds['fq_name'], canonical_id=cid))

    for rel in sample['relationships']:
        from_id = canonical_dataset_id('onprem', rel['from']['environment'], rel['from']['system'], rel['from']['container'], rel['from']['object'])
        to_id = canonical_dataset_id('onprem', rel['to']['environment'], rel['to']['system'], rel['to']['container'], rel['to']['object'])
        e1 = Edge(from_canonical_id=from_id, to_canonical_id=process_id, edge_type='READS_FROM', process_canonical_id=process_id, confidence_score=compute_confidence(source_system), source_system=source_system)
        e2 = Edge(from_canonical_id=process_id, to_canonical_id=to_id, edge_type='WRITES_TO', process_canonical_id=process_id, confidence_score=compute_confidence(source_system), source_system=source_system)
        edges.extend([e1, e2])
        evidence.extend([
            Evidence(edge_key=edge_key(e1.from_canonical_id, e1.to_canonical_id, e1.edge_type, e1.process_canonical_id), evidence_type='SOURCE_EXTRACT', source_system=source_system, source_reference='sample', evidence_uri='onprem://sample/customer_stage_extract', confidence_contribution=e1.confidence_score),
            Evidence(edge_key=edge_key(e2.from_canonical_id, e2.to_canonical_id, e2.edge_type, e2.process_canonical_id), evidence_type='SOURCE_EXTRACT', source_system=source_system, source_reference='sample', evidence_uri='onprem://sample/customer_stage_extract', confidence_contribution=e2.confidence_score),
        ])

    upsert_nodes(nodes)
    upsert_aliases(aliases)
    upsert_edges(edges)
    insert_evidence(evidence)
