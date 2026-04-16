from __future__ import annotations

from app.database import edge_key, insert_evidence, upsert_edges, upsert_nodes
from app.models import Edge, Evidence, Node, OpenLineageEvent
from app.services.id_service import canonical_dataset_id, canonical_process_id, canonical_run_id
from app.services.scoring import compute_confidence


def _namespace_to_parts(namespace: str) -> tuple[str, str, str]:
    # Example: teradata.prod.staging or bigquery.prod.customer360
    system, environment, container = namespace.split('.', maxsplit=2)
    platform = 'onprem' if system in {'teradata', 'oracle'} else 'gcp' if system == 'bigquery' else 'custom'
    return platform, environment, f"{system}/{container}"


def ingest_event(event: OpenLineageEvent) -> dict:
    source_system = 'openlineage'
    platform = 'custom'
    process_id = canonical_process_id(platform, 'prod', event.job.namespace.replace('.', '_'), 'batch', event.job.name)
    run_id = canonical_run_id(process_id, event.run.runId)

    nodes = [
        Node(canonical_id=process_id, node_type='PROCESS', display_name=event.job.name, platform=platform, environment='prod', source_system=source_system, domain='customer360', owner_team='batch-team'),
        Node(canonical_id=run_id, node_type='RUN', display_name=event.run.runId, platform=platform, environment='prod', source_system=source_system, domain='customer360', owner_team='batch-team'),
    ]
    edges = []
    evidence = []

    for ds in event.inputs + event.outputs:
        ds_platform, ds_environment, system_container = _namespace_to_parts(ds.namespace)
        system, container = system_container.split('/', maxsplit=1)
        cid = canonical_dataset_id(ds_platform, ds_environment, system, container, ds.name.replace('.', '_'))
        nodes.append(Node(canonical_id=cid, node_type='DATASET', display_name=ds.name.replace('.', '_'), platform=ds_platform, environment=ds_environment, source_system=source_system, domain='customer360', owner_team='batch-team'))

    upsert_nodes(nodes)

    for ds in event.inputs:
        ds_platform, ds_environment, system_container = _namespace_to_parts(ds.namespace)
        system, container = system_container.split('/', maxsplit=1)
        input_id = canonical_dataset_id(ds_platform, ds_environment, system, container, ds.name.replace('.', '_'))
        e = Edge(from_canonical_id=input_id, to_canonical_id=process_id, edge_type='READS_FROM', process_canonical_id=process_id, run_canonical_id=run_id, confidence_score=compute_confidence(source_system), verification_state='OBSERVED', source_system=source_system)
        edges.append(e)
        evidence.append(Evidence(edge_key=edge_key(e.from_canonical_id, e.to_canonical_id, e.edge_type, e.process_canonical_id), evidence_type='OPENLINEAGE_EVENT', source_system=source_system, source_reference=event.run.runId, evidence_uri=f'openlineage://{event.run.runId}', confidence_contribution=0.90))

    for ds in event.outputs:
        ds_platform, ds_environment, system_container = _namespace_to_parts(ds.namespace)
        system, container = system_container.split('/', maxsplit=1)
        output_id = canonical_dataset_id(ds_platform, ds_environment, system, container, ds.name.replace('.', '_'))
        e = Edge(from_canonical_id=process_id, to_canonical_id=output_id, edge_type='WRITES_TO', process_canonical_id=process_id, run_canonical_id=run_id, confidence_score=compute_confidence(source_system), verification_state='OBSERVED', source_system=source_system)
        edges.append(e)
        evidence.append(Evidence(edge_key=edge_key(e.from_canonical_id, e.to_canonical_id, e.edge_type, e.process_canonical_id), evidence_type='OPENLINEAGE_EVENT', source_system=source_system, source_reference=event.run.runId, evidence_uri=f'openlineage://{event.run.runId}', confidence_contribution=0.90))

    upsert_edges(edges)
    insert_evidence(evidence)
    return {'process_id': process_id, 'run_id': run_id, 'edge_count': len(edges)}
