from __future__ import annotations

from pathlib import Path
from typing import Iterable

from app.database import edge_key, insert_evidence, upsert_edges, upsert_nodes
from app.models import Edge, Evidence, Node, OpenLineageDataset, OpenLineageEvent, OpenLineageJob, OpenLineageRun
from app.services.id_service import canonical_dataset_id, canonical_process_id, canonical_run_id
from app.services.scoring import compute_confidence


def _dataset(namespace: str, name: str) -> OpenLineageDataset:
    return OpenLineageDataset(namespace=namespace, name=name)


def build_openlineage_event(
    *,
    producer: str,
    event_time: str,
    job_namespace: str,
    job_name: str,
    run_id: str,
    input_names: Iterable[str],
    output_names: Iterable[str],
    input_namespace: str,
    output_namespace: str,
) -> OpenLineageEvent:
    return OpenLineageEvent(
        eventType="COMPLETE",
        eventTime=event_time,
        job=OpenLineageJob(namespace=job_namespace, name=job_name),
        run=OpenLineageRun(runId=run_id),
        inputs=[_dataset(input_namespace, name) for name in input_names],
        outputs=[_dataset(output_namespace, name) for name in output_names],
        producer=producer,
    )


def ingest_standalone_process_with_edges(
    *,
    process_name: str,
    source_system: str,
    evidence_type: str,
    evidence_uri: str,
    source_reference: str,
    inputs: list[str],
    outputs: list[str],
    platform: str,
    environment: str,
    system: str,
    container: str,
    process_type: str,
    domain: str,
    owner_team: str,
    run_id: str | None = None,
    verification_state: str = "INFERRED",
) -> dict:
    process_id = canonical_process_id(platform, environment, system, process_type, process_name)
    resolved_run_id = canonical_run_id(process_id, run_id or f"{process_name}-run")
    confidence = compute_confidence(source_system)

    nodes = [
        Node(
            canonical_id=process_id,
            node_type="PROCESS",
            display_name=process_name,
            platform=platform,
            environment=environment,
            source_system=source_system,
            domain=domain,
            owner_team=owner_team,
        ),
        Node(
            canonical_id=resolved_run_id,
            node_type="RUN",
            display_name=run_id or f"{process_name}-run",
            platform=platform,
            environment=environment,
            source_system=source_system,
            domain=domain,
            owner_team=owner_team,
        ),
    ]

    edges: list[Edge] = []
    evidence: list[Evidence] = []

    def make_dataset_id(name: str) -> str:
        return canonical_dataset_id(platform, environment, system, container, name)

    for name in sorted(set(inputs + outputs)):
        nodes.append(
            Node(
                canonical_id=make_dataset_id(name),
                node_type="DATASET",
                display_name=name,
                platform=platform,
                environment=environment,
                source_system=source_system,
                domain=domain,
                owner_team=owner_team,
            )
        )

    upsert_nodes(nodes)

    for name in sorted(set(inputs)):
        dataset_id = make_dataset_id(name)
        edge = Edge(
            from_canonical_id=dataset_id,
            to_canonical_id=process_id,
            edge_type="READS_FROM",
            process_canonical_id=process_id,
            run_canonical_id=resolved_run_id,
            confidence_score=confidence,
            verification_state=verification_state,
            source_system=source_system,
        )
        edges.append(edge)
        evidence.append(
            Evidence(
                edge_key=edge_key(edge.from_canonical_id, edge.to_canonical_id, edge.edge_type, edge.process_canonical_id),
                evidence_type=evidence_type,
                source_system=source_system,
                source_reference=source_reference,
                evidence_uri=evidence_uri,
                confidence_contribution=confidence,
            )
        )

    for name in sorted(set(outputs)):
        dataset_id = make_dataset_id(name)
        edge = Edge(
            from_canonical_id=process_id,
            to_canonical_id=dataset_id,
            edge_type="WRITES_TO",
            process_canonical_id=process_id,
            run_canonical_id=resolved_run_id,
            confidence_score=confidence,
            verification_state=verification_state,
            source_system=source_system,
        )
        edges.append(edge)
        evidence.append(
            Evidence(
                edge_key=edge_key(edge.from_canonical_id, edge.to_canonical_id, edge.edge_type, edge.process_canonical_id),
                evidence_type=evidence_type,
                source_system=source_system,
                source_reference=source_reference,
                evidence_uri=evidence_uri,
                confidence_contribution=confidence,
            )
        )

    upsert_edges(edges)
    insert_evidence(evidence)
    return {"process_id": process_id, "run_id": resolved_run_id, "edge_count": len(edges), "source_reference": source_reference}
