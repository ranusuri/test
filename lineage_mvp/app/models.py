from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional
from pydantic import BaseModel, Field

NodeType = Literal['DATASET', 'PROCESS', 'RUN', 'SYSTEM']
EdgeType = Literal['READS_FROM', 'WRITES_TO', 'TRANSFORMS', 'PUBLISHES_TO']
VerificationState = Literal['INFERRED', 'OBSERVED', 'VERIFIED']


class Node(BaseModel):
    canonical_id: str
    node_type: NodeType
    display_name: str
    platform: str
    environment: str
    source_system: str
    domain: Optional[str] = None
    owner_team: Optional[str] = None
    status: str = 'active'


class Edge(BaseModel):
    from_canonical_id: str
    to_canonical_id: str
    edge_type: EdgeType
    process_canonical_id: Optional[str] = None
    run_canonical_id: Optional[str] = None
    confidence_score: float = Field(ge=0.0, le=1.0)
    verification_state: VerificationState = 'OBSERVED'
    source_system: str


class Evidence(BaseModel):
    edge_key: str
    evidence_type: str
    source_system: str
    source_reference: str
    evidence_uri: str
    collected_ts: datetime = Field(default_factory=datetime.utcnow)
    confidence_contribution: float = Field(default=0.0, ge=0.0, le=1.0)


class AliasRecord(BaseModel):
    source_system: str
    native_id: str
    fq_name: str
    canonical_id: str
    match_method: str = 'rules'
    match_confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    status: str = 'active'


class SearchResponse(BaseModel):
    results: List[Node]


class LineageHop(BaseModel):
    node: Node
    via_edge_type: Optional[str] = None
    process_canonical_id: Optional[str] = None
    confidence_score: Optional[float] = None


class LineageResponse(BaseModel):
    asset_id: str
    direction: Literal['upstream', 'downstream']
    hops: List[LineageHop]


class OpenLineageDataset(BaseModel):
    namespace: str
    name: str


class OpenLineageJob(BaseModel):
    namespace: str
    name: str


class OpenLineageRun(BaseModel):
    runId: str


class OpenLineageEvent(BaseModel):
    eventType: str
    eventTime: datetime
    job: OpenLineageJob
    run: OpenLineageRun
    inputs: List[OpenLineageDataset]
    outputs: List[OpenLineageDataset]
    producer: Optional[str] = 'custom-batch'




class FileIngestRequest(BaseModel):
    path: str


class FileIngestResponse(BaseModel):
    status: str = 'accepted'
    details: dict

class PublishRequest(BaseModel):
    asset_id: str
    target: Literal['purview_export'] = 'purview_export'


class PublishResponse(BaseModel):
    target: str
    export_file: str
    published_asset_count: int
    published_edge_count: int
