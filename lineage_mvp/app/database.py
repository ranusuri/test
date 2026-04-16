from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterable, List, Optional

from app.config import DB_PATH
from app.models import AliasRecord, Edge, Evidence, Node


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS nodes (
    canonical_id TEXT PRIMARY KEY,
    node_type TEXT NOT NULL,
    display_name TEXT NOT NULL,
    platform TEXT NOT NULL,
    environment TEXT NOT NULL,
    source_system TEXT NOT NULL,
    domain TEXT,
    owner_team TEXT,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS edges (
    edge_key TEXT PRIMARY KEY,
    from_canonical_id TEXT NOT NULL,
    to_canonical_id TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    process_canonical_id TEXT,
    run_canonical_id TEXT,
    confidence_score REAL NOT NULL,
    verification_state TEXT NOT NULL,
    source_system TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS evidence (
    evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
    edge_key TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_reference TEXT NOT NULL,
    evidence_uri TEXT NOT NULL,
    collected_ts TEXT NOT NULL,
    confidence_contribution REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS aliases (
    alias_key TEXT PRIMARY KEY,
    source_system TEXT NOT NULL,
    native_id TEXT NOT NULL,
    fq_name TEXT NOT NULL,
    canonical_id TEXT NOT NULL,
    match_method TEXT NOT NULL,
    match_confidence REAL NOT NULL,
    status TEXT NOT NULL
);
"""


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA_SQL)



def edge_key(from_id: str, to_id: str, edge_type: str, process_id: Optional[str]) -> str:
    return '|'.join([from_id, to_id, edge_type, process_id or ''])



def upsert_nodes(nodes: Iterable[Node]) -> None:
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO nodes (canonical_id, node_type, display_name, platform, environment, source_system, domain, owner_team, status)
            VALUES (:canonical_id, :node_type, :display_name, :platform, :environment, :source_system, :domain, :owner_team, :status)
            ON CONFLICT(canonical_id) DO UPDATE SET
                display_name=excluded.display_name,
                platform=excluded.platform,
                environment=excluded.environment,
                source_system=excluded.source_system,
                domain=excluded.domain,
                owner_team=excluded.owner_team,
                status=excluded.status
            """,
            [node.model_dump() for node in nodes],
        )



def upsert_edges(edges: Iterable[Edge]) -> None:
    with get_conn() as conn:
        payload = []
        for edge in edges:
            row = edge.model_dump()
            row['edge_key'] = edge_key(edge.from_canonical_id, edge.to_canonical_id, edge.edge_type, edge.process_canonical_id)
            payload.append(row)
        conn.executemany(
            """
            INSERT INTO edges (edge_key, from_canonical_id, to_canonical_id, edge_type, process_canonical_id, run_canonical_id, confidence_score, verification_state, source_system)
            VALUES (:edge_key, :from_canonical_id, :to_canonical_id, :edge_type, :process_canonical_id, :run_canonical_id, :confidence_score, :verification_state, :source_system)
            ON CONFLICT(edge_key) DO UPDATE SET
                run_canonical_id=excluded.run_canonical_id,
                confidence_score=MAX(edges.confidence_score, excluded.confidence_score),
                verification_state=excluded.verification_state,
                source_system=excluded.source_system
            """,
            payload,
        )



def insert_evidence(records: Iterable[Evidence]) -> None:
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO evidence (edge_key, evidence_type, source_system, source_reference, evidence_uri, collected_ts, confidence_contribution)
            VALUES (:edge_key, :evidence_type, :source_system, :source_reference, :evidence_uri, :collected_ts, :confidence_contribution)
            """,
            [
                {
                    **record.model_dump(),
                    'collected_ts': record.collected_ts.isoformat(),
                }
                for record in records
            ],
        )



def upsert_aliases(records: Iterable[AliasRecord]) -> None:
    with get_conn() as conn:
        payload = []
        for r in records:
            row = r.model_dump()
            row['alias_key'] = f"{r.source_system}|{r.native_id}"
            payload.append(row)
        conn.executemany(
            """
            INSERT INTO aliases (alias_key, source_system, native_id, fq_name, canonical_id, match_method, match_confidence, status)
            VALUES (:alias_key, :source_system, :native_id, :fq_name, :canonical_id, :match_method, :match_confidence, :status)
            ON CONFLICT(alias_key) DO UPDATE SET
                fq_name=excluded.fq_name,
                canonical_id=excluded.canonical_id,
                match_method=excluded.match_method,
                match_confidence=excluded.match_confidence,
                status=excluded.status
            """,
            payload,
        )



def search_nodes(query: str) -> List[sqlite3.Row]:
    q = f"%{query.lower()}%"
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT * FROM nodes
            WHERE lower(display_name) LIKE ? OR lower(canonical_id) LIKE ?
            ORDER BY display_name
            """,
            (q, q),
        ).fetchall()



def get_node(canonical_id: str) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM nodes WHERE canonical_id = ?", (canonical_id,)).fetchone()



def get_edges() -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM edges").fetchall()



def get_evidence_by_edge(edge_key_value: str) -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM evidence WHERE edge_key = ?", (edge_key_value,)).fetchall()



def get_all_related_edges(asset_id: str) -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM edges WHERE from_canonical_id = ? OR to_canonical_id = ?",
            (asset_id, asset_id),
        ).fetchall()
