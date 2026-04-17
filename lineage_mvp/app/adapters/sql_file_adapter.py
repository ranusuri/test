from __future__ import annotations

from pathlib import Path

from app.adapters.derived_ingestion import ingest_standalone_process_with_edges
from app.parsers.sql_parser import parse_sql_file


def ingest_sql_file(path: str | Path) -> dict:
    parsed = parse_sql_file(path)
    return ingest_standalone_process_with_edges(
        process_name=parsed["process_name"],
        source_system="sql_parser",
        evidence_type="SQL_FILE",
        evidence_uri=f"file://{parsed['source_path']}",
        source_reference=Path(parsed["source_path"]).name,
        inputs=parsed["sources"],
        outputs=parsed["targets"],
        platform="onprem",
        environment="prod",
        system="sql_parser",
        container="derived",
        process_type="sql_file",
        domain="customer360",
        owner_team="lineage-mvp",
    )
