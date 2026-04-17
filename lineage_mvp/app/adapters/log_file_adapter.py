from __future__ import annotations

from pathlib import Path

from app.adapters.derived_ingestion import ingest_standalone_process_with_edges
from app.parsers.log_parser import parse_log_file


def ingest_log_file(path: str | Path) -> dict:
    parsed = parse_log_file(path)
    return ingest_standalone_process_with_edges(
        process_name=parsed["process_name"],
        source_system="log_parser",
        evidence_type="LOG_FILE",
        evidence_uri=f"file://{parsed['source_path']}",
        source_reference=Path(parsed["source_path"]).name,
        inputs=parsed["sources"],
        outputs=parsed["targets"],
        platform="custom",
        environment="prod",
        system="log_parser",
        container="derived",
        process_type="runtime_log",
        domain="customer360",
        owner_team="lineage-mvp",
        run_id=parsed["run_id"],
    )
