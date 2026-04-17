from __future__ import annotations

from pathlib import Path

from app.adapters.derived_ingestion import ingest_standalone_process_with_edges
from app.parsers.api_spec_parser import parse_openapi_spec


def ingest_api_spec(path: str | Path) -> dict:
    parsed = parse_openapi_spec(path)
    results = []
    for operation in parsed["operations"]:
        results.append(
            ingest_standalone_process_with_edges(
                process_name=operation["process_name"],
                source_system="api_spec",
                evidence_type="OPENAPI_SPEC",
                evidence_uri=f"file://{parsed['source_path']}",
                source_reference=f"{Path(parsed['source_path']).name}:{operation['method']} {operation['route']}",
                inputs=operation["sources"],
                outputs=operation["targets"],
                platform="custom",
                environment="prod",
                system=parsed["system_name"],
                container="api",
                process_type="api_operation",
                domain="customer360",
                owner_team="lineage-mvp",
                run_id=f"{operation['process_name']}-spec",
            )
        )
    return {
        "source_path": parsed["source_path"],
        "operation_count": len(results),
        "results": results,
    }
