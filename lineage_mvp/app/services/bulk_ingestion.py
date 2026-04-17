from __future__ import annotations

from pathlib import Path
from typing import Callable

from app.adapters.api_spec_adapter import ingest_api_spec
from app.adapters.log_file_adapter import ingest_log_file
from app.adapters.sql_file_adapter import ingest_sql_file

IngestFn = Callable[[str | Path], dict]


def _bulk_ingest(directory: str | Path, patterns: tuple[str, ...], ingest_fn: IngestFn) -> dict:
    root = Path(directory)
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Directory not found: {root}")

    matched_files = []
    for pattern in patterns:
        matched_files.extend(sorted(root.rglob(pattern)))

    results = []
    errors = []
    for file_path in sorted(set(matched_files)):
        try:
            details = ingest_fn(file_path)
            results.append({"path": str(file_path), "details": details})
        except Exception as exc:  # pragma: no cover - defensive path for runtime use
            errors.append({"path": str(file_path), "error": str(exc)})

    return {
        "directory": str(root),
        "matched_file_count": len(set(matched_files)),
        "ingested_count": len(results),
        "error_count": len(errors),
        "results": results,
        "errors": errors,
    }


def bulk_ingest_sql(directory: str | Path) -> dict:
    return _bulk_ingest(directory, ("*.sql",), ingest_sql_file)


def bulk_ingest_logs(directory: str | Path) -> dict:
    return _bulk_ingest(directory, ("*.log", "*.txt"), ingest_log_file)


def bulk_ingest_api_specs(directory: str | Path) -> dict:
    return _bulk_ingest(directory, ("*.yaml", "*.yml", "*.json"), ingest_api_spec)
