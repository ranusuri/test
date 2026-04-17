from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import yaml


def _load_spec(path: Path) -> Dict:
    raw_text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        return yaml.safe_load(raw_text)
    return json.loads(raw_text)


def _normalize_name(*parts: str) -> str:
    return "_".join(part.strip().replace('/', '_').replace('.', '_') for part in parts if part).strip('_')


def parse_openapi_spec(path: str | Path) -> Dict:
    source_path = Path(path)
    spec = _load_spec(source_path)
    spec_name = spec.get("info", {}).get("title", source_path.stem)

    operations: List[Dict] = []
    for route, methods in spec.get("paths", {}).items():
        for method, details in methods.items():
            if method.lower() not in {"get", "post", "put", "patch", "delete"}:
                continue
            operation_id = details.get("operationId") or _normalize_name(method, route)
            tags = details.get("tags") or [spec_name]
            input_name = _normalize_name(spec_name, tags[0], route, "request")
            output_name = _normalize_name(spec_name, tags[0], route, "response")
            operations.append(
                {
                    "process_name": operation_id,
                    "sources": [input_name],
                    "targets": [output_name],
                    "route": route,
                    "method": method.upper(),
                }
            )

    return {
        "source_path": str(source_path),
        "system_name": _normalize_name(spec_name),
        "operations": operations,
    }
