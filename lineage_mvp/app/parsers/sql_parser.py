from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List

_SQL_TARGET_PATTERNS = [
    re.compile(r"insert\s+into\s+([\w.]+)", re.IGNORECASE),
    re.compile(r"create\s+(?:or\s+replace\s+)?table\s+([\w.]+)\s+as", re.IGNORECASE),
]
_SQL_SOURCE_PATTERN = re.compile(r"(?:from|join)\s+([\w.]+)", re.IGNORECASE)


def _normalize_dataset_name(name: str) -> str:
    return name.strip().strip('"`[]').replace('.', '_')


def parse_sql_file(path: str | Path) -> Dict:
    source_path = Path(path)
    sql_text = source_path.read_text(encoding="utf-8")

    target_names: List[str] = []
    for pattern in _SQL_TARGET_PATTERNS:
        target_names.extend(match.group(1) for match in pattern.finditer(sql_text))

    source_names = [match.group(1) for match in _SQL_SOURCE_PATTERN.finditer(sql_text)]
    source_names = [name for name in source_names if name not in target_names]

    return {
        "source_path": str(source_path),
        "process_name": source_path.stem,
        "sources": [_normalize_dataset_name(name) for name in dict.fromkeys(source_names)],
        "targets": [_normalize_dataset_name(name) for name in dict.fromkeys(target_names)],
    }
