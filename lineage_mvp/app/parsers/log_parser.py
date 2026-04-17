from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List

READ_PATTERN = re.compile(r"reading\s+(?:table|dataset|file):\s*([\w./-]+)", re.IGNORECASE)
WRITE_PATTERN = re.compile(r"writing\s+(?:table|dataset|file):\s*([\w./-]+)", re.IGNORECASE)
PROCESS_PATTERN = re.compile(r"job:\s*([\w.-]+)", re.IGNORECASE)
RUN_PATTERN = re.compile(r"run_id:\s*([\w.-]+)", re.IGNORECASE)


def _normalize_dataset_name(name: str) -> str:
    return name.strip().replace('/', '_').replace('.', '_')


def parse_log_file(path: str | Path) -> Dict:
    source_path = Path(path)
    text = source_path.read_text(encoding="utf-8")

    process_match = PROCESS_PATTERN.search(text)
    run_match = RUN_PATTERN.search(text)
    sources: List[str] = [m.group(1) for m in READ_PATTERN.finditer(text)]
    targets: List[str] = [m.group(1) for m in WRITE_PATTERN.finditer(text)]

    return {
        "source_path": str(source_path),
        "process_name": process_match.group(1) if process_match else source_path.stem,
        "run_id": run_match.group(1) if run_match else f"{source_path.stem}-run-001",
        "sources": [_normalize_dataset_name(name) for name in dict.fromkeys(sources)],
        "targets": [_normalize_dataset_name(name) for name in dict.fromkeys(targets)],
    }
