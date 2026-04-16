from __future__ import annotations


def canonical_dataset_id(platform: str, environment: str, system: str, container: str, obj: str) -> str:
    return f"eld://dataset/{platform}/{environment}/{system}/{container}/{obj}"


def canonical_process_id(platform: str, environment: str, system: str, process_type: str, name: str) -> str:
    return f"eld://process/{platform}/{environment}/{system}/{process_type}/{name}"


def canonical_run_id(process_canonical_id: str, run_id: str) -> str:
    return f"eld://run/{process_canonical_id}/{run_id}"


def canonical_system_id(platform: str, environment: str, system: str) -> str:
    return f"eld://system/{platform}/{environment}/{system}"
