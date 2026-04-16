BASE_CONFIDENCE = {
    'dataplex': 0.95,
    'purview': 0.95,
    'onprem_sql': 0.85,
    'openlineage': 0.90,
}


def compute_confidence(source_system: str, corroboration_count: int = 1) -> float:
    base = BASE_CONFIDENCE.get(source_system, 0.75)
    bonus = min(max(corroboration_count - 1, 0) * 0.03, 0.08)
    return round(min(base + bonus, 0.99), 2)
