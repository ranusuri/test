from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Set

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sqlglot_lineage


class SQLLineageError(Exception):
    pass


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip('"`[]').replace('.', '_')


def _extract_tables(parsed_expressions: List[exp.Expression]) -> tuple[list[str], list[str]]:
    sources: Set[str] = set()
    targets: Set[str] = set()

    for expression in parsed_expressions:
        for table in expression.find_all(exp.Table):
            parts = [part for part in (table.catalog, table.db, table.name) if part]
            name = ".".join(parts) if parts else table.name
            if name:
                sources.add(_normalize_identifier(name))

        insert = expression.find(exp.Insert)
        if insert and isinstance(insert.this, exp.Table):
            parts = [part for part in (insert.this.catalog, insert.this.db, insert.this.name) if part]
            targets.add(_normalize_identifier(".".join(parts) if parts else insert.this.name))

        create = expression.find(exp.Create)
        if create and isinstance(create.this, exp.Table):
            parts = [part for part in (create.this.catalog, create.this.db, create.this.name) if part]
            targets.add(_normalize_identifier(".".join(parts) if parts else create.this.name))

    sources -= targets
    return sorted(sources), sorted(targets)


def _extract_field_lineage(parsed_expressions: List[exp.Expression], targets: list[str]) -> list[dict]:
    field_mappings: list[dict] = []
    if not targets:
        return field_mappings

    target_name = targets[0].replace('_', '.')
    for expression in parsed_expressions:
        query = expression.find(exp.Select)
        if not query:
            continue
        aliases = [projection.alias_or_name for projection in query.expressions if projection.alias_or_name]
        for alias in aliases:
            try:
                node = sqlglot_lineage(alias, query)
            except Exception:
                continue
            source_columns = sorted({_normalize_identifier(col.sql(dialect="")) for col in node.source.find_all(exp.Column)})
            if source_columns:
                field_mappings.append(
                    {
                        "target_field": f"{_normalize_identifier(target_name)}_{_normalize_identifier(alias)}",
                        "source_fields": source_columns,
                    }
                )
    return field_mappings


def parse_sql_file(path: str | Path) -> Dict:
    source_path = Path(path)
    sql_text = source_path.read_text(encoding="utf-8")

    try:
        parsed = sqlglot.parse(sql_text)
    except Exception as exc:
        raise SQLLineageError(f"Failed to parse SQL file {source_path}: {exc}") from exc

    sources, targets = _extract_tables(parsed)
    field_mappings = _extract_field_lineage(parsed, targets)

    return {
        "source_path": str(source_path),
        "process_name": source_path.stem,
        "sources": sources,
        "targets": targets,
        "field_mappings": field_mappings,
        "statement_count": len(parsed),
    }
