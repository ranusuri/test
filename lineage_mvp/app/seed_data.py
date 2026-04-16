from __future__ import annotations

from app.adapters.dataplex_adapter import ingest_sample as ingest_dataplex
from app.adapters.onprem_adapter import ingest_sample as ingest_onprem
from app.adapters.openlineage_adapter import ingest_event
from app.adapters.purview_adapter import ingest_sample as ingest_purview
from app.config import SAMPLES_DIR
from app.database import init_db
from app.models import OpenLineageEvent
from app.adapters.common import load_json


def main() -> None:
    init_db()
    ingest_onprem()
    ingest_dataplex()
    ingest_purview()
    sample_event = OpenLineageEvent(**load_json(SAMPLES_DIR / 'openlineage_event.json'))
    ingest_event(sample_event)
    print('Seeded lineage MVP data successfully.')


if __name__ == '__main__':
    main()
