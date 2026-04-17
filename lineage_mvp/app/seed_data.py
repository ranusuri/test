from __future__ import annotations

from app.adapters.api_spec_adapter import ingest_api_spec
from app.adapters.dataplex_adapter import ingest_sample as ingest_dataplex
from app.adapters.log_file_adapter import ingest_log_file
from app.adapters.onprem_adapter import ingest_sample as ingest_onprem
from app.adapters.sql_file_adapter import ingest_sql_file
from app.adapters.openlineage_adapter import ingest_event
from app.adapters.purview_adapter import ingest_sample as ingest_purview
from app.config import BASE_DIR, SAMPLES_DIR
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
    ingest_sql_file(BASE_DIR / 'data' / 'ingest_samples' / 'sql' / 'customer_publish.sql')
    ingest_log_file(BASE_DIR / 'data' / 'ingest_samples' / 'logs' / 'customer_api_bridge.log')
    ingest_api_spec(BASE_DIR / 'data' / 'ingest_samples' / 'apis' / 'customer_publish_api.yaml')
    print('Seeded lineage MVP data successfully, including SQL/log/API-derived lineage.')


if __name__ == '__main__':
    main()
