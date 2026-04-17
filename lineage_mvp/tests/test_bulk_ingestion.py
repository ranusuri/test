from app.services.bulk_ingestion import bulk_ingest_api_specs, bulk_ingest_logs, bulk_ingest_sql


def test_bulk_ingest_sql():
    result = bulk_ingest_sql('data/ingest_samples/sql')
    assert result['matched_file_count'] >= 1
    assert result['ingested_count'] >= 1


def test_bulk_ingest_logs():
    result = bulk_ingest_logs('data/ingest_samples/logs')
    assert result['matched_file_count'] >= 1
    assert result['ingested_count'] >= 1


def test_bulk_ingest_api_specs():
    result = bulk_ingest_api_specs('data/ingest_samples/apis')
    assert result['matched_file_count'] >= 1
    assert result['ingested_count'] >= 1
