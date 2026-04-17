from pathlib import Path

from app.parsers.api_spec_parser import parse_openapi_spec
from app.parsers.log_parser import parse_log_file
from app.parsers.sql_parser import parse_sql_file



def test_parse_sql_file():
    parsed = parse_sql_file(Path('data/ingest_samples/sql/customer_publish.sql'))
    assert 'customer_raw_bridge' in parsed['sources']
    assert 'orders_reference' in parsed['sources']
    assert 'customer_publish_ready' in parsed['targets']
    assert parsed['statement_count'] >= 1



def test_parse_log_file():
    parsed = parse_log_file(Path('data/ingest_samples/logs/customer_api_bridge.log'))
    assert parsed['process_name'] == 'customer_api_bridge'
    assert 'customer_publish_ready' in parsed['sources']



def test_parse_api_spec():
    parsed = parse_openapi_spec(Path('data/ingest_samples/apis/customer_publish_api.yaml'))
    assert parsed['operations'][0]['process_name'] == 'publishCustomers'
