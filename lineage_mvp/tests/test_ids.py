from app.services.id_service import canonical_dataset_id, canonical_process_id


def test_canonical_dataset_id():
    assert canonical_dataset_id('gcp', 'prod', 'bigquery', 'customer360', 'orders') == 'eld://dataset/gcp/prod/bigquery/customer360/orders'


def test_canonical_process_id():
    assert canonical_process_id('azure', 'prod', 'synapse', 'pipeline', 'load_customers') == 'eld://process/azure/prod/synapse/pipeline/load_customers'
