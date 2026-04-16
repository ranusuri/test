from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / 'lineage_mvp.db'
EXPORT_DIR = BASE_DIR / 'export'
SAMPLES_DIR = BASE_DIR / 'data' / 'samples'
