from pathlib import Path


OUTPUT_ROOT = Path("/workspace/output")
GEMINI_PROFILES_DIR = OUTPUT_ROOT / "gemini_profiles"
MATERIAL_MATCHES_DIR = OUTPUT_ROOT / "material_matches"
DATABASES_DIR = OUTPUT_ROOT / "databases"
DEFAULT_DB_PATH = DATABASES_DIR / "company_material_flows.sqlite3"

