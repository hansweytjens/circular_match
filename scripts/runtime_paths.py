import os
from pathlib import Path


APP_ROOT = Path(os.getenv("APP_ROOT", "/app"))
DEFAULT_CONTEXT_DIR = Path(os.getenv("CONTEXT_DIR", str(APP_ROOT / "context" / "active")))
DEFAULT_OUTPUT_ROOT = Path(os.getenv("OUTPUT_ROOT", str(APP_ROOT / "output")))
