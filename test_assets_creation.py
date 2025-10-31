# Test your asset creation directly
from src.fresh_start.defs.assets import build_assets_from_yaml
from pathlib import Path

BASE_DIR = Path(__file__).parent
YAML_PATH = BASE_DIR / "src" / "fresh_start" / "defs" / "replication_mapping_generated.yaml"

try:
    assets = build_assets_from_yaml(str(YAML_PATH), filtered_groups=["d10"])
    print(f"Result: {len(assets)} assets created")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()