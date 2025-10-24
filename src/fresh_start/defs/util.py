import os
import re
import yaml
from pathlib import Path
from typing import List, Optional

BASE_DIR = os.path.dirname(__file__)
yaml_path = os.path.join(BASE_DIR, "replication_mapping_generated.yaml")

print(f"Loading YAML from: {yaml_path}")
assert os.path.isfile(yaml_path), f"YAML file not found at {yaml_path}"

# Load enabled groups from YAML, optionally filtered by prefix.
def load_enabled_groups(yaml_path: Path, prefix: Optional[str] = None) -> List[dict]:
    """Load enabled groups from YAML, optionally filtered by prefix."""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    groups = [g for g in data.get("groups", []) if g.get("enabled")]

    if prefix:
        assert len(prefix) == 1, "Prefix must be a single character"
        pattern = re.compile(rf"^{re.escape(prefix)}", re.IGNORECASE)
        groups = [g for g in groups if pattern.match(g.get("name", ""))]

    return groups