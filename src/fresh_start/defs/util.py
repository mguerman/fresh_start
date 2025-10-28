import yaml
import re
from pathlib import Path
from typing import List, Optional

def load_enabled_groups(yaml_path: Path, prefix: Optional[str] = None) -> List[dict]:
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    groups = [g for g in data.get("groups", []) if g.get("enabled")]
    if prefix:
        assert len(prefix) == 1
        pattern = re.compile(rf"^{re.escape(prefix)}", re.I)
        groups = [g for g in groups if pattern.match(g.get("name", ""))]
    return groups