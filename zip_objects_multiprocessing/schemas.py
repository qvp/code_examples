from dataclasses import dataclass
from typing import Dict, List


@dataclass
class Obj:
    vars: Dict[str, str]
    objects: List[str]
