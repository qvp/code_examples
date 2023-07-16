from enum import StrEnum
from dataclasses import dataclass
from typing import Dict, List, Any


@dataclass
class Obj:
    vars: Dict[str, str]
    objects: List[str]


class QueueMessageType(StrEnum):
    NEW_OBJ = 'new_obj'
    ARCHIVE_PROCESSED = 'archive_processed'
    ERROR = 'error'


@dataclass
class QueueMessage:
    type: QueueMessageType
    payload: Any = None
