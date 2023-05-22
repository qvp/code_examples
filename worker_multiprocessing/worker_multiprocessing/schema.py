from enum import Enum
from typing import Any
from dataclasses import dataclass


@dataclass
class TaskMessage:
    id: int
    session_id: int
    payload: dict


@dataclass
class TaskProgress:
    message: str
    in_percentages: int = 0


@dataclass
class TaskDone:
    task_id: int
    payload: Any = None


class QueueMessageType(str, Enum):
    done = 'done'
    error = 'error'
    progress_changed = 'progress_changed'
    session_canceled = 'session_canceled'


@dataclass
class QueueMessage:
    type: QueueMessageType
    payload: Any = None
