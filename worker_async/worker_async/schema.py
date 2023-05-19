from enum import Enum
from typing import Any
from dataclasses import dataclass


@dataclass
class TaskMessage:
    id: int
    session_id: int
    payload: dict


@dataclass
class CommandMessage:
    class Types(str, Enum):
        session_canceled = 'session_canceled'
        say_hello = 'say_hello'

    type: Types
    payload: Any = None


@dataclass
class TaskError:
    message: str
    traceback: str
