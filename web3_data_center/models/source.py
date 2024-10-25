from enum import Enum, auto
from dataclasses import dataclass
from typing import Any

class SourceType(Enum):
    TEXT = auto()
    URL = auto()
    IMAGE_URL = auto()
    HANDLE = auto()
    IMAGE_DATA = auto()
    TWEET = auto()

@dataclass
class Source:
    type: SourceType
    content: Any
