from dataclasses import dataclass
from typing import Literal


@dataclass
class MessageHeader:
    event_id: str
    event_type: Literal[
        "image.ocr.request",
        "image.validation.request",
        "image.thumbnail.request",
    ]
    trace_id: str
    timestamp: str
    source_service: str


@dataclass
class MessagePayload:
    gid: str
    bucket: str
    original_object_key: str
