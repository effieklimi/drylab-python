from __future__ import annotations

import datetime as _dt
import hashlib
from typing import NewType, Callable, Iterable, List, Tuple

from pydantic import BaseModel, Field

Blob      = NewType("Blob", bytes)        # raw bytes from any source
SchemaId  = NewType("SchemaId", str)      # e.g. "RMSD_CSV@1"
Sha256    = NewType("Sha256", str)        # 64‑char hex digest
Timestamp = NewType("Timestamp", int)     # epoch ms


def _now_ts() -> Timestamp:
    return Timestamp(int(_dt.datetime.utcnow().timestamp() * 1000))


class EventHeader(BaseModel):
    """Minimal, immutable metadata for every payload."""

    id: Sha256
    schema: SchemaId
    ts: Timestamp = Field(default_factory=_now_ts)


class Event(BaseModel):
    header: EventHeader
    blob: Blob

    model_config = {
        "frozen": True,  # immutability -> hashable & audit‑proof
        "arbitrary_types_allowed": True,
    }


class EventRow(Event):
    run_id: str
    seq: int  # monotonic row id within a run