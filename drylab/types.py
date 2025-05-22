from __future__ import annotations

import datetime as _dt
import hashlib
from typing import NewType

from pydantic import BaseModel, Field

Blob      = NewType("Blob", bytes)         # raw bytes from any source
SchemaId  = NewType("SchemaId", str)       # e.g. "RMSD_CSV@1"
Sha256    = NewType("Sha256", str)         # 64‑char hex digest
Timestamp = NewType("Timestamp", int)      # epoch ms


def _now_ts() -> Timestamp:
    return Timestamp(int(_dt.datetime.utcnow().timestamp() * 1000))


class EventHeader(BaseModel):
    """Immutable metadata for every payload with clear naming."""
    id: Sha256
    schema_id: SchemaId = Field(..., alias="schema")
    ts: Timestamp = Field(default_factory=_now_ts)
    model_config = {
        "populate_by_field_name": True,
        "frozen": True,
    }
    @property
    def schema(self) -> SchemaId:
        """Convenience property mapping to alias 'schema'."""
        return self.schema_id


class Event(BaseModel):
    header: EventHeader
    blob: Blob

    model_config = {
        "frozen": True,
        "arbitrary_types_allowed": True,
    }


class EventRow(Event):
    run_id: str
    seq: int
