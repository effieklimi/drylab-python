from __future__ import annotations # enables modern Python type hinting behavior
import datetime as _dt # the underscore prefix suggests it's for internal use
import hashlib # module for creating hash values (like SHA-256)
from typing import NewType # tool from Python's typing system to create distinct types
from pydantic import BaseModel, Field

Blob      = NewType("Blob", bytes)         # raw bytes from any source. Represents raw binary data
SchemaId  = NewType("SchemaId", str)       # A string identifier for data schemas. e.g. "RMSD_CSV@1"
Sha256    = NewType("Sha256", str)         # string containing a SHA-256 hash (64 hexadecimal characters)
Timestamp = NewType("Timestamp", int)      # epoch in ms


def _now_ts() -> Timestamp:
    """Returns the current timestamp in milliseconds since the Unix epoch."""
    return Timestamp(int(_dt.datetime.utcnow().timestamp() * 1000)) # Returns it as a Timestamp type


class EventHeader(BaseModel):
    id: Sha256
    schema_id: SchemaId = Field(alias="schema")
    ts: Timestamp = Field(default_factory=_now_ts)

    model_config = {
        "populate_by_name": True,   # <-- correct key
        "frozen": True,
    }

    # convenience for legacy `.schema`
    @property
    def schema(self) -> SchemaId:
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
