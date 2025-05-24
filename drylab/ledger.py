import hashlib
import sqlite3
from pathlib import Path
from typing import Iterator
from .validators import EventValidator
from .types import Blob, EventHeader, EventRow, SchemaId, Sha256
from .schema_registry import validate_schema
import asyncio

_DB_SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS blobs (
    sha TEXT PRIMARY KEY,
    bytes BLOB
);
CREATE TABLE IF NOT EXISTS events (
    run_id TEXT,
    seq    INTEGER,
    sha    TEXT,
    schema TEXT,
    ts     INTEGER,
    PRIMARY KEY (run_id, seq)
);
"""


class Ledger:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.executescript(_DB_SCHEMA_SQL)
        self._db.commit()

    @staticmethod
    def _hash(blob: Blob) -> Sha256:
        return Sha256(hashlib.sha256(blob).hexdigest())

    # ------------------------------------------------ api
     # internal: set by publish() when a new row is committed
    _condition = asyncio.Condition()

    async def subscribe(self, run_id: str, cursor: int = 0):
        """
        Async generator that yields new EventRow objects for the given run_id.
        It blocks until at least one new event exists or until the generator
        is exhausted (e.g. all reactors finished and ledger is closed).
        """
        while True:
            # 1️⃣  fetch anything newer than cursor
            rows = self._db.execute(
                "SELECT seq,sha,schema,ts FROM events WHERE run_id=? AND seq>? ORDER BY seq",
                (run_id, cursor),
            ).fetchall()

            if rows:
                for seq, sha, schema, ts in rows:
                    header = EventHeader(id=sha, schema=schema, ts=ts)
                    blob = self.cat(sha)
                    yield EventRow(
                        header=header,   # keyword args instead of positionals
                        blob=blob,
                        run_id=run_id,
                        seq=seq,
                    )
                    cursor = seq
                continue               # loop again without blocking

            # 2️⃣  wait for a publish() to notify
            async with self._condition:
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=5)
                except asyncio.TimeoutError:
                    # No new data for 5 s  → assume upstream is done
                    return
    
    
    def publish(self, event: EventRow) -> Sha256:
        """Publish an event to the ledger.
        
        Args:
            event: The complete EventRow to publish
            
        Returns:
            Sha256: The hash of the event's blob
            
        Raises:
            ValueError: If the event fails validation
        """
        # Validate the complete event
        validator = EventValidator(event)
        if not validator.validate():
            raise ValueError(f"Invalid event: {validator.validation_error}")

        sha = event.header.id
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO blobs (sha, bytes) VALUES (?,?)",
                (sha, event.blob)
            )
            seq = self._db.execute(
                "SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE run_id=?",
                (event.run_id,)
            ).fetchone()[0]
            self._db.execute(
                "INSERT INTO events(run_id,seq,sha,schema,ts) VALUES(?,?,?,?,strftime('%s','now')*1000)",
                (event.run_id, seq, sha, event.header.schema_id),
            )
            asyncio.create_task(self._notify())
        return sha
    
    
    async def _notify(self):
        async with self._condition:
            self._condition.notify_all()

    def cat(self, sha: Sha256) -> Blob:
        row = self._db.execute("SELECT bytes FROM blobs WHERE sha=?", (sha,)).fetchone()
        if not row:
            raise KeyError(sha)
        return row[0]

    def tail(self, run_id: str, from_seq: int = 0) -> Iterator[EventRow]:
        cursor = from_seq
        while True:
            rows = self._db.execute(
                "SELECT seq,sha,schema,ts FROM events WHERE run_id=? AND seq>? ORDER BY seq",
                (run_id, cursor),
            ).fetchall()
            if not rows:
                break
            for seq, sha, schema, ts in rows:
                header = EventHeader(id=sha, schema=schema, ts=ts)  # Use schema
                blob = self.cat(sha)
                event = EventRow(header=header, blob=blob, run_id=run_id, seq=seq)
                
                validator = EventValidator(event)
                if not validator.validate():
                    raise ValueError(f"Invalid event in database: {validator.validation_error}")
                
                yield event
                cursor = seq

    def replay(self, run_id: str):
        return self.tail(run_id)