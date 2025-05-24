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
        Async generator that yields EventRow objects for *run_id*.
        It blocks until new rows appear; when no rows arrive for 5 s the
        generator returns so the calling reactor can finish.
        """
        while True:
            rows = self._db.execute(
                "SELECT seq, sha, schema, ts "
                "FROM   events "
                "WHERE  run_id = ? AND seq > ? "
                "ORDER  BY seq",
                (run_id, cursor),
            ).fetchall()

            if rows:
                for seq, sha, schema, ts in rows:
                    cursor = seq               # ← update BEFORE yield
                    header = EventHeader(id=sha, schema=schema, ts=ts)
                    blob   = self.cat(sha)
                    yield EventRow(
                        header=header,
                        blob=blob,
                        run_id=run_id,
                        seq=seq,
                    )
                # → loop immediately to fetch rows newer than the final seq
                continue

            # ── No rows; wait for a notifier or timeout ─────────────────
            async with self._condition:
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=5)
                except asyncio.TimeoutError:
                    return                      # generator exhausted



    # ------------------------------------------------------------------
    # drylab/ledger.py  (inside class Ledger)
    # ---------------------------------------------------------------
    # drylab/ledger.py  ───────────────────────────────────────────────────
    def publish(self, event: EventRow) -> bool:
        """
        Store *event* exactly once.

        Returns
        -------
        bool
            True  – event row inserted and subscribers notified  
            False – identical (run_id, schema, sha) already present; nothing inserted

        Raises
        ------
        ValueError
            If schema-validation fails.
        """
        # 1. Validate against JSON-Schema
        validator = EventValidator(event)
        if not validator.validate():
            raise ValueError(f"Invalid event: {validator.validation_error}")

        run_id, schema_id, sha = event.run_id, event.header.schema_id, event.header.id

        # 2. Skip if artefact already logged for this run
        exists = self._db.execute(
            "SELECT 1 FROM events WHERE run_id=? AND schema=? AND sha=?",
            (run_id, schema_id, sha)
        ).fetchone()
        if exists:
            return False                         # duplicate → caller may ignore

        # 3. Insert blob (dedup on sha) + new event row
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO blobs (sha, bytes) VALUES (?,?)",
                (sha, event.blob),
            )
            seq = self._db.execute(
                "SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
            self._db.execute(
                "INSERT INTO events(run_id, seq, sha, schema, ts) "
                "VALUES(?, ?, ?, ?, strftime('%s','now')*1000)",
                (run_id, seq, sha, schema_id),
            )

        # 4. Notify subscribers about the new row
        asyncio.create_task(self._notify())
        return True

    
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