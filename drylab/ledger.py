import hashlib
import sqlite3
from pathlib import Path
from typing import Iterator

from .types import Blob, EventHeader, EventRow, SchemaId, Sha256
from .schema_registry import validate

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
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.executescript(_DB_SCHEMA_SQL)
        self._db.commit()

    # ------------------------------------------------ utils
    @staticmethod
    def _hash(blob: Blob) -> Sha256:
        return Sha256(hashlib.sha256(blob).hexdigest())

    # ------------------------------------------------ api
    def publish(self, *, run_id: str, schema: SchemaId, blob: Blob) -> Sha256:
        sha = self._hash(blob)
        validate(schema, blob)
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO blobs (sha, bytes) VALUES (?, ?)",
                (sha, blob),
            )
            seq = self._db.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]
            self._db.execute(
                "INSERT INTO events (run_id, seq, sha, schema, ts) VALUES (?,?,?,?,strftime('%s','now')*1000)",
                (run_id, seq, sha, schema),
            )
        return sha

    def cat(self, sha: Sha256) -> Blob:
        row = self._db.execute("SELECT bytes FROM blobs WHERE sha=?", (sha,)).fetchone()
        if not row:
            raise KeyError(sha)
        return row[0]

    def tail(self, run_id: str, from_seq: int = 0) -> Iterator[EventRow]:
        cursor = from_seq
        while True:
            rows = self._db.execute(
                "SELECT seq, sha, schema, ts FROM events WHERE run_id=? AND seq>? ORDER BY seq",
                (run_id, cursor),
            ).fetchall()
            if not rows:
                break
            for seq, sha, schema, ts in rows:
                header = EventHeader(id=sha, schema=schema, ts=ts)
                blob = self.cat(sha)
                yield EventRow(header=header, blob=blob, run_id=run_id, seq=seq)
                cursor = seq

    def replay(self, run_id: str):
        return self.tail(run_id)
