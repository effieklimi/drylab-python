import asyncio
from inspect import iscoroutinefunction
from typing import Dict, Pattern, Union

from .types import Blob, Event, SchemaId
from .ledger import Ledger

PatternDict = Dict[str, Union[str, int]]


class Reactor:
    pattern: PatternDict = {}

    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    async def handle(self, ev: Event):
        raise NotImplementedError

    # ------------------------------------------------ runner
    async def run(self, run_id: str):
        cursor = 0
        while True:
            for row in self.ledger.tail(run_id, cursor):
                cursor = row.seq
                if self._matches(row.header):
                    outputs = await self.handle(row)
                    for schema, blob in outputs or []:
                        self.ledger.publish(run_id=run_id, schema=schema, blob=blob)
            await asyncio.sleep(0.2)

    # ------------------------------------------------ helpers
    def _matches(self, header: EventHeader) -> bool:
        for key, val in self.pattern.items():
            if getattr(header, key) != val:
                return False
        return True
