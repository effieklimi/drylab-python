import asyncio
from typing import Dict, List, Tuple, Union
from .types import EventRow, EventHeader, SchemaId, Blob
from .ledger import Ledger


# | Step                                                                              | Term we use        | What really happens                    |
# | --------------------------------------------------------------------------------- | ------------------ | -------------------------------------- |
# | ① It **waits for** an event whose header matches its `pattern`.                  | “subscribe”        | `ledger.subscribe()` yields the event. |
# | ② It **processes** the event.                                                    | “handle”           | Your `handle()` coroutine runs.        |
# | ③ It **produces → publishes** new artefacts that downstream reactors might need. | “emit” / “publish” | You call `self.ledger.publish(...)`.   |


Pattern = Dict[str,Union[str,int]]

class Reactor:
    pattern: Pattern = {}

    def __init__(self, ledger: Ledger, *, activity_event: asyncio.Event | None = None):
        self.ledger = ledger
        self._activity_event = activity_event
            
    async def handle(self, ev: EventRow) -> List[Tuple[SchemaId, Blob]]: 
        raise NotImplementedError

    async def run(self, run_id: str):
        async for row in self.ledger.subscribe(run_id):
            if self._match(row.header):
                outputs = await self.handle(row)
                for schema, blob in outputs or []:
                    header = EventHeader(id=self.ledger._hash(blob), schema_id=schema)
                    output_event = EventRow(
                        header=header,
                        blob=blob,
                        run_id=run_id,
                        seq=0,
                    )
                    self.ledger.publish(output_event)
        # Generator exhausted → nothing more to do
        if self._activity_event:
            self._activity_event.set()  

    # helpers
    def _match(self, header: EventHeader) -> bool:
        for k,v in self.pattern.items():
            if getattr(header, k) != v: return False
        return True