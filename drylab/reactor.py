import asyncio
from typing import Dict, List, Tuple, Union
from .types import EventRow, EventHeader, SchemaId, Blob
from .ledger import Ledger

Pattern = Dict[str,Union[str,int]]

class Reactor:
    pattern: Pattern = {}

    def __init__(self, ledger: Ledger): 
        self.ledger = ledger
        
    async def handle(self, ev: EventRow) -> List[Tuple[SchemaId, Blob]]: 
        raise NotImplementedError

    async def run(self, run_id: str): 
        cursor = 0
        while True:
            for row in self.ledger.tail(run_id, cursor):
                cursor = row.seq
                if self._match(row.header):
                    outputs = await self.handle(row)
                    for schema, blob in outputs or []:
                        # Create a complete EventRow for each output
                        header = EventHeader(
                            id=self.ledger._hash(blob),
                            schema_id=schema
                        )
                        output_event = EventRow(
                            header=header,
                            blob=blob,
                            run_id=run_id,
                            seq=0  # This will be set by the ledger
                        )
                        self.ledger.publish(output_event)
            await asyncio.sleep(0.2)

    # helpers
    def _match(self, header: EventHeader) -> bool:
        for k,v in self.pattern.items():
            if getattr(header, k) != v: return False
        return True