import asyncio, csv, io, random, statistics, textwrap 
from drylab import Ledger, Reactor, Blob, SchemaId, EventHeader, EventRow

SEQ = SchemaId("SEQ_PDB@1")
RMSD = SchemaId("RMSD_CSV@1")
REP = SchemaId("REPORT_MD@1")

class SimReactor(Reactor): 
    pattern = {"schema": SEQ}  # Use schema
    async def handle(self, ev: EventRow) -> list[tuple[SchemaId, Blob]]:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["time", "rmsd"])
        [w.writerow([t, random.random()*4]) for t in range(100)]
        return [(RMSD, Blob(buf.getvalue().encode()))]

class ReportReactor(Reactor): 
    pattern = {"schema": RMSD}  # Use schema
    async def handle(self, ev: EventRow) -> list[tuple[SchemaId, Blob]]:
        rows = list(csv.DictReader(io.StringIO(ev.blob.decode())))
        mean = statistics.mean(float(r["rmsd"]) for r in rows)
        md = textwrap.dedent(f"""
            # Folding report
            mean RMSD: {mean:.3f}
            (src sha: {ev.header.schema_id})  # Changed from schema to schema_id
        """
        ).encode()
        return [(REP, Blob(md))]

async def main():
    l = Ledger("demo.db")
    asyncio.create_task(SimReactor(l).run("run1"))
    asyncio.create_task(ReportReactor(l).run("run1"))
    
    # Create and publish the initial event
    header = EventHeader(
        id=l._hash(Blob(b"FAKEPDB")),
        schema=SEQ  # Use schema
    )
    initial_event = EventRow(
        header=header,
        blob=Blob(b"FAKEPDB"),
        run_id="run1",
        seq=0
    )
    l.publish(initial_event)
    
    await asyncio.sleep(1)
    [print(ev.header.schema, ev.header.id) for ev in l.replay("run1")]  # Use schema
    
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())