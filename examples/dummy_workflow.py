import asyncio, csv, io, random, statistics, textwrap
from drylab import Ledger, Reactor, Blob, SchemaId

SEQ_SCHEMA: SchemaId = "SEQ_PDB@1"
RMSD_SCHEMA: SchemaId = "RMSD_CSV@1"
REPORT_SCHEMA: SchemaId = "REPORT_MD@1"


class SimReactor(Reactor):
    pattern = {"schema": SEQ_SCHEMA}

    async def handle(self, ev):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["time", "rmsd"])
        for t in range(100):
            w.writerow([t, random.random() * 4])
        return [(RMSD_SCHEMA, Blob(buf.getvalue().encode()))]


class ReportReactor(Reactor):
    pattern = {"schema": RMSD_SCHEMA}

    async def handle(self, ev):
        rows = list(csv.DictReader(io.StringIO(ev.blob.decode())))
        mean = statistics.mean(float(r["rmsd"]) for r in rows)
        md = textwrap.dedent(
            f"""
            # Folding run report
            mean RMSD: {mean:.3f}
            (source sha: {ev.header.id})
            """
        ).encode()
        return [(REPORT_SCHEMA, Blob(md))]


async def main():
    ledger = Ledger("demo.db")
    asyncio.create_task(SimReactor(ledger).run("run1"))
    asyncio.create_task(ReportReactor(ledger).run("run1"))
    ledger.publish(run_id="run1", schema=SEQ_SCHEMA, blob=Blob(b"FAKEPDB"))
    await asyncio.sleep(1.0)
    for ev in ledger.replay("run1"):
        print(ev.header.schema, ev.header.id)

if __name__ == "__main__":
    asyncio.run(main())
