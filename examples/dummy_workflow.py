import asyncio,csv,io,random,statistics,textwrap
from drylab import Ledger,Reactor,Blob,SchemaId

SEQ=SchemaId("SEQ_PDB@1")
RMSD=SchemaId("RMSD_CSV@1")
REP=SchemaId("REPORT_MD@1")
class SimReactor(Reactor): 
    pattern={"schema":SEQ}
    async def handle(self,ev):
        buf=io.StringIO();w=csv.writer(buf);w.writerow(["time","rmsd"]);
        [w.writerow([t,random.random()*4]) for t in range(100)]
        return [(RMSD,Blob(buf.getvalue().encode()))]

class ReportReactor(Reactor): 
    pattern={"schema":RMSD}
    async def handle(self,ev):
        rows=list(csv.DictReader(io.StringIO(ev.blob.decode())))
        mean=statistics.mean(float(r["rmsd"]) for r in rows)
        md=textwrap.dedent(f"""
            # Folding report
            mean RMSD: {mean:.3f}
            (src sha: {ev.header.schema})
        """
        ).encode()
        return [(REP,Blob(md))]

async def main():
    l=Ledger("demo.db")
    asyncio.create_task(SimReactor(l).run("run1"))
    asyncio.create_task(ReportReactor(l).run("run1"))
    l.publish(run_id="run1",schema=SEQ,blob=Blob(b"FAKEPDB"))
    await asyncio.sleep(1)
    [print(ev.header.schema,ev.header.id) for ev in l.replay("run1")]
if __name__=="__main__":import asyncio;asyncio.run(main())