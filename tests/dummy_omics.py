import io, csv, statistics, textwrap, asyncio, pandas as pd
from drylab import Pipeline, Reactor, SchemaId, Blob, EventRow, EventHeader
from drylab.llms.gemini import GoogleGemini

FASTQ   = SchemaId("FASTQ_RAW@1")
COUNTS  = SchemaId("COUNTS_MATRIX@1")
DEGS    = SchemaId("DEG_TABLE@1")
ENRICH  = SchemaId("ENRICH_TABLE@1")
REPORT  = SchemaId("OMICS_REPORT_MD@1")

# ------------------------------------------------------------------ #
class KallistoReactor(Reactor):
    pattern = {"schema": FASTQ}

    async def handle(self, ev: EventRow):
        # pretend alignment → counts; in reality call kallisto + convert to CSV
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["gene", "sample1", "sample2"])
        w.writerow(["TP53", 42, 17])
        w.writerow(["ACTA2", 500, 250])
        print("KallistoReactor before publish: ", buf.getvalue())
        return [(COUNTS, Blob(buf.getvalue().encode()))]

# ------------------------------------------------------------------ #
class DiffExprReactor(Reactor):
    pattern = {"schema": COUNTS}

    async def handle(self, ev: EventRow):
        counts = pd.read_csv(io.StringIO(ev.blob.decode()))
        # fake DE calculation
        degs = counts.head(100)                # demo subset
        buf = io.StringIO(); degs.to_csv(buf, index=False)
        print("DiffExprReactor before publish: ", buf.getvalue())
        return [(DEGS, Blob(buf.getvalue().encode()))]

# ------------------------------------------------------------------ #
class EnrichReactor(Reactor):
    pattern = {"schema": DEGS}

    async def handle(self, ev: EventRow):
        degs = pd.read_csv(io.StringIO(ev.blob.decode()))
        enr = degs[["gene"]].copy()
        enr["Term"] = "Pathway_X"
        enr["Adjusted P-value"] = 0.05
        buf = io.StringIO(); enr.to_csv(buf, index=False)
        print("EnrichReactor before publish: ", buf.getvalue())
        return [(ENRICH, Blob(buf.getvalue().encode()))]

# ------------------------------------------------------------------ #
class LLMReportReactor(Reactor):
    pattern = {"schema": ENRICH}

    def __init__(self, ledger, **kw):
        super().__init__(ledger, **kw)
        self.llm = GoogleGemini(ledger)

    async def handle(self, ev: EventRow):
        enr_tbl = pd.read_csv(io.StringIO(ev.blob.decode()))
        top = enr_tbl[["Term", "Adjusted P-value"]].head(10).to_dict("records")

        summary = await self.llm.chat([
            {"role": "system", "content": "You are a transcriptomics expert."},
            {"role": "user", "content": f"Explain these enriched pathways:\n{top}"}
        ])

        md = textwrap.dedent(f"""
        # Omics Report
        **Top pathways:**  
        {summary}

        _(source sha: {ev.header.id})_
        """).encode()
        print("LLMReportReactor before publish: ", md)
        return [(REPORT, Blob(md))]

# ------------------------------------------------------------------ #
async def main():
    pipe = Pipeline("omics.db")
    pipe.add(KallistoReactor,      run_id="omics-run")
    pipe.add(DiffExprReactor,      run_id="omics-run")
    pipe.add(EnrichReactor,        run_id="omics-run")
    pipe.add(LLMReportReactor,     run_id="omics-run")

    # seed with a dummy FASTQ artifact
    first_blob = Blob(b"@SEQ\nACGT\n+\n####")      # placeholder
    first_id   = pipe.ledger._hash(first_blob)
    header     = EventHeader(id=first_id, schema_id=FASTQ)
    pipe.ledger.publish(EventRow(header=header, blob=first_blob,
                                 run_id="omics-run", seq=0))

    await pipe.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
