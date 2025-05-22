place your json‑schema files here. for the mvp we include three minimal schemas:

`drylab/schemas/SEQ_PDB.v1.json`

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Sequence PDB",
  "type": "string",
  "description": "FAKE PDB sequence blob encoded as utf-8 string",
  "payload_encoding": "utf-8"
}
```

`drylab/schemas/RMSD_CSV.v1.json`

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "RMSD CSV",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "time": { "type": "number" },
      "rmsd": { "type": "number" }
    },
    "required": ["time", "rmsd"]
  },
  "payload_encoding": "utf-8"
}
```

`drylab/schemas/REPORT_MD.v1.json`

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Report Markdown",
  "type": "string",
  "description": "Markdown report string including references to event shas",
  "payload_encoding": "utf-8"
}
```
