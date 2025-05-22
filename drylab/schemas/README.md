place your json‑schema files here, for example:

```
RMSD_CSV.v1.json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "RMSD CSV",
  "payload_encoding": "utf-8",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "time": {"type": "number"},
      "rmsd": {"type": "number"}
    },
    "required": ["time", "rmsd"]
  }
}
```
