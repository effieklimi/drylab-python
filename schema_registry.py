import importlib.resources as _resources
import json
from pathlib import Path
from typing import Dict

import jsonschema

_SCHEMA_CACHE: Dict[SchemaId, dict] = {}


class UnknownSchemaError(RuntimeError):
    pass


def load_schema(schema_id: SchemaId) -> dict:
    """Locate and cache a JSON schema stored under drylab.schemas.*"""

    if schema_id in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[schema_id]

    name, _, version = schema_id.partition("@")
    fname = f"{name}.v{version or '1'}.json"
    try:
        with _resources.files("drylab.schemas").joinpath(fname).open("rb") as fp:
            schema = json.load(fp)
    except FileNotFoundError as exc:
        raise UnknownSchemaError(schema_id) from exc

    _SCHEMA_CACHE[schema_id] = schema
    return schema


def validate(schema_id: SchemaId, blob: Blob) -> None:
    schema = load_schema(schema_id)
    data = blob  # let downstream code decide parsing; validator may decode if needed
    if schema.get("payload_encoding") == "utf‑8":
        data = blob.decode("utf‑8")
    jsonschema.validate(data, schema)
