import importlib.resources as _r
import json
from typing import Dict
import jsonschema
from .types import SchemaId, Blob, Event

_SCHEMA_CACHE: Dict[SchemaId, dict] = {} 
# cache to store loaded schemas. two "arguments", the type of its keys and the type 
# of its values. this one maps schema IDs to their JSON representations (dicts in python)


class UnknownSchemaError(RuntimeError): # custom error for when a schema isn't found
    pass # Similar to how TypeScript would throw an error for missing type definitions


def load_schema(schema_id: SchemaId) -> dict:
    if schema_id in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[schema_id]

    name, _, version = schema_id.partition("@")
    fname = f"{name}.v{version or '1'}.json"
    try:
        with _r.files("drylab.schemas").joinpath(fname).open("rb") as fp:
            schema = json.load(fp)
    except FileNotFoundError as exc:
        raise UnknownSchemaError(schema_id) from exc

    _SCHEMA_CACHE[schema_id] = schema
    return schema


def validate_schema(schema_id: SchemaId, blob: Blob) -> None:
    schema = load_schema(schema_id)
    data = blob
    if schema.get("payload_encoding") == "utf-8":
        data = blob.decode("utf-8")
    jsonschema.validate(data, schema)

# def validate_event(event: Event) -> None:
#     validate_schema(event.header.schema_id, event.blob)