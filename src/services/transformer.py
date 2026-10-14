"""Source-to-canonical data transformation.

Every source system names fields differently (`account_name` in Salesforce,
`firstname` in HubSpot, `customer_name` in Stripe). The platform stores one
canonical shape so each destination only needs one mapping: canonical -> itself.

Canonical record (schema_version 2):

    entity_id  str, required
    name       str, required, non-empty
    email      str | None (validated if present)
    status     str | None
    metadata   dict: every source field that was not mapped (keys snake_cased)

Source keys are matched after snake_case normalisation, so `AccountName`,
`account_name` and `Account Name` are the same field. The `custom` system is
passed through as-is: its keys must already be canonical.
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.observability.tracing import get_tracer
from src.services.validator import TransformationError, validate_canonical

CANONICAL_SCHEMA_VERSION = 2

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")

# A candidate source for a canonical field: one key, or several keys joined with
# spaces (e.g. first + last name).
Candidate = str | tuple[str, ...]


@dataclass(frozen=True)
class FieldMapping:
    """Where one source system keeps each canonical field, in priority order."""

    name: tuple[Candidate, ...]
    email: tuple[Candidate, ...]
    status: tuple[Candidate, ...]


MAPPINGS: dict[str, FieldMapping] = {
    "salesforce": FieldMapping(
        name=("account_name", "name", ("first_name", "last_name")),
        email=("email",),
        status=("account_status", "status"),
    ),
    "hubspot": FieldMapping(
        name=(("firstname", "lastname"), "name"),
        email=("email",),
        status=("hs_lead_status",),
    ),
    "stripe": FieldMapping(
        name=("customer_name", "name"),
        email=("email",),
        status=("status",),
    ),
}

PASSTHROUGH_SYSTEM = "custom"
SUPPORTED_SYSTEMS = (*MAPPINGS, PASSTHROUGH_SYSTEM)
_CANONICAL_FIELDS = ("name", "email", "status")


def to_snake_case(key: str) -> str:
    """Convert `FirstName`, `first-name` or `First Name` to `first_name`."""
    key = _CAMEL_BOUNDARY.sub("_", key)
    return _NON_ALNUM.sub("_", key).strip("_").lower()


def _normalise(value: Any) -> Any:
    """Recursively snake_case the keys of nested dicts/lists."""
    if isinstance(value, dict):
        return {to_snake_case(str(k)): _normalise(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalise(v) for v in value]
    return value


def _clean(value: Any) -> Any:
    """Strip surrounding whitespace from strings; leave other values for validation."""
    return value.strip() if isinstance(value, str) else value


def _describe(candidate: Candidate) -> str:
    """Human-readable form of a candidate, for error messages."""
    return " + ".join(candidate) if isinstance(candidate, tuple) else candidate


def _resolve(
    fields: dict[str, Any], candidates: tuple[Candidate, ...], used: set[str]
) -> tuple[bool, Any]:
    """Return `(found, value)` from the first candidate present in `fields`.

    A candidate counts as present if its key exists, even with a null value, so
    an explicit null is reported as null rather than as missing. Keys consumed
    are added to `used` so they don't also land in `metadata`.
    """
    for candidate in candidates:
        keys = candidate if isinstance(candidate, tuple) else (candidate,)
        present = [key for key in keys if key in fields]
        if not present:
            continue
        used.update(present)
        if len(keys) == 1:
            return True, _clean(fields[keys[0]])
        parts = [fields[key] for key in present if fields[key] is not None]
        if not parts:
            return True, None
        if not all(isinstance(part, str) for part in parts):
            # Surface the offending value so the validator reports its type.
            return True, next(part for part in parts if not isinstance(part, str))
        return True, " ".join(part.strip() for part in parts if part.strip())
    return False, None


def _map_fields(source_system: str, data: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Apply the system's field mapping. Returns (partial record, name hint)."""
    record: dict[str, Any]
    if source_system == PASSTHROUGH_SYSTEM:
        record = {field: _clean(data[field]) for field in _CANONICAL_FIELDS if field in data}
        extra = {k: v for k, v in data.items() if k not in (*_CANONICAL_FIELDS, "metadata")}
        metadata = data.get("metadata", {})
        record["metadata"] = {**metadata, **extra} if isinstance(metadata, dict) else metadata
        return record, " (custom records must contain 'name')"

    mapping = MAPPINGS[source_system]
    fields = {to_snake_case(str(key)): value for key, value in data.items()}
    used: set[str] = set()
    record = {}
    for field in _CANONICAL_FIELDS:
        found, value = _resolve(fields, getattr(mapping, field), used)
        if found:
            record[field] = value
    record["metadata"] = {k: _normalise(v) for k, v in fields.items() if k not in used}
    expected = ", ".join(_describe(c) for c in mapping.name)
    return record, f" ({source_system} expects one of: {expected})"


def transform(source_data: dict[str, Any], source_system: str, entity_id: str) -> dict[str, Any]:
    """Map a raw source payload to a validated canonical record.

    Raises `TransformationError` listing every problem if the source system is
    unsupported or the mapped record fails validation.
    """
    if source_system not in SUPPORTED_SYSTEMS:
        supported = ", ".join(SUPPORTED_SYSTEMS)
        raise TransformationError(
            [f"unsupported source system '{source_system}' (supported: {supported})"]
        )
    if not isinstance(source_data, dict):
        raise TransformationError(
            [f"source data must be an object, got {type(source_data).__name__}"]
        )

    record, name_hint = _map_fields(source_system, source_data)
    canonical: dict[str, Any] = {
        "schema_version": CANONICAL_SCHEMA_VERSION,
        "entity_id": entity_id,
        **record,
        "source_system": source_system,
        "transformed_at": datetime.now(UTC).isoformat(),
    }
    with get_tracer().start_as_current_span("validate"):
        validate_canonical(canonical, name_hint=name_hint)
    canonical.setdefault("email", None)
    canonical.setdefault("status", None)
    return canonical
