"""Validation rules for canonical records.

Every rule violation is collected (not just the first), so a failed sync's
`error_message` tells an operator everything that is wrong with the record.
"""

import re
from typing import Any

# Deliberately simple: catches obvious garbage without rejecting valid but unusual
# addresses. Deliverability is the destination system's concern.
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

MAX_NAME_LENGTH = 255
MAX_EMAIL_LENGTH = 320
MAX_STATUS_LENGTH = 50


class TransformationError(ValueError):
    """A source record could not be turned into a valid canonical record.

    `errors` lists every problem found; `str(exc)` joins them into one message.
    """

    def __init__(self, errors: list[str]) -> None:
        """Store the individual problems and build a readable summary."""
        self.errors = errors
        super().__init__("; ".join(errors))


def _check_string(
    record: dict[str, Any], field: str, *, required: bool, max_length: int, hint: str = ""
) -> list[str]:
    """Rules shared by the canonical string fields."""
    if field not in record:
        return [f"'{field}' is missing{hint}"] if required else []
    value = record[field]
    if value is None:
        return [f"'{field}' is required but was null"] if required else []
    if not isinstance(value, str):
        return [f"'{field}' must be a string, got {type(value).__name__}"]
    if required and not value.strip():
        return [f"'{field}' must not be empty"]
    if len(value) > max_length:
        return [f"'{field}' is longer than {max_length} characters"]
    return []


def validate_canonical(record: dict[str, Any], name_hint: str = "") -> None:
    """Raise `TransformationError` unless `record` satisfies the canonical schema.

    Schema: `entity_id` and `name` are required non-empty strings; `email` and
    `status` are optional (null allowed) strings, `email` must look like an
    address; `metadata` must be an object. `name_hint` is appended to the
    "'name' is missing" error to say which source fields were looked for.
    """
    errors = _check_string(record, "entity_id", required=True, max_length=255)
    errors += _check_string(
        record, "name", required=True, max_length=MAX_NAME_LENGTH, hint=name_hint
    )
    errors += _check_string(record, "email", required=False, max_length=MAX_EMAIL_LENGTH)
    errors += _check_string(record, "status", required=False, max_length=MAX_STATUS_LENGTH)

    email = record.get("email")
    if isinstance(email, str) and not _EMAIL.match(email):
        errors.append(f"'email' is not a valid email address: {email!r}")

    if not isinstance(record.get("metadata"), dict):
        errors.append("'metadata' must be an object")

    if errors:
        raise TransformationError(errors)
