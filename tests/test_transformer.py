"""Transformer and validator tests: per-system mappings, validation errors, edge cases."""

from typing import Any

import pytest

from src.services.transformer import SUPPORTED_SYSTEMS, to_snake_case, transform
from src.services.validator import TransformationError, validate_canonical


def errors_for(data: Any, system: str = "salesforce") -> str:
    """Return the error message `transform` raises for `data`."""
    with pytest.raises(TransformationError) as exc_info:
        transform(data, system, "ent_1")
    return str(exc_info.value)


# --- mappings -------------------------------------------------------------------


def test_salesforce_mapping() -> None:
    """Salesforce: account_name -> name, account_status -> status; rest -> metadata."""
    result = transform(
        {
            "AccountName": "Acme Corp",
            "Account_Status": "Active",
            "Email": "ops@acme.com",
            "Industry": "Manufacturing",
        },
        "salesforce",
        "acct_1",
    )

    assert result["entity_id"] == "acct_1"
    assert result["name"] == "Acme Corp"
    assert result["status"] == "Active"
    assert result["email"] == "ops@acme.com"
    assert result["metadata"] == {"industry": "Manufacturing"}
    assert result["source_system"] == "salesforce"
    assert result["schema_version"] == 2


def test_salesforce_contact_falls_back_to_first_and_last_name() -> None:
    """Salesforce contacts have no account_name: FirstName + LastName become the name."""
    result = transform(
        {"FirstName": "Ada", "LastName": "Lovelace", "Email": "ada@example.com"},
        "salesforce",
        "contact_1",
    )

    assert result["name"] == "Ada Lovelace"
    assert result["status"] is None
    assert result["metadata"] == {}


def test_hubspot_mapping() -> None:
    """HubSpot: firstname (+ lastname) -> name, hs_lead_status -> status."""
    result = transform(
        {
            "firstname": "Grace",
            "lastname": "Hopper",
            "email": "grace@example.com",
            "hs_lead_status": "OPEN",
            "lifecyclestage": "lead",
        },
        "hubspot",
        "hs_1",
    )

    assert result["name"] == "Grace Hopper"
    assert result["status"] == "OPEN"
    assert result["email"] == "grace@example.com"
    assert result["metadata"] == {"lifecyclestage": "lead"}


def test_hubspot_firstname_only() -> None:
    """A missing lastname is fine: the name is just the first name."""
    assert transform({"firstname": "Grace"}, "hubspot", "hs_2")["name"] == "Grace"


def test_stripe_mapping() -> None:
    """Stripe: customer_name -> name, status -> status; nested objects kept in metadata."""
    result = transform(
        {
            "customer_name": "Linus",
            "status": "active",
            "email": "linus@example.com",
            "address": {"PostalCode": "94107"},
        },
        "stripe",
        "cus_1",
    )

    assert result["name"] == "Linus"
    assert result["status"] == "active"
    assert result["metadata"] == {"address": {"postal_code": "94107"}}


def test_custom_passes_through_as_is() -> None:
    """Custom: canonical fields used as-is; other keys kept verbatim (not snake_cased)."""
    result = transform(
        {
            "name": "Internal Thing",
            "email": "it@example.com",
            "status": "live",
            "metadata": {"tier": "gold"},
            "ExtraKey": 1,
        },
        "custom",
        "c_1",
    )

    assert result["name"] == "Internal Thing"
    assert result["status"] == "live"
    assert result["metadata"] == {"tier": "gold", "ExtraKey": 1}


def test_every_supported_system_produces_the_same_shape() -> None:
    """All systems emit identical canonical keys, so destinations need one mapping."""
    samples = {
        "salesforce": {"account_name": "A"},
        "hubspot": {"firstname": "A"},
        "stripe": {"customer_name": "A"},
        "custom": {"name": "A"},
    }
    assert set(samples) == set(SUPPORTED_SYSTEMS)
    shapes = {frozenset(transform(data, system, "e").keys()) for system, data in samples.items()}
    assert shapes == {
        frozenset(
            {
                "schema_version",
                "entity_id",
                "name",
                "email",
                "status",
                "metadata",
                "source_system",
                "transformed_at",
            }
        )
    }


def test_whitespace_is_trimmed() -> None:
    """Surrounding whitespace in mapped strings is removed."""
    result = transform({"customer_name": "  Linus  ", "email": " l@x.io "}, "stripe", "cus")
    assert result["name"] == "Linus"
    assert result["email"] == "l@x.io"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("FirstName", "first_name"),
        ("first-name", "first_name"),
        ("First Name", "first_name"),
        ("hs_lead_status", "hs_lead_status"),
    ],
)
def test_to_snake_case(raw: str, expected: str) -> None:
    """Source keys are normalised before mapping."""
    assert to_snake_case(raw) == expected


# --- validation errors ----------------------------------------------------------


def test_missing_name_explains_expected_fields() -> None:
    """The error says which source fields would have supplied the name."""
    message = errors_for({"Email": "a@b.co"}, "salesforce")
    assert "'name' is missing" in message
    assert "account_name" in message


def test_null_required_field() -> None:
    """An explicit null in a required field is reported as null, not as missing."""
    assert "'name' is required but was null" in errors_for({"customer_name": None}, "stripe")


def test_empty_required_field() -> None:
    """Blank strings don't satisfy a required field."""
    assert "'name' must not be empty" in errors_for({"customer_name": "   "}, "stripe")


def test_wrong_type() -> None:
    """Non-string values in string fields are rejected with the actual type."""
    assert "'name' must be a string, got int" in errors_for({"customer_name": 42}, "stripe")
    assert "'status' must be a string, got bool" in errors_for(
        {"customer_name": "x", "status": True}, "stripe"
    )


def test_invalid_email() -> None:
    """Emails must look like addresses."""
    assert "not a valid email" in errors_for({"customer_name": "x", "email": "nope"}, "stripe")


def test_null_optional_fields_are_allowed() -> None:
    """email and status may be null."""
    result = transform({"customer_name": "x", "email": None, "status": None}, "stripe", "c")
    assert result["email"] is None and result["status"] is None


def test_all_errors_are_reported_together() -> None:
    """Every problem is listed so one failed sync explains everything that's wrong."""
    with pytest.raises(TransformationError) as exc_info:
        transform({"customer_name": 1, "email": "bad", "status": 5}, "stripe", "c")
    assert len(exc_info.value.errors) == 3


def test_unsupported_system() -> None:
    """Unknown source systems are rejected and the supported ones listed."""
    message = errors_for({"name": "x"}, "oracle")
    assert "unsupported source system 'oracle'" in message
    assert "salesforce" in message


def test_non_dict_source_data() -> None:
    """Corrupt rows (source_data not an object) fail cleanly."""
    assert "must be an object" in errors_for(["not", "a", "dict"])


def test_custom_requires_canonical_fields() -> None:
    """Custom records aren't remapped, so they must already contain 'name'."""
    assert "custom records must contain 'name'" in errors_for({"title": "x"}, "custom")


def test_custom_metadata_must_be_an_object() -> None:
    """A custom record with non-object metadata fails validation."""
    assert "'metadata' must be an object" in errors_for({"name": "x", "metadata": 3}, "custom")


def test_validate_canonical_accepts_valid_record() -> None:
    """The validator can be used on its own (e.g. by the API in a later phase)."""
    validate_canonical(
        {"entity_id": "e", "name": "n", "email": None, "status": None, "metadata": {}}
    )
