"""Kafka connection options for local vs managed (SSL / SASL) clusters."""

import ssl
import subprocess
from pathlib import Path

import pytest

from src.config import Settings


def test_plaintext_by_default() -> None:
    """Local docker-compose: no TLS, no SASL."""
    options = Settings(kafka_bootstrap_servers="localhost:9092").kafka_client_options()

    assert options == {"bootstrap_servers": "localhost:9092", "security_protocol": "PLAINTEXT"}


def test_sasl_ssl_sets_credentials_and_tls() -> None:
    """SASL_SSL (e.g. Confluent/Aiven SASL): credentials plus a default-trust TLS context."""
    options = Settings(
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="SCRAM-SHA-256",
        kafka_sasl_username="avnadmin",
        kafka_sasl_password="secret",
    ).kafka_client_options()

    assert options["security_protocol"] == "SASL_SSL"
    assert options["sasl_mechanism"] == "SCRAM-SHA-256"
    assert options["sasl_plain_username"] == "avnadmin"
    assert options["sasl_plain_password"] == "secret"
    assert isinstance(options["ssl_context"], ssl.SSLContext)


def test_ssl_client_certificates_are_loaded(tmp_path: Path) -> None:
    """SSL with client certs (Aiven's default): files are loaded into the context."""
    key, cert = tmp_path / "service.key", tmp_path / "service.cert"
    try:
        subprocess.run(
            [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", str(key), "-out", str(cert), "-days", "1", "-subj", "/CN=test",
            ],
            check=True,
            capture_output=True,
        )  # fmt: skip
    except (FileNotFoundError, subprocess.CalledProcessError):
        pytest.skip("openssl not available to generate a test certificate")

    options = Settings(
        kafka_security_protocol="SSL",
        kafka_ssl_cafile=str(cert),
        kafka_ssl_certfile=str(cert),
        kafka_ssl_keyfile=str(key),
    ).kafka_client_options()

    assert "sasl_mechanism" not in options
    assert isinstance(options["ssl_context"], ssl.SSLContext)


def test_missing_certificate_file_fails_fast(tmp_path: Path) -> None:
    """A wrong cert path is a startup error, not a silent plaintext fallback."""
    settings = Settings(
        kafka_security_protocol="SSL", kafka_ssl_cafile=str(tmp_path / "missing.pem")
    )

    with pytest.raises((FileNotFoundError, ssl.SSLError)):
        settings.kafka_client_options()
