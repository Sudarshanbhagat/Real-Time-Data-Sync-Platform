#!/bin/sh
# Container entrypoint: prepare config from the environment, migrate, then exec CMD.
set -e

# Managed Kafka (e.g. Aiven) authenticates with client certificates. Platforms
# store secrets as env vars, so accept the PEM *contents* and write them to files
# that aiokafka can load. Paths already set via KAFKA_SSL_*FILE win.
CERT_DIR="${KAFKA_CERT_DIR:-/tmp/kafka-certs}"
write_pem() {  # $1 = env var holding PEM text, $2 = file name, $3 = path env var to set
    value=$(printenv "$1" || true)
    if [ -n "$value" ] && [ -z "$(printenv "$3" || true)" ]; then
        mkdir -p "$CERT_DIR"
        printf '%s\n' "$value" > "$CERT_DIR/$2"
        chmod 600 "$CERT_DIR/$2"
        export "$3=$CERT_DIR/$2"
    fi
}
write_pem KAFKA_SSL_CA_PEM ca.pem KAFKA_SSL_CAFILE
write_pem KAFKA_SSL_CERT_PEM service.cert KAFKA_SSL_CERTFILE
write_pem KAFKA_SSL_KEY_PEM service.key KAFKA_SSL_KEYFILE

# Render's Postgres gives postgres:// (or postgresql://) URLs; the app needs the
# asyncpg driver in the scheme.
case "${DATABASE_URL:-}" in
    postgres://*) DATABASE_URL="postgresql+asyncpg://${DATABASE_URL#postgres://}" ;;
    postgresql://*) DATABASE_URL="postgresql+asyncpg://${DATABASE_URL#postgresql://}" ;;
esac
export DATABASE_URL

# Create/upgrade the schema (idempotent). Set RUN_MIGRATIONS=false to skip.
if [ "${RUN_MIGRATIONS:-true}" = "true" ]; then
    python scripts/setup_db.py
fi

# Create the Kafka topic if it doesn't exist (managed clusters often disable
# auto-create). Non-fatal: the app also starts while Kafka is unreachable.
if [ "${CREATE_KAFKA_TOPIC:-true}" = "true" ]; then
    python scripts/create_topic.py || echo "WARNING: could not ensure Kafka topic; continuing"
fi

exec "$@"
