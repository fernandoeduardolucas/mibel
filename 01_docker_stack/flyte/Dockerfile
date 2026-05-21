FROM ghcr.io/flyteorg/flyte-sandbox:latest

# Increase Helm install timeout from the default 5 min to 30 min.
# Docker Desktop on Windows/Mac allocates fewer resources than a bare-metal Linux host,
# so the flyte-core Helm chart frequently times out at the default value.
RUN grep -rl -- "--timeout" /usr/local/bin/ 2>/dev/null \
    | xargs sed -i 's/--timeout [0-9]*m[0-9]*s/--timeout 30m0s/g' \
    || true

# Helm value overrides: point Flyte artifact storage at the Compose MinIO
# instead of the sandbox's internal MinIO.
COPY flyte-core-overrides.yaml /etc/flyte/state/flyte-core-overrides.yaml
