FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/
WORKDIR /app
COPY . .

RUN uv sync --locked --package sensor-producer --no-dev --no-editable --compile-bytecode

FROM python:3.12-slim

RUN groupadd --gid 10001 sensor && \
    useradd --uid 10001 --gid sensor --create-home sensor

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY --from=builder --chown=sensor:sensor /app/.venv /app/.venv

USER sensor
ENTRYPOINT ["sensor-producer"]
CMD ["--help"]
