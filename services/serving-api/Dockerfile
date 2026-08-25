FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/
WORKDIR /app
COPY . .

RUN uv sync --locked --package serving-api --no-dev --no-editable \
    && rm -rf /root/.cache/uv

CMD ["uv", "run", "--no-sync", "--package", "serving-api", "serving-api"]
