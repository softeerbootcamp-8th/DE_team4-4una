FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/
WORKDIR /app
COPY . .

RUN uv sync --locked --package dashboard --no-dev --no-editable

CMD ["uv", "run", "--no-sync", "--package", "dashboard", "dashboard"]
