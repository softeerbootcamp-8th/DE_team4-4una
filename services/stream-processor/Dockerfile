FROM python:3.12-slim

# PySpark 실행에 필요한 JVM
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/
WORKDIR /app
COPY . .

RUN uv sync --locked --package stream-processor --no-dev --no-editable

CMD ["uv", "run", "--no-sync", "--package", "stream-processor", "stream-processor"]
