FROM python:3.12-slim

# pyspark 4.x는 JVM(Java 17+)이 필요하다. python:3.12-slim은 기본적으로 Java가 없어
# 이게 빠지면 컨테이너 안에서 SparkSession 생성 시 JAVA_GATEWAY_EXITED로 실패한다.
# python:3.12-slim 베이스(Debian trixie)에는 openjdk-17 패키지가 없어 21을 쓴다.
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/
WORKDIR /app
COPY . .

RUN uv sync --locked --package batch-jobs --no-dev --no-editable

CMD ["uv", "run", "--no-sync", "--package", "batch-jobs", "batch-jobs"]
