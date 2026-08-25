FROM python:3.12-slim

# remediation.py가 대상 EC2로 접속하는 데 필요한 ssh 클라이언트만 추가한다 —
# 그 외 shell 실행 권한을 넓히지 않는다(policy.py의 allowlist가 유일한 진입점).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/
WORKDIR /app
COPY . .

RUN uv sync --locked --package ops-agent --no-dev --no-editable

CMD ["uv", "run", "--no-sync", "--package", "ops-agent", "ops-agent"]
