# 1. React 빌드
FROM node:22-alpine AS frontend

WORKDIR /frontend

# 매니페스트만 먼저 복사해, 소스만 바뀐 빌드에서는 의존성 설치 레이어를 재사용한다.
COPY services/dashboard/frontend/package*.json ./

RUN npm ci

COPY services/dashboard/frontend/ ./
RUN npm run build


# 2. Python 런타임
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/
WORKDIR /app
COPY . .

RUN uv sync --locked --package dashboard --no-dev --no-editable \
    && rm -rf /root/.cache/uv

# 패키지가 site-packages에 설치되므로 소스 트리 기준 경로로는 찾을 수 없다.
# api.py가 이 환경변수로 정적 파일 위치를 찾는다.
COPY --from=frontend /frontend/dist /app/frontend-dist
ENV DASHBOARD_STATIC_DIR=/app/frontend-dist

EXPOSE 8501

CMD ["uv", "run", "--no-sync", "--package", "dashboard", "dashboard"]
