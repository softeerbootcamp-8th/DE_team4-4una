UV := uv
COMPOSE := docker compose
COMPOSE_DIR := infra/compose
MIGRATE_CMD ?= uv run --package batch-jobs batch-jobs migrate-database

.PHONY: help sync lock lint test package-jobs build-batch-jobs-image migrate up-kafka up-postgres up-airflow up-monitoring

help:
	@echo "sync                    워크스페이스 의존성 동기화"
	@echo "lock                    단일 uv.lock 갱신"
	@echo "lint                    Ruff 검사"
	@echo "test                    전체 테스트 실행"
	@echo "package-jobs            batch-jobs 배포 패키지 빌드"
	@echo "build-batch-jobs-image  batch-jobs docker 이미지를 git SHA로 태깅해 빌드"
	@echo "                        (hourly_pipeline DAG의 cleanse task가 이 태그를 참조;"
	@echo "                        결과 태그를 .env의 BATCH_JOBS_IMAGE_TAG에 넣어야 함)"
	@echo "up-<component>          infra/compose/<component>.yaml 실행"
	@echo "migrate                 DB 마이그레이션 실행(도구 구성 후 MIGRATE_CMD 지정)"

sync:
	$(UV) sync --all-packages

lock:
	$(UV) lock

lint:
	$(UV) run --all-packages ruff check .

test:
	$(UV) run --all-packages pytest

package-jobs:
	$(UV) build --package batch-jobs --out-dir dist

build-batch-jobs-image:
	$(eval TAG := $(shell git rev-parse --short HEAD))
	docker build -t batch-jobs:$(TAG) -f services/batch-jobs/Dockerfile .
	@echo "빌드됨: batch-jobs:$(TAG)"
	@echo ".env의 BATCH_JOBS_IMAGE_TAG=$(TAG) 로 설정하세요."

up-kafka up-postgres up-airflow up-monitoring:
	@test -f "$(COMPOSE_DIR)/$(@:up-%=%).yaml" || { echo "$(COMPOSE_DIR)/$(@:up-%=%).yaml 파일이 필요합니다."; exit 1; }
	$(COMPOSE) --env-file "$(CURDIR)/.env" -f "$(COMPOSE_DIR)/$(@:up-%=%).yaml" up -d

migrate:
	@test -n "$(MIGRATE_CMD)" || { echo "MIGRATE_CMD를 지정해 주세요."; exit 1; }
	$(MIGRATE_CMD)
