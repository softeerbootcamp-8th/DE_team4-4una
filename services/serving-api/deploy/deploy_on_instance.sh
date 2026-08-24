#!/usr/bin/env bash
# EC2에서 실행되는 배포 스크립트. 워크플로가 SSH로 접속해, 아래 변수 대입문을 앞에
# 붙인 이 파일 내용을 stdin으로 넘긴다.
#
#   IMAGE REGISTRY AWS_REGION CONTAINER HOST_PORT METRICS_PORT ENV_FILE REVISION
#   HEALTH_PATH HEALTH_TIMEOUT
#
# 비밀값은 다루지 않는다. DB 접속 정보는 인스턴스의 ENV_FILE에만 있고 경로만 넘긴다.

set -euo pipefail

log() { printf '[deploy] %s\n' "$*"; }

if [ ! -f "$ENV_FILE" ]; then
  log "env 파일이 없습니다: $ENV_FILE (docs/deploy-serving-api.md 참고)"
  exit 1
fi

# 실패 시 되돌릴 대상. 첫 배포면 빈 값이고, 그때는 되돌릴 상태가 없어 그냥 실패한다.
PREVIOUS_IMAGE="$(docker inspect --format '{{.Config.Image}}' "$CONTAINER" 2>/dev/null || true)"
log "이전 이미지: ${PREVIOUS_IMAGE:-(없음)}"

aws ecr get-login-password --region "$AWS_REGION" |
  docker login --username AWS --password-stdin "$REGISTRY"

log "pull: $IMAGE"
docker pull "$IMAGE"

start_container() {
  local image="$1"
  local revision="$2"

  docker rm --force "$CONTAINER" >/dev/null 2>&1 || true

  # --env가 --env-file보다 뒤라 SERVING_API_PORT/SERVING_API_METRICS_PORT는 이 값이
  # 이긴다. 앱이 듣는 포트와 publish 대상이 어긋나지 않게 못박는다.
  docker run --detach \
    --name "$CONTAINER" \
    --restart unless-stopped \
    --env-file "$ENV_FILE" \
    --env "SERVING_API_PORT=${HOST_PORT}" \
    --env "SERVING_API_METRICS_PORT=${METRICS_PORT}" \
    --publish "${HOST_PORT}:${HOST_PORT}" \
    --publish "${METRICS_PORT}:${METRICS_PORT}" \
    --label "org.opencontainers.image.revision=${revision}" \
    "$image"
}

# /health는 DB에 못 닿으면 503을 준다. curl --fail 성공만으로 앱과 DB를 함께 본다.
# database 값을 한 번 더 확인하는 건 200에 degraded 본문을 주는 변경까지 잡기 위함이다.
wait_for_health() {
  local interval=3
  local remaining=$((HEALTH_TIMEOUT / interval))
  local body=""

  while [ "$remaining" -gt 0 ]; do
    remaining=$((remaining - 1))
    if body="$(curl --fail --silent --show-error \
      "http://127.0.0.1:${HOST_PORT}${HEALTH_PATH}" 2>/dev/null)"; then
      if printf '%s' "$body" | grep -q '"database"[[:space:]]*:[[:space:]]*"ok"'; then
        log "health 통과: $body"
        return 0
      fi
    fi
    sleep "$interval"
  done

  log "health 실패 (${HEALTH_TIMEOUT}초 초과). 마지막 응답: ${body:-(응답 없음)}"
  docker logs --tail 50 "$CONTAINER" 2>&1 || true
  return 1
}

# 현재 이미지와 rollback용 직전 이미지만 남기고 이 리포지터리의 나머지 태그를 지운다.
#
# 전에는 `docker image prune --force`만 했는데, 그것은 dangling(태그 없는) 이미지만
# 지우므로 <repo>:<sha> 태그가 붙은 이전 배포 이미지가 계속 쌓였다. 반대로
# `docker image prune -af`는 쓰면 안 된다 — 이 EC2에는 Kafka, Airflow, exporter 등
# 다른 서비스 이미지가 함께 있다.
prune_old_images() {
  local keep_current="$1"
  local keep_previous="$2"
  local repo="${keep_current%:*}"
  local ref

  while IFS= read -r ref; do
    [ -n "$ref" ] || continue
    [ "$ref" = "$keep_current" ] && continue
    [ -n "$keep_previous" ] && [ "$ref" = "$keep_previous" ] && continue

    if docker rmi "$ref" >/dev/null 2>&1; then
      log "이미지 삭제: $ref"
    else
      # 다른 컨테이너가 쓰고 있으면 실패한다. 정리는 부가 작업이라 배포를 깨지 않는다.
      log "이미지 삭제 실패(사용 중일 수 있음): $ref"
    fi
  done < <(docker images --filter "reference=${repo}:*" --format '{{.Repository}}:{{.Tag}}')
}

log "기동: $IMAGE (revision ${REVISION})"
start_container "$IMAGE" "$REVISION"

if wait_for_health; then
  log "배포 성공: $IMAGE"
  prune_old_images "$IMAGE" "$PREVIOUS_IMAGE"
  exit 0
fi

if [ -z "$PREVIOUS_IMAGE" ]; then
  log "되돌릴 이전 이미지가 없습니다 (첫 배포)."
  exit 1
fi

log "rollback: $PREVIOUS_IMAGE"
PREVIOUS_REVISION="$(
  docker inspect \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
    "$PREVIOUS_IMAGE" 2>/dev/null || echo unknown
)"
start_container "$PREVIOUS_IMAGE" "$PREVIOUS_REVISION"

if wait_for_health; then
  log "rollback 후 정상. 배포는 실패로 처리한다."
else
  log "rollback 후에도 health 실패. 수동 확인이 필요하다."
fi

exit 1
