#!/usr/bin/env bash
# EC2에서 SSM(AWS-RunShellScript)으로 실행되는 배포 스크립트.
# 워크플로가 아래 변수 대입문을 앞에 붙여 이 파일과 함께 하나의 스크립트로 보낸다.
#
#   IMAGE REGISTRY AWS_REGION CONTAINER HOST_PORT ENV_FILE REVISION
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

  # --env가 --env-file보다 뒤라 SERVING_API_PORT는 이 값이 이긴다. 앱이 듣는 포트와
  # publish 대상이 어긋나지 않게 못박는다.
  docker run --detach \
    --name "$CONTAINER" \
    --restart unless-stopped \
    --env-file "$ENV_FILE" \
    --env "SERVING_API_PORT=${HOST_PORT}" \
    --publish "${HOST_PORT}:${HOST_PORT}" \
    --label "org.opencontainers.image.revision=${revision}" \
    "$image"
}

# /health는 DB에 못 닿으면 503을 준다. curl --fail 성공만으로 앱과 DB를 함께 본다.
# database 값을 한 번 더 확인하는 건 200에 degraded 본문을 주는 변경까지 잡기 위함이다.
# SSM이 어떤 셸로 실행할지 보장되지 않아 bash 전용 문법($SECONDS)은 쓰지 않는다.
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

log "기동: $IMAGE (revision ${REVISION})"
start_container "$IMAGE" "$REVISION"

if wait_for_health; then
  log "배포 성공: $IMAGE"
  docker image prune --force >/dev/null 2>&1 || true
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
