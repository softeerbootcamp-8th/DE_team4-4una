#!/usr/bin/env bash
# EC2에서 실행되는 배포 스크립트. 워크플로가 SSH로 접속해, 아래 변수 대입문을 앞에
# 붙인 이 파일 내용을 stdin으로 넘긴다.
#
#   IMAGE REGISTRY AWS_REGION CONTAINER PORT REVISION HEALTH_PATH HEALTH_TIMEOUT
#   ROAD_SEGMENT_S3_URI ZONE_MASTER_S3_URI SERVING_API_URL
#
# 비밀값은 다루지 않는다. S3 접근은 인스턴스 role로 하고, 나머지 설정은 비밀이 아닌
# S3 URI와 내부 주소뿐이라 repository variables에서 그대로 내려온다.

set -euo pipefail

log() { printf '[deploy] %s\n' "$*"; }

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

  # 호스트 네트워크를 쓴다. Serving API가 같은 EC2에 8000으로 떠 있는데, 기본 브리지
  # 네트워크에서는 localhost가 이 컨테이너 자신을 가리켜 못 붙는다. stream-processor가
  # Kafka에 붙는 것과 같은 이유다.
  #
  # 포트는 publish하지 않는다. dashboard/__init__.py가 0.0.0.0:8501에 고정으로
  # 바인딩하므로 호스트 네트워크에서는 그대로 호스트의 8501이 된다.
  #
  # AWS 자격증명은 넘기지 않는다. 컨테이너가 인스턴스 role로 S3를 읽는다.
  docker run --detach \
    --name "$CONTAINER" \
    --network host \
    --restart unless-stopped \
    --env "AWS_REGION=${AWS_REGION}" \
    --env "AWS_DEFAULT_REGION=${AWS_REGION}" \
    --env "DASHBOARD_ROAD_SEGMENT_S3_URI=${ROAD_SEGMENT_S3_URI}" \
    --env "DASHBOARD_ZONE_MASTER_S3_URI=${ZONE_MASTER_S3_URI}" \
    --env "DASHBOARD_SERVING_API_URL=${SERVING_API_URL}" \
    --label "org.opencontainers.image.revision=${revision}" \
    "$image"
}

# Streamlit이 제공하는 health 엔드포인트다. 서버가 스크립트를 받을 준비가 되면 ok를
# 준다. 앱이 S3나 Serving API에 못 닿는 것까지는 보지 않는다 — 그 둘은 사용자가 화면을
# 열 때 처음 접근하므로 기동 시점에는 판정할 수 없다.
wait_for_health() {
  local interval=3
  local remaining=$((HEALTH_TIMEOUT / interval))
  local body=""

  while [ "$remaining" -gt 0 ]; do
    remaining=$((remaining - 1))
    if body="$(curl --fail --silent --show-error \
      "http://127.0.0.1:${PORT}${HEALTH_PATH}" 2>/dev/null)"; then
      log "health 통과: $body"
      return 0
    fi
    sleep "$interval"
  done

  log "health 실패 (${HEALTH_TIMEOUT}초 초과). 마지막 응답: ${body:-(응답 없음)}"
  docker logs --tail 50 "$CONTAINER" 2>&1 || true
  return 1
}

# 현재 이미지와 rollback용 직전 이미지만 남기고 이 리포지터리의 나머지 태그를 지운다.
#
# `docker image prune`은 dangling(태그 없는) 이미지만 지우므로 <repo>:<sha> 태그가
# 붙은 이전 배포 이미지를 정리하지 못한다. 반대로 `docker image prune -af`는 쓰면
# 안 된다 — 이 EC2에는 Kafka, Airflow, exporter 등 다른 서비스 이미지가 함께 있다.
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
