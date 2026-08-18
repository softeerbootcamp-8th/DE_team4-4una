---
status: accepted
date: 2026-08-18
supersedes:
superseded_by:
---

# 0003. Gold 발행과 서빙 DB 마이그레이션은 batch-jobs가 소유한다

## 배경

목표 아키텍처(`context/architecture.md`)는 Gold 점수 발행(계산 결과를 서빙
DB에 적재)을 별도 서비스 `services/gold-loader`에 할당했다. 그러나 실제
구현은 이 로직(`load-segment-comfort-score`, `migrate-database` 커맨드와
Spark→JDBC MERGE 적재 로직)이 처음부터 `services/batch-jobs`에 들어 있고,
`gold-loader`는 `Dockerfile`/`pyproject.toml`/빈 `__main__.py`뿐인
스켈레톤 상태로 방치돼 있다. 이 불일치는 `context/open-questions.md`의
OQ-040으로 미해결 기록돼 있었다.

`hourly_pipeline` DAG의 4번째(publish) TaskGroup 이슈를 발행하려는 시점에
이 결정을 계속 미루면, 이슈·문서마다 "지금 어디 있는 코드를 부를지"를
매번 설명해야 하는 비용이 반복된다.

## 결정

Gold 점수 계산·발행과 서빙 DB 마이그레이션은 **`services/batch-jobs`가
계속 소유**한다. `services/gold-loader`는 삭제한다.

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| 로직을 `gold-loader`로 이관 | 목표 아키텍처대로 연산(batch-jobs)과 적재(gold-loader) 관심사 분리 | 이미 검증된 Spark→JDBC MERGE 로직 이전 작업, `gold-loader`도 Spark 의존성을 다시 갖거나 Parquet 직접 적재로 재설계 필요 | 지금 얻는 관심사 분리 이득 대비 재작업 비용·리스크가 크고, 현재 단계에서 실익이 작다 |
| 결정 보류, OQ-040 계속 open | 당장 결정 비용 없음 | publish 이슈·문서·온보딩마다 불일치를 계속 설명해야 하고, 죽은 스켈레톤 코드가 남는다 | publish 이슈 발행 시점에 정리하는 게 낫다고 판단 |

## 결과

- `services/gold-loader` 삭제, workspace에서 자동 제외됨 (`uv lock` 재생성 필요)
- Gold 발행 관련 신규 요구사항(권한, 배포 등)은 `batch-jobs` 범위에서 다룬다

## 영향 범위

- `services/gold-loader` — 디렉터리 삭제
- `context/architecture.md` — 목표 흐름에서 `gold-loader` 제거, `batch-jobs → 서빙 DB` 직접 흐름으로 갱신
- `context/services.md` — 서비스 표에서 `gold-loader` 행 제거, Ownership questions 항목 정리
- `context/open-questions.md` — OQ-040 상태를 Accepted로 변경
- `uv.lock` — 재생성

## 참고

- 관련 이슈: #157, #169 / 관련 오픈퀘스천: OQ-040
