-- current_segment_comfort_score_quarantine: run_current_score_job이 UPSERT
-- 직전 GX로 검증해 걸러낸 이상 행을 격리한다 (#251, docs/adr/0008-current-
-- score-row-level-quarantine-and-circuit-breaker.md).
--
-- 메인 테이블과 달리 "현재 상태"가 아니라 append-only 거부 로그다 — 같은
-- (segment_id, vehicle_profile_id)가 여러 실행에 걸쳐 반복 격리될 수 있어
-- 유니크 제약을 걸지 않는다. raw_row/reject_detail을 JSONB로 남겨 규칙이
-- 추가돼도 메인 테이블과 별도로 마이그레이션할 필요가 없게 한다. 재처리/
-- 복구 워크플로는 이 서브이슈 범위 밖이라(#251 "제외 범위") 스키마는 감사·
-- 조회용 최소 요건까지만 만족한다.
CREATE TABLE current_segment_comfort_score_quarantine (
    id BIGSERIAL PRIMARY KEY,
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,
    reject_reason TEXT NOT NULL,
    reject_detail JSONB NOT NULL,
    raw_row JSONB NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX current_segment_comfort_score_quarantine_segment_vehicle_idx
    ON current_segment_comfort_score_quarantine (segment_id, vehicle_profile_id);

CREATE INDEX current_segment_comfort_score_quarantine_calculated_at_idx
    ON current_segment_comfort_score_quarantine (calculated_at);
