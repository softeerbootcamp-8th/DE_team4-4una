-- vehicle_profile_id=0은 "차량 구분 없음" sentinel이다 (OQ-038, accepted
-- 2026-08-16). formula.py::_combined_hourly_score가 두 grain(per-vehicle/
-- vehicle-agnostic) 모두에 동일한 comfort_score.yaml 전역 가중치를 적용하고
-- vehicle-agnostic 경로는 차량별 보정을 하지 않으므로, 이 행의 가중치는
-- 정확히 그 전역 가중치(0.5/0.3/0.2)와 같아야 한다.
--
-- 주의(drift 위험): 이 값은 services/batch-jobs/config/comfort_score.yaml과
-- 별개 파일에 하드코딩된 중복 값이다. comfort_score.yaml의 가중치가
-- 바뀌면 이 파일도 같이 갱신해야 하며 자동 동기화 장치는 없다.
--
-- vehicle_class='ALL'은 실제 차량 등급과 겹치지 않는 sentinel 전용 값이다.
--
-- 이 행은 "샘플 데이터"가 아니라 스키마 무결성의 일부라 db/seeds/가 아닌
-- 마이그레이션에 둔다 — make migrate 한 번으로 스키마와 함께 반드시
-- 적용되고, formula.py의 vehicle-agnostic 행이 FK 위반 없이 들어간다.
INSERT INTO vehicle_profile (
    vehicle_profile_id, profile_name, vehicle_class,
    vertical_weight, longitudinal_weight, lateral_weight,
    is_active, created_at, updated_at
) VALUES (
    0, 'ALL_VEHICLES', 'ALL',
    0.5, 0.3, 0.2,
    TRUE, '2026-08-16T00:00:00Z', '2026-08-16T00:00:00Z'
)
ON CONFLICT (vehicle_profile_id) DO NOTHING;
