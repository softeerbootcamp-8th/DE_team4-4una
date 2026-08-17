-- segment_comfort_score/_staging에 data_period_start/data_period_end를
-- 추가한다 (#163). 새로 계산하는 값이 아니라, comfort_score/formula.py가
-- hourly_comfort_score 입력이 이미 갖고 있는 값을 MIN(data_period_start)/
-- MAX(data_period_end)로 롤업해 그대로 전달한다.
--
-- PK(segment_id, vehicle_profile_id)는 변경하지 않는다 — 이 기간 쌍이
-- 키에 포함돼야 하는지는 별도의 open question(context/data/schema-catalog.md
-- 참고)으로 남아 있다.
ALTER TABLE segment_comfort_score
    ADD COLUMN data_period_start TIMESTAMPTZ NOT NULL,
    ADD COLUMN data_period_end TIMESTAMPTZ NOT NULL;

ALTER TABLE segment_comfort_score_staging
    ADD COLUMN data_period_start TIMESTAMPTZ NOT NULL,
    ADD COLUMN data_period_end TIMESTAMPTZ NOT NULL;
