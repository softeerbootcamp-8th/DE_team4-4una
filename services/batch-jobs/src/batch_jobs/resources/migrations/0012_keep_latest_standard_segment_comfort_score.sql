-- standard_segment_comfort_score를 (구간, 프로필)별 최신 세대 1행만 담는 서빙
-- 스토어로 바꾼다 (#503, docs/adr/0012-validate-in-place-without-dedicated-job-
-- runs.md가 정한 "S3 Gold = 기준, Postgres = 서빙 스토어"의 실행).
--
-- PK에 score_as_of가 있어 매시 실행이 (구간 199,466 x 프로필 5) 전량을 새 행으로
-- INSERT했다 — 하루 약 2,400만 행씩 무한히 늘어난다
-- (docs/perf/2026-08-25-comfort-score-pipeline-baseline.md). 읽는 쪽은
-- serving-api도 current_score job도 (구간, 프로필)별 최신 1건만 쓰고, 이력은 S3
-- Gold에 score_as_of별 불변 snapshot으로 이미 남아 있어 Postgres 쪽 이력은 중복이다.

-- 0006의 3컬럼 FK. 이름은 Postgres가 63자로 잘라 만든 것이라 standard_score_as_of가
-- 통째로 빠져 있다(pg_constraint에서 확인한 실제 값이며, 추측으로는 맞힐 수 없다).
--
-- 이 FK가 남아 있으면 옛 세대를 지울 수 없다. current_score_pipeline은 standard가
-- 새 세대를 적재한 "뒤에" 도는 구조라(current_score_pipeline.py) 갱신 직전의 current는
-- 항상 이전 세대를 가리키고, 거기에 더해 GX 행 단위 격리(ADR-0008)에 걸린 행과
-- changed_zones_only 실행이 놓친 행(jobs/current_score.py)은 몇 시간이든 옛 세대에
-- 묶여 있다. 아래에서 참조를 (구간, 프로필)로 좁혀 되건다.
ALTER TABLE current_segment_comfort_score
    DROP CONSTRAINT current_segment_comfort_score_segment_id_vehicle_profile_i_fkey;

-- (구간, 프로필)별 최신 score_as_of 1행만 남긴다. 0007의 zone_weather_snapshot
-- 정리와 같은 형태다.
DELETE FROM standard_segment_comfort_score
WHERE (segment_id, vehicle_profile_id, score_as_of) NOT IN (
    SELECT segment_id, vehicle_profile_id, MAX(score_as_of)
    FROM standard_segment_comfort_score
    GROUP BY segment_id, vehicle_profile_id
);

-- score_as_of는 0006의 CREATE TABLE이 직접 NOT NULL로 선언했으므로 PK에서 빠져도
-- NOT NULL 일반 컬럼으로 남는다. 컬럼 순서는 segment_id 선행을 유지한다 —
-- jobs/current_score.py의 WHERE segment_id = ANY(...)가 선행 컬럼을 요구한다.
ALTER TABLE standard_segment_comfort_score
    DROP CONSTRAINT standard_segment_comfort_score_pkey;
ALTER TABLE standard_segment_comfort_score
    ADD PRIMARY KEY (segment_id, vehicle_profile_id);

-- 이제 이 테이블이 받는 건 매시 전량 UPSERT뿐이다. 시간당 약 100만 건의 UPDATE가
-- PK 인덱스를 churn시키지 않으려면 HOT update가 성립해야 하는데, HOT은 새 행 버전을
-- 같은 페이지에 놓을 여유가 있을 때만 성립한다. 힙 fillfactor 기본값 100은 그 여유를
-- 남기지 않는다. (이미 채워진 페이지에는 소급되지 않는다 — 위 DELETE가 비운 공간을
-- autovacuum이 회수하면서 점차 적용된다.)
--
-- autovacuum_vacuum_scale_factor는 안전장치다. 기본 0.2면 dead tuple이 전체의 20%까지
-- 쌓여야 vacuum이 도는데, HOT이 어떤 이유로 깨지면 그때까지 테이블이 부풀어 오른다.
--
-- 인덱스는 PK 하나로 끝낸다. 특히 score_as_of에 걸지 않는다 — HOT은 갱신되는 컬럼이
-- 전부 비인덱스일 때만 성립하고 score_as_of는 이제 매시 갱신 대상이라, 인덱스를 걸면
-- HOT이 통째로 깨진다. 얻는 건 하루 한 번 도는 gold_audit_validation.py의
-- MAX(score_as_of)뿐이다.
ALTER TABLE standard_segment_comfort_score
    SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.05);

-- 자동 명명에 맡기면 절단 때문에 방금 지운 3컬럼 FK와 이름이 똑같아진다. 정의가 다른
-- 제약이므로 이름을 명시해 구분한다. score_as_of가 FK에서 빠졌으니 current 행이 가리키는
-- standard 세대가 실제로 존재하는지는 DB가 더 이상 강제하지 않는다 — 어느 세대로
-- 계산했는지 남기는 감사 컬럼이 된다(신선도 임계치는 OQ-042로 열려 있다).
ALTER TABLE current_segment_comfort_score
    ADD CONSTRAINT current_segment_comfort_score_standard_score_fkey
        FOREIGN KEY (segment_id, vehicle_profile_id)
        REFERENCES standard_segment_comfort_score (segment_id, vehicle_profile_id);
