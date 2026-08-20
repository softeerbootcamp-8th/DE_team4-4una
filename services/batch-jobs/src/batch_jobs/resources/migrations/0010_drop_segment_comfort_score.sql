-- 구 Gold 테이블을 삭제한다 (#227, context/comfort-score.md "Migration order" 7단계).
--
-- #193에서 이 테이블 하나를 standard/latest_zone_weather/current 셋으로 쪼갰고,
-- #226에서 serving API가 current_segment_comfort_score로 옮겨갔다. 이제 이 테이블을
-- 읽는 곳도 쓰는 곳도 없다.
--
-- 0002/0004는 이미 적용된 migration이라 수정하지 않는다. 새 DB를 만들면 0002가
-- 테이블을 만들고 이 파일이 다시 지우는 흐름이 되는데, 이력을 그대로 재생하는 게
-- migration의 계약이므로 의도된 동작이다.
--
-- 데이터는 옮기지 않는다. 원본인 hourly_comfort_score가 데이터 레이크에 남아 있어
-- standard 경로로 언제든 재산출할 수 있다.
DROP TABLE segment_comfort_score_staging;
DROP TABLE segment_comfort_score;
