# `hourly_comfort_score` 시간 파티셔닝 설계 (#469)

부모 이슈: #468

## 배경

`standard_score_pipeline`은 실행마다 `hourly_comfort_score`(Silver3) 전체 이력을
읽는다. 근본 원인은 하나다 — 이 테이블에 시간 파티션이 없고 매 실행 전량
overwrite된다.

`hourly_segment_features`(Silver2)는 `data_period_date=YYYY-MM-DD/hour=HH`로 물리
파티션이 있지만(`hourly_segment_feature_storage.py:26-30`), Silver3는 파티션 없이
루트에 평면으로 쓰인다.

```python
# hourly_comfort_job.py:80  — Silver2 루트 전체를 필터 없이 read
features = spark.read.schema(HOURLY_SEGMENT_FEATURE_SCHEMA).parquet(config.feature_input_path)
# hourly_comfort_job.py:95-96 — Silver3와 rejected 전량 재작성
scored.write.mode("overwrite").parquet(config.score_output_path)
rejected.write.mode("overwrite").parquet(config.rejected_output_path)
```

`score-hourly-comfort` CLI에 `--target-hour`가 없고(`cli.py:66-71`), DAG가 넘기는
`_HOURLY_COMFORT_INPUT_PATH`(`standard_score_pipeline.py:87-90`)에도 시간 템플릿이
없어 대상 시간을 지정할 수단이 아예 없다.

### 전량 스캔이 일어나는 네 곳

| 위치 | 증상 | #460 베이스라인 |
| --- | --- | --- |
| `hourly_comfort_job.py:80,95-96` | Silver2 전체 read, Silver3 전량 overwrite | `run_hourly_scoring` 3:03 |
| `hourly_scoring_validation.py:116,136,143,147` | Silver3 전체를 3회 스캔 | `validate_hourly_scoring` 2:25 |
| `comfort_score/loader.py:52,86-89` | 루트 전체를 읽은 뒤 168시간 필터. 파티션 컬럼이 없어 프루닝 불가 | `run_standard_score` 5:36 |
| `jobs/pipeline_counts.py:92` | 루트를 재귀 나열해 전량 카운트 | 베이스라인 표에 없음 |

세 구간 모두 데이터가 쌓일수록 선형으로 늘어난다.

### 잘못된 전제가 문서에 굳어져 있다

`hourly_comfort_job.py:112-118`의 docstring은 "대상 시간대는 Airflow가 실행마다
템플릿으로 갈아끼우는 feature_input_path에 들어 있다"고 설명하지만 사실이 아니다.
DAG는 시간 템플릿을 넘기지 않는다. 이 전제가 `standard_score_pipeline.py:272-276`,
`hourly_scoring_validation.py:8-11`, `context/data/quality-rules.md:113-118`에
복제돼 "hour 파티션이 없으니 전체 검증이 맞다"는 설계 근거로 쓰이고 있다.

### 시간별 계산은 전량 재계산과 동치다

`calculate_hourly_comfort_scores`는 행 단위 계산이다. 교차 행 연산은
`distinct()`(`hourly_comfort.py:172`)와 `groupBy`(`:177`) 두 곳뿐이고 둘 다 입력
검증용이며, PK에 `data_period_start`가 포함돼 있어 한 시간 입력으로도 동일하게
동작한다. 파티셔닝의 전제가 성립한다.

## 확정된 결정

### 1. 파티션 레이아웃 — Silver2와 동일한 2단

```
silver/hourly_comfort_score/data_period_date=2026-08-25/hour=09/part-*.parquet
quarantine/hourly_comfort_score/data_period_date=2026-08-25/hour=09/part-*.parquet
```

Silver2가 이미 같은 규칙을 쓰고 있어 읽는 쪽에서 예측 가능하다. `rejected`도
PK에 `data_period_start`를 포함하므로(`hourly_comfort.py:95`) 그대로 적용된다.

### 2. 쓰기 — 기존 staging/rename 패턴 재사용

저장소에 이미 두 벌 있다(`cleansing/hourly_storage.py`,
`hourly_segment_feature_storage.py`). 신규 `hourly_comfort_storage.py`가 같은 절차를
따른다.

```
staging에 쓰기 → read-back으로 스키마·행수 검증 → 기존 파티션 backup → rename 교체 → backup 삭제
```

EMRFS의 `rename()`이 객체별 copy+delete라 원자적이지 않은 위험은 기존 두 곳과
동일하게 감수한다(#290에서 이미 문서화된 판단).

**backup 디렉터리는 `_` 접두어를 쓴다.** 기존 두 writer의 규칙이 다르다.
`hourly_storage.py:158`은 `_backup_<name>`이라 Spark 파티션 탐색이 무시하지만,
`hourly_segment_feature_storage.py:88`은 `hour=09.bak`이라 `hour="09.bak"` 값으로
인식돼 컬럼 타입 추론이 int에서 string으로 바뀔 수 있다. 새 writer는
`hourly_storage.py` 규칙을 따른다.

### 3. 읽기 — 검증은 파티션 스코프, 롤업은 정확한 168시간 프루닝

`validate-hourly-scoring`은 `--target-hour`를 받아 이번 실행이 쓴 파티션만 검증한다.
`validate-sensor-processing`이 이미 쓰는 방식과 같다
(`sensor_processing_validation.py:131-147`). 파티션이 없으면 hard fail하고,
`row_count == 0` 가드는 유지한다.

`load_hourly_comfort_score_for_gold`는 루트를 읽어 파티션 컬럼으로 프루닝한다.
경로 168개를 나열하면 존재 확인만 168번 필요하고 결손 처리도 따로 해야 한다.
프루닝은 존재하지 않는 파티션이 자연히 빠진다.

윈도우 `[as_of - window_hours, as_of)`의 양 끝이 시(hour) 경계이므로, 날짜 단위로
자르면 최대 24시간을 더 읽는다. 정확히 168개만 읽도록 복합 조건을 쓴다.

```
(data_period_date >  start.date()) OR (data_period_date == start.date() AND hour >= start.hour)
AND
(data_period_date <  end.date())   OR (data_period_date == end.date()   AND hour <  end.hour)
```

기존 `data_period_start` 정밀 필터(`loader.py:86-89`)는 그대로 둔다 — 파티션 값과
데이터 값이 어긋나는 경우에 대한 안전망이다.

파티션 컬럼이 읽기 시 추가 컬럼으로 붙지만, `loader.py:66-68`의 `_validate_schema`가
"여기 없는 추가 컬럼은 문제 삼지 않는다"고 명시돼 있고 `formula.py`의
`REQUIRED_SCHEMA` 검사도 같은 함수를 재사용하므로 영향이 없다.

### 4. `scoring_version` 혼합을 허용한다

지금은 매 실행이 전 이력을 덮어쓰기 때문에, `hourly_comfort.yaml`의
`scoring_version`을 올리면 다음 실행 한 번으로 과거 전체가 새 공식으로 통일된다.
의도한 설계가 아니라 전량 재계산의 부수효과다. 파티셔닝하면 이 효과가 사라지고
168시간 윈도우에 여러 버전이 섞인다.

이 상태를 그대로 허용한다.

- `N`(qualifying hours)과 `Confidence = N/(N+k)`가 유지된다. 버전 변경 후에도
  윈도우가 168시간을 채우므로 점수가 급변하지 않는다
- 버전 변경의 영향이 7일에 걸쳐 서서히 반영된다. 절벽이 아니라 경사로다
- `scoring_version` 변경 빈도가 낮고, 점수의 절대값보다 구간 간 상대 순위가 중요하다

받아들이는 대가는 이렇다.

- 한 standard 점수가 두 공식의 결과를 평균한 값이 된다
- 이 이동이 아무 신호 없이 일어난다. 나중에 "왜 점수가 움직였는지" 추적할 단서가
  데이터에도 로그에도 남지 않는다

`loader.py::_select_latest_scoring_version`은 이 혼합을 걸러내지 못한다 — PK가
`(segment_id, vehicle_profile_id, data_period_start)`라 시간당 행이 하나뿐이면 고를
대상이 없다. 현재도 전량 overwrite라 항상 단일 버전이어서 이미 실질적으로 동작하지
않는 코드이고, 이 결정으로 앞으로도 그렇다.

**향후 전환 경로.** 혼합이 문제가 되면(예: `scoring_version`을 자주 바꾸게 되거나,
점수 절대값의 시계열 비교가 필요해지면) `score-hourly-comfort`에 시간 범위 인자를
추가해 버전 변경 시 168시간을 명시적으로 재계산하는 방식으로 옮긴다. 저장 레이아웃을
바꾸지 않으므로 추가 마이그레이션 없이 가능하다. 재계산 비용은 현재 매시간 치르고
있는 비용과 같고, 범위가 168시간으로 한정되므로 오히려 가볍다.

**채택하지 않은 대안** — `run_standard_score`가 현재 `scoring_version`과 일치하는
행만 읽는 방식. 버전 변경 직후 윈도우에 1시간만 남아 `N`이 168에서 1로,
`Confidence`가 0.944에서 0.091로(k=10) 붕괴하고, 점수의 91%가 모집단 평균이 되어
구간 간 구분이 7일간 사라진다.

### 5. `zero_sample_rate` 검증을 제거한다

이 검증은 구조적으로 실패할 수 없다. `hourly_comfort.py:72`의 `eligible` 조건에
`sample_count > 0`이 있어 `sample_count = 0`인 행은 전부 `rejected`로 빠지고
(`:95`), `output`(`:81`)에는 들어갈 수 없다. 따라서
`hourly_scoring_validation.py:143`의 분자가 항상 0이고 비율은 항상 0.0이라 임계값
0.05를 언제나 통과한다.

파티션 스코프로 다시 쓰는 김에 이식하지 않고 뺀다. `validate_hourly_scoring`에는
점수 범위(0~100)와 `scoring_version` SemVer 형식 검증이 남는다.

원래 의도했을 카나리아는 `sensor_processing`의 격리 비율 선례를 따른
`rejected / (scored + rejected)`로 보이지만, `rejected` 출력을 읽는 경로가 지금
없어 이번 범위에서 다루지 않는다.

### 6. `pipeline_counts`에 파티션 경로를 넘긴다 (#470에서 이관)

`pipeline_counts.py:92`는 `hourly_comfort_output_path`를 루트 그대로 넘기고,
`ObjectStore.list_objects`는 재귀다(`storage.py:192`). 파티션 writer가 루트 아래
`_staging/<run_id>`를 만들므로, 직전 실행이 죽어 남은 잔여물(#380에서 실제로 겪음)이
있으면 그 행까지 세어 건수가 부풀어 오른다.

quarantine/feature와 동일하게 `target_hour` 파티션 경로를 조합해 넘긴다. #470에서
"#469 이후로" 미뤄둔 항목이며, 이 이슈에서 함께 처리하지 않으면 해당 구간이 깨진
채로 남는다.

### 7. 기존 평면 데이터는 아카이브로 옮긴다

평면 `part-*.parquet`와 파티션 디렉터리가 한 루트에 공존하면
`spark.read.parquet()`가 `Conflicting directory structures`로 실패한다. 전환 전에
평면 데이터를 루트에서 치워야 한다.

재파티션(기존 점수 보존)이나 Silver2에서의 재생성 대신, 평면 데이터를 reference
버킷의 `raw/comfort_score_archive/` 아래로 통째로 옮기고 빈 상태에서 새로 쌓는다.
계산이 없는 순수 객체 이동이라 `aws s3 mv` 두 번으로 끝나고, 마이그레이션용 CLI
서브커맨드와 그 제거 후속 이슈가 통째로 사라진다. 삭제가 아니라 이동이므로 판단이
틀렸을 경우 아카이브에서 되꺼내 재파티션할 수 있다.

평면 데이터는 시간별로 나뉘어 있지 않아 부분 선별이 불가능하다 — 전량이 아카이브로
간다.

**받아들이는 대가.** 이동 후 첫 실행의 168시간 윈도우에는 1시간만 들어 있어 `N`이
1, `Confidence`가 1/11 = 0.091(k=10)이 되고, 점수의 91%가 모집단 평균이 된다. 구간
간 구분이 사실상 사라진 상태가 윈도우가 다시 찰 때까지 7일간 이어진다.
`current_segment_comfort_score`는 `standard_segment_comfort_score`를 그대로 읽어
날씨 보정만 얹으므로(`jobs/current_score.py:82`) 같은 영향을 그대로 물려받는다.
개발 단계이고 7일 뒤 자연 회복되므로 감수한다.

**배포 순서.** 4단계를 2~3단계보다 먼저 하면 구 writer가 평면 파일을 다시 만들어
같은 문제가 재발한다.

```
1. standard_score_pipeline DAG 일시정지
2. aws s3 mv --recursive
     s3://<lake>/silver/hourly_comfort_score/
     s3://<reference>/raw/comfort_score_archive/hourly_comfort_score/
3. aws s3 mv --recursive
     s3://<lake>/quarantine/hourly_comfort_score/
     s3://<reference>/raw/comfort_score_archive/quarantine_hourly_comfort_score/
4. 코드 배포 (파티션 writer/reader)
5. DAG 재개, 첫 실행 확인
```

## 전체 데이터 흐름

**이전** — 매 실행이 전 이력을 읽고 전 이력을 다시 쓴다.

```
hourly_segment_features/          ─ 전체 read ─┐
  data_period_date=…/hour=…/                   │
                                               ▼
                                    calculate_hourly_comfort_scores
                                               │
hourly_comfort_score/       ◀── 전량 overwrite ┘
  part-*.parquet                    │
                                    ├─ validate: 전체 3회 스캔
                                    ├─ loader:   전체 read 후 168h 필터
                                    └─ counts:   전체 재귀 나열
```

**이후** — 한 시간만 읽고 한 파티션만 교체한다.

```
hourly_segment_features/                     hourly_comfort_score/
  data_period_date=D/hour=H/ ── read ──▶ score ──▶ data_period_date=D/hour=H/
                                                     │
                                                     ├─ validate: 해당 파티션만
                                                     ├─ loader:   168개 파티션만 (프루닝)
                                                     └─ counts:   해당 파티션만
```

## 컴포넌트

### `services/batch-jobs`

| 파일 | 변경 |
| --- | --- |
| `hourly_comfort_storage.py` (신규) | staging → read-back 검증 → `_` 접두어 backup → rename 교체. `hour_output_path` 헬퍼 |
| `hourly_comfort_job.py` | Silver2 파티션만 read, 신규 storage 모듈로 쓰기. `:112-118` docstring 정정 |
| `hourly_scoring_validation.py` | `target_hour` 파티션만 검증, `zero_sample_rate` 제거. `:8-11` docstring 정정 |
| `comfort_score/loader.py` | 정확한 168시간 파티션 프루닝 |
| `cli.py` | `score-hourly-comfort`/`validate-hourly-scoring`에 `--target-hour` 추가 |
| `resources/expectations/hourly_comfort_score_zero_sample_rate_suite.json` | 삭제 |

### `services/orchestration`

| 파일 | 변경 |
| --- | --- |
| `dags/standard_score_pipeline.py` | 두 task에 `--target-hour` 전달, `:272-276` 주석 정정 |
| `jobs/pipeline_counts.py` | `hourly_comfort_output_path`에 파티션 경로 조합 |
| `README.md` | 아카이브 이동 절차와 168시간 회복 구간 기록 |

### `context/`

| 파일 | 변경 |
| --- | --- |
| `data/quality-rules.md:113-118` | "full recompute of every historical hour" 기술 갱신, 버전 혼합 결정 반영, `zero_sample_rate` 항목 제거 |
| `data/schema-catalog.md` | `hourly_comfort_score` 절에 파티션 키 명시 |

## 테스트 전략

- **쓰기 멱등성** — 같은 `--target-hour`로 두 번 실행해도 해당 파티션만 교체되고
  다른 시간대 파티션은 바뀌지 않는다
- **파티션 격리** — 두 시간대를 순서대로 쓰면 둘 다 남는다(덮어쓰지 않는다)
- **backup 명명** — 실패 주입 시 backup 디렉터리 이름이 `_`로 시작해 Spark 파티션
  탐색에 잡히지 않는다
- **정확한 프루닝** — 169시간치 파티션을 만들고 loader가 정확히 168개만 읽는지,
  경계 시각이 포함/제외되는지 확인한다
- **결손 파티션** — 윈도우 중간에 파티션이 없어도 실패하지 않고 나머지를 읽는다
- **검증 스코프** — `validate-hourly-scoring`이 다른 시간대의 잘못된 값에 영향받지
  않는다
- **건수 조회** — `report_processing_counts`가 stale `_staging` 잔여물에
  영향받지 않는다

## 제외 범위

- `hourly_comfort_score` PK에 `scoring_version` 포함 여부 (open question)
- scoring 알고리즘 자체의 변경
- 버전 변경 시 명시적 백필 수단 — 위 "향후 전환 경로" 참고, 필요해지면 별도 이슈
- 제거한 `zero_sample_rate`를 대신할 rejection-rate 검증 — 카나리아가 필요해지면
  별도 이슈
- `hourly_segment_feature_storage.py:88`의 `.bak` 명명 문제 — 별도 이슈
- `run_sensor_processing`(20:04) 최적화 — #468 제외 범위에 이미 명시된 별도 작업
