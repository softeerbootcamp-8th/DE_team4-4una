---
status: accepted
date: 2026-08-25
supersedes:
superseded_by:
---

# 0012. in-flight 품질 검증을 별도 Job Run 없이 수행한다

## 배경

ADR-0004는 in-flight 품질 검증을 각 TaskGroup 안에 `run_X` 다음 순서의
`validate_X` task로 붙이기로 했다. 이 저장소에서 batch-jobs task는 곧 EMR
Serverless Job Run이므로(ADR-0001), 이 배치는 검증마다 Job Run을 하나씩 더
낸다는 뜻이 되었다.

#462가 수집한 베이스라인
(`docs/perf/2026-08-25-comfort-score-pipeline-baseline.md`,
`standard_score_pipeline` 5건)에서 그 비용이 드러났다.

| task | 총소요 | 프로비저닝 | 검증 연산 |
| --- | ---: | ---: | ---: |
| `validate_sensor_processing` | 3:01~4:02 | 1:24~1:38 | 0:24~0:33 |
| `validate_hourly_scoring` | 3:01~3:02 | 1:23~1:31 | 0:22 |
| `validate_standard_score` | 7:02 | 1:24~1:31 | 4:50~5:04 |

검증 3개 task 합계는 약 13분으로 DAG run 총시간의 약 30%인데, 실제 검증 연산은
5~6분이다. 나머지는 Job Run 프로비저닝(건당 평균 1:27)과 Spark 세션 생성이다.
ADR-0001이 사전 초기화 용량을 두지 않기로 했으므로 이 콜드 스타트는 저절로
줄지 않는다.

낭비가 두 갈래로 겹쳐 있다.

**첫째, Spark 검증이 방금 계산한 데이터를 S3에서 다시 읽는다.**
`validate_sensor_processing`과 `validate_hourly_scoring`이 검증하는 파티션은
바로 앞 task의 Spark 세션이 이미 들고 있던 DataFrame이다. 별도 Job Run이라 그
메모리에 닿을 수 없어 다시 읽는다.

**둘째, Gold 검증이 기준 데이터셋이 아니라 서빙 복제본을 훑는다.**
`validate_standard_score`는 `standard_segment_comfort_score`를
`WHERE score_as_of = ...`로 조회한다. 이 테이블의 PK는
`(segment_id, vehicle_profile_id, score_as_of)`라 `score_as_of` 단독 등치
조건으로는 인덱스를 탈 수 없고, 테이블은 매 실행 약 100만 행씩 누적되며
retention 경로가 없다. 결과 크기는 고정인데 그것을 찾는 비용만 계속 커진다.

그런데 같은 데이터가 S3 Gold에 `as_of`별 경로로 이미 격리돼 있고
(`comfort_score/standard_storage.py`), `run_standard_score`가 PostgreSQL에 쓰기
전에 그 snapshot을 이미 읽는다. `context/data/lineage.md`가 정한 대로 **S3
Gold가 기준 데이터셋이고 PostgreSQL은 서빙 스토어**다. 검증 대상은 서빙
복제본보다 기준 데이터셋이 자연스럽다.

**셋째, in-flight 검증 항목의 대부분이 실패할 수 없다.** 지금 suite가 검사하는
항목을 산출 코드와 대조하면 다음과 같다.

| 검사 | 항목 수 | 실패할 수 있는가 |
| --- | ---: | --- |
| `rms_*`, `p95_abs_*` 음수 불가 | 14 | **없다.** `_rms`는 `sqrt(avg(v²))`, `_p95_abs`는 `percentile_approx(abs(v))`다(`sensor_features/aggregation.py`). 산술적으로 음수가 나오지 않는다 |
| `avg_speed_mps` 음수 불가 | 1 | **없다.** `cleansing_rules.yaml`의 `speed_mps.min: 0`이 상류에서 `OUT_OF_RANGE`로 격리한다 |
| `comfort_score` 0~100 | 1 | **없다.** 마이그레이션 0006의 `CHECK (comfort_score BETWEEN 0 AND 100)`이 적재 시점에 강제한다 |
| 방향 점수 0~100 (hourly 3 + standard 3) | 6 | **현재 설정에서는 없다.** hourly는 `_normalized_penalty`가 `greatest(0, least(1, ratio))`로 clamp돼 `100*(1-penalty)` 형태고, standard는 `(N*c_obs + k*mu)/(N+k)` 볼록결합이라 범위를 물려받는다. 다만 아래 전제가 깨지면 벗어난다 |
| `score_version` / `scoring_version` SemVer 형식 | 2 | 사실상 없다. `score_version`은 `formula.py`의 모듈 상수 `SCORE_VERSION`을 `F.lit()`으로 박는다. `scoring_version`은 설정 파일에서 오지만 실행당 값 하나다 |
| `quarantine_rate ≤ 0.05` | 1 | **있다.** 입력 데이터 품질에 직접 좌우된다 |

in-flight 25개 항목 중 실패할 수 있는 것은 하나다. 나머지는 매시간 100만 행에
대해 이미 참인 명제를 다시 확인한다.

그런데 점수 범위 보장은 설정에 대한 전제 위에 서 있고, **그 전제를 검사하는 코드가
대부분 없다.**

| 전제 | 현재 상태 |
| --- | --- |
| hourly 컴포넌트 가중치 ≥ 0 | 없다. 합이 1인지만 본다(`comfort_scoring_config.py`) — `[-0.5, 1.5]`도 통과한다 |
| standard 방향 가중치 ≥ 0, 합 = 1 | 둘 다 없다. `ProvisionalThreshold`는 값이 숫자인지만 확인한다 |
| `shrinkage_k` ≥ 0 | 없다 |
| `SCORE_VERSION`이 SemVer 형식 | 없다 |
| `cleansing_rules.yaml`에 `speed_mps.min`이 남아 있음 | 없다. 지워도 아무도 모른다 |

가중치가 `[-0.5, 1.5]`이고 penalty가 `[1.0, 0.0]`이면 `weighted_penalty`가 -0.5가
되어 점수는 150이 된다. 지금 설정값으로는 그런 일이 없지만 막는 것도 없다. 즉 이
항목들은 "코드가 보장하는 값"이 아니라 **"코드가 보장하도록 되어 있으나 그 보장을
지키는 장치가 없는 값"**이다.

이 구조에서 GX는 설정 실수를 100만 행을 계산하고 적재까지 마친 뒤에 잡는다. 같은
실수를 설정 로드 시점에 몇 줄로 잡을 수 있다.

ADR-0004는 "하드 인바리언트는 GX로 옮기지 않는다"고 이미 정했다 — 스키마·PK·
필수값은 쓰기 시점에 강제하므로 GX가 다시 보지 않는다. 위 표들은 그 원칙이 한 걸음
덜 갔음을 보여준다. **값을 결정하는 자리에서 지킬 수 있는 것은 GX로 옮기지 않는다.**

## 결정

in-flight 품질 검증은 별도 EMR Serverless Job Run을 만들지 않는다. 검증 대상이
이미 존재하는 가장 가까운 자리에서 수행한다.

- **Spark 산출물**(`hourly_segment_features`, `hourly_comfort_score`) — 생산
  job과 같은 Spark 세션 안에서 검증한다. `validate_sensor_processing`과
  `validate_hourly_scoring` task는 제거하고, 검증 실패는 생산 task의 실패가 된다.
- **Gold**(`standard_segment_comfort_score`) — 검증 대상을 PostgreSQL 조회가
  아니라 manifest가 가리키는 S3 Gold 활성 snapshot으로 바꾼다. 대상이 단일
  parquet snapshot이라 Spark가 필요 없으므로 Airflow `PythonOperator`에서 GX
  `PandasExecutionEngine`으로 검증한다. `validate_standard_score` task와 그
  asset outlet(#249)은 그대로 유지한다.

suite JSON은 그 검증을 실행하는 서비스가 갖는다. 지금까지는 모든 검증이
batch-jobs에서 돌아 `batch_jobs/resources/expectations/`에 모여 있었지만,
`standard_segment_comfort_score` suite는 검증이 Airflow로 옮겨가므로
`services/orchestration/jobs/resources/expectations/`로 함께 옮긴다 —
`current_score_quarantine`(#251)의 suite가 이미 그 자리에 있다. git으로 버전
관리하고 배포 산출물에 포함한다는 ADR-0004의 원칙은 그대로다.

ADR-0004의 나머지 결정은 유지한다 — GX 채택, in-flight hard fail / at-rest soft
fail 정책, at-rest `data_quality_audit` DAG 분리. 이 ADR은 ADR-0004의 "Airflow
연결" 항목, "Gold는 `SqlAlchemyExecutionEngine`으로 직접 조회한다"는 대목, 그리고
"Suite 저장 위치" 항목을 개정한다.

### 검증 레벨

무엇을 GX로 볼지는 **그 값을 무엇이 결정하는가**로 나눈다. 값이 다른 자리에서
완전히 결정되면 GX로 보지 않는다.

- **레벨 0-A — 산술적으로 불가능.** `rms_*`, `p95_abs_*`(14개). suite에서 제거하고
  **아무것도 추가하지 않는다.** `sqrt(avg(v²)) ≥ 0`을 테스트하는 것은 산술을
  테스트하는 것이다. 의미 있는 테스트는 그 함수가 정말 RMS·절댓값 백분위를
  계산하는지이고, `test_sensor_features.py`에 이미 있다.
- **레벨 0-B — 값의 범위를 명시적으로 강제하는 코드나 제약이 이미 있다.**
  `comfort_score` 0~100(DB CHECK), `avg_speed_mps ≥ 0`(클렌징 `OUT_OF_RANGE`),
  `current_segment_comfort_score`의 방향 점수(`weather_rules.py`의
  `min(max(value, SCORE_MIN), SCORE_MAX)`), `standard_segment_comfort_score`의
  참조 무결성(FK). suite에서 제거한다. 이 강제 장치들은 지우면 코드에서 눈에
  띄므로 조용히 사라지지 않는다.
- **레벨 0-C — 전제만 지켜지면 값이 그대로 따라오는 것.** `score_version`,
  `scoring_version`. 상수나 설정값을 `F.lit()`으로 그대로 박을 뿐 사이에 계산이
  없으므로, 전제(상수·설정이 SemVer 형식인지)를 검사하면 100% 덮인다. **전제를
  검사하는 코드를 새로 만들고** suite에서 제거한다.
- **레벨 1 — in-flight hard fail.** 어긋나면 하류로 보내면 안 되는 것. 둘로 갈린다.
  - **데이터가 결정하는 것**: `quarantine_rate ≤ 0.05`.
  - **계산 구조의 부수 효과로만 범위가 지켜지는 것**: hourly와 standard의 방향
    점수 0~100(6개). hourly는 `100*(1-weighted_penalty/valid_weight)`, standard는
    `(N*c_obs + k*mu)/(N+k)` 볼록결합이라 범위가 명시적으로 강제되는 것이 아니라
    공식의 구조에서 따라 나온다. 전제(가중치 비음수, 합 = 1, `shrinkage_k` ≥ 0)를
    검사하는 코드는 **새로 만들되 suite에도 남긴다** — 설정 검증은 잘못된 입력만
    막을 뿐, 공식 자체가 바뀌어 범위가 조용히 깨지는 경우까지는 막지 못한다.
- **레벨 2 — at-rest soft fail.** 축적된 결과에서만 드러나는 것. freshness와, DB
  제약이 없어 실제로 깨질 수 있는 참조 무결성이 여기 속한다 —
  `current_segment_comfort_score.vehicle_profile_id`에는 FK가 없어 anti-join이
  의미가 있다(`gold_audit_validation.py`가 주석으로 이미 이 비대칭을 적어두었다).

기준을 한 문장으로 줄이면 이렇다. **범위가 명시적으로 강제되면 GX에서 빼고,
구조의 부수 효과로만 지켜지면 남긴다.** 명시적 강제는 지울 때 드러나지만 부수
효과는 조용히 깨지기 때문이다.

이 분류를 적용하면 GX 항목은 37개에서 13개로 줄고(in-flight 7, at-rest 6), 남는
항목은 모두 실제로 실패할 수 있는 것이 된다. 동시에 지금 어느 층에서도 막지 않는
설정 실수 — 음수 가중치, 합이 1이 아닌 가중치, 비-SemVer 버전 문자열 — 를 배치가
시작되기 전에 막게 된다.

레벨 1의 전제 검증(가중치 비음수, 방향 가중치 합 = 1, `shrinkage_k` 비음수)은
#495에서 함께 넣는다 — 지금 어느 층에도 없는 검사라 미룰 이유가 없다.

레벨 0-A/0-B/0-C 항목을 suite에서 걷어내는 일은 후속 이슈로 미룬다. 이번 성능
문제의 원인은 Job Run 오버헤드였고 그것은 위 실행 위치 변경으로 해결되므로, suite
정리는 급하지 않은 정합성 작업이다. 레벨 기준 자체는 이 ADR로 확정된 상태로 남는다.

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| 현행 유지(검증마다 별도 Job Run) | task 단위로 실패 원인과 소요시간이 Airflow UI에 그대로 드러난다 | 검증 1건마다 콜드 스타트 약 1:27을 계속 낸다. Spark 검증은 방금 쓴 데이터를 다시 읽는다 | 검증 연산이 20~30초인데 오버헤드가 그 5배다 |
| EMR Serverless 사전 초기화 용량(웜풀) 도입 | 코드를 바꾸지 않고 콜드 스타트를 줄인다 | 유휴 비용이 생겨 ADR-0001의 결정을 뒤집고, #432의 Application stop task와 충돌한다 | 오버헤드의 원인을 없애지 않고 비용으로 가린다 |
| Gold 검증에 `score_as_of` 인덱스 추가 | 마이그레이션 하나로 풀스캔이 사라진다 | 테이블 무한 증가는 남고, 검증이 여전히 서빙 복제본을 본다. 별도 Job Run 비용도 그대로다 | 증상만 완화한다. 기준 데이터셋이 이미 `as_of`별로 격리돼 있어 더 단순한 답이 있다 |
| Gold 검증 대상만 S3로 바꾸고 실행은 batch-jobs Job Run에 유지 | 실행 위치를 안 바꿔도 풀스캔이 사라진다 | 단일 parquet 하나를 읽으려고 Spark Job Run 콜드 스타트를 계속 낸다 | 검증 대상이 Spark를 필요로 하지 않는다 |
| 모든 산출물에 지금 수준의 GX 검증을 그대로 유지 | 규칙이 한곳에 모여 있고 계층별로 빠진 데가 없어 보인다 | 실패할 수 없는 검사 24개에 매시간 비용을 낸다. 통과가 아무것도 보증하지 않아 신호가 희석된다 | 검증의 가치는 실패 가능성에서 나온다 |
| 레벨 0 항목을 지우지 않고 실행 주기만 낮춘다(at-rest에서만 확인) | 회귀 안전망이 남고 삭제 결정을 미룰 수 있다 | 코드 결함을 여전히 데이터에서 사후에 찾는 구조다 | 같은 보장을 단위 테스트가 더 이르고 싸게 준다 |

## 결과

- 파이프라인에서 EMR Serverless Job Run이 3건 줄고, 검증 경로의 프로비저닝
  대기가 사라진다.
- `hourly_scoring` TaskGroup에는 task가 하나만 남는다. 그래도 그룹은 유지한다 —
  UI에서 단계 구분이 일관되게 보이는 편이 낫고, 그룹을 없애면 이 변경과 무관한
  DAG 구조까지 건드리게 된다.
- **Spark 검증의 실패 원인 구분이 task 경계에서 예외·로그 경계로 옮겨간다.**
  ADR-0004가 이미 단점으로 적어둔 사항이며, 전용 예외
  (`SensorProcessingValidationFailed` 등)와 #461의 PERF phase 로그로 구분한다.
- **검증만 따로 재시도할 수 없다.** 검증 실패는 데이터가 잘못됐다는 뜻이므로
  생산부터 다시 실행하는 편이 맞다고 판단했다.
- **PostgreSQL에 실린 값 자체를 보는 눈이 사라진다.** GX suite가 묻는 것은 점수
  범위와 `score_version` 형식으로 계산 결과의 성질이고, 적재 정합성은
  `comfort_score/standard_writer.py`가 쓰기 시점에 이미 강제한다(스키마 일치, PK
  중복, NaN, inserted/updated count). 다만 MERGE가 컬럼을 잘못 매핑하는 종류의
  결함은 어느 쪽으로도 잡히지 않게 된다 — 감수한다.
- Gold 검증 비용이 테이블 크기와 무관해진다. 다만 `jobs/pipeline_counts.py`의
  동일한 `WHERE score_as_of = %s` 조회와 `jobs/current_score.py`의 인덱스 전체
  스캔은 그대로 남는다. 서빙 스토어의 수명 정책은 이 ADR의 범위 밖이다.
- 검증이 Airflow 워커에서 도는 경로가 하나 늘어난다. 새 의존성은 없다 —
  `great-expectations`/`pandas`/`pyarrow`가 이미 설치돼 있고
  `jobs/current_score_quarantine.py`(#251)가 같은 GX pandas 경로를 쓴다.
- **suite를 정리하고 나면 검증 통과가 의미를 갖게 된다.** 지금은 in-flight 25개
  항목 중 실패 가능한 것이 1개뿐이라 통과 사실이 거의 아무것도 보증하지 않는다.
  레벨 기준을 적용하면 GX 항목이 37개에서 13개로 줄고, 남는 것은 모두 실제로
  실패할 수 있다. 이 정리는 후속 이슈에서 한다.
- **전제 검증을 새로 만들면서 지금보다 안전해지는 구간이 생긴다.** 음수 가중치,
  합이 1이 아닌 방향 가중치, 비-SemVer 버전 문자열은 현재 어느 층에서도 막히지
  않는다. 설정 로드 시점 검증은 그 실수를 배치가 시작되기도 전에 막는다.
- **방향 점수 범위는 설정 검증과 GX 양쪽에서 본다 — 의도한 중복이다.** 설정 검증은
  잘못된 입력을 막고, GX는 공식이 바뀌어 범위가 조용히 깨지는 경우를 잡는다. 두
  장치가 막는 대상이 다르므로 한쪽으로 합치지 않는다.
- **GX가 남을 만큼의 역할은 유지된다.** #250은 규칙 수가 적고 in-flight/at-rest
  suite 공유가 없으면 GX 대신 인라인 Python을 쓰기로 했다(`latest_zone_weather`).
  정리 후 13개는 그 기준을 넘고, at-rest Data Docs도 GX만 제공한다. 다만 여기서
  더 줄어들면 같은 판단을 다시 해야 한다.
- 레벨 분류는 지금 코드 기준의 판정이다. 산출 공식이 바뀌면 레벨도 다시 봐야
  하며, 특히 clamp나 볼록결합 구조를 걷어내는 변경은 레벨 0 항목을 레벨 1로
  되돌려야 한다.

## 영향 범위

- `libs/de4-core` — Gold snapshot manifest(`StandardGoldManifest`)를 서비스 간
  계약으로 올린다. batch-jobs가 쓰고 orchestration이 읽게 되므로 한쪽 서비스가
  소유할 수 없다. `RoadEnvironmentManifest`가 같은 이유로 이미 여기 있다.
- `services/batch-jobs` — `sensor_processing_validation.py`와
  `hourly_scoring_validation.py`를 생산 job이 직접 호출하도록 바꾼다.
  `standard_score_validation.py`와 그 테스트는 검증이 orchestration으로 옮겨가므로
  제거한다. `standard_storage.py`는 자체 manifest dataclass 대신 de4-core의 것을
  쓴다. CLI의 `validate-sensor-processing` / `validate-hourly-scoring` /
  `validate-standard-score` 서브커맨드를 제거한다.
- `services/orchestration` — `jobs/standard_score_validation.py`를 새로 만든다
  (`jobs/road_environment.py`와 같은 패턴: 계약은 de4-core에서 가져오고 읽는
  로직은 서비스 안에 둔다). `standard_score_pipeline`에서 검증 task 2개를
  제거하고 `validate_standard_score`를 `PythonOperator`로 바꾼다.
- `comfort_scoring_config.py`(컴포넌트 가중치 비음수), `comfort_score/config.py`
  (방향 가중치 비음수 및 합 = 1, `shrinkage_k` 비음수) — 레벨 1 항목이 기대는
  전제를 로드 시점에 강제한다.
- `services/batch-jobs/src/batch_jobs/resources/expectations/`(후속 이슈) — 레벨
  0-A/0-B/0-C 항목을 suite에서 제거한다. `hourly_segment_features_suite`는 15개가
  모두 빠져 파일 자체가 사라지고,
  `current_segment_comfort_score_audit_range_suite`도 마찬가지다. 방향 점수
  범위(레벨 1)는 남긴다. `cleansing/rules.py`의 `speed_mps` 하한 존재 확인과
  `SCORE_VERSION` 형식 단위 테스트도 그때 함께 넣는다 — 둘 다 지금은 대응하는 GX
  항목이 남아 있어 중복이다.
- `docs/adr/0004-data-quality-validation-with-great-expectations.md` — 개정되는
  두 대목에서 이 ADR을 참조한다.
- `context/data/quality-rules.md` — 레벨 체계와, 각 규칙이 어느 레벨인지 반영한다.
- `context/` — 검증의 실행 위치와 대상 변경을 반영한다.

## 참고

- 관련 이슈: #495
- 개정 대상: ADR-0004의 "Airflow 연결" 및 Gold 조회 경로 항목
- 베이스라인: `docs/perf/2026-08-25-comfort-score-pipeline-baseline.md` (#460, #462)
- 유사 선례: ADR-0006 — 동일 배치 흐름 안의 중간 영속 경계를 제거한 결정
