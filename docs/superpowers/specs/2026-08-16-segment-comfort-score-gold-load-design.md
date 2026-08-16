# segment_comfort_score Gold PostgreSQL 적재 설계 (#129)

## 배경

`#127(comfort_score/formula.py)`이 이미 Segment × vehicle_profile 단위와
vehicle-agnostic(`vehicle_profile_id=0` sentinel) 단위의 `comfort_score`/
`confidence_score`를 계산해 하나의 Spark DataFrame으로 내놓는다. 이 이슈는
그 결과를 Gold `segment_comfort_score` 테이블(PostgreSQL)에 UPSERT로
적재하는 부분만 다룬다. Serving Store 이관(`gold-loader` 영역)은 범위
밖이다.

관련 이슈: #101, #102, #117, #127, #129 (본 이슈)

> 이 문서는 세 차례 검토(외부 리뷰 2회 + 자체 리뷰 1회)를 거쳐 정리됐다.
> 각 절의 결정에는 왜 그렇게 정했는지 근거를 남겼다.

## 확정된 결정

### 1. 컬럼 범위 — MVP

`segment_comfort_score`는 `#127` 출력과 정확히 일치하는 컬럼만 갖는다.
타입은 formula.py가 실제로 내놓는 Spark 타입을 그대로 따른다:

| 컬럼 | PostgreSQL 타입 | 제약 | 출처 |
| --- | --- | --- | --- |
| `segment_id` | `TEXT` | `NOT NULL`, PK | formula.py (`StringType`) — `road_segment` FK는 이번 이슈 범위 밖(orphan `segment_id`를 막지 못함, 후속 이슈) |
| `vehicle_profile_id` | `INTEGER` | `NOT NULL`, PK, FK → `vehicle_profile` | formula.py (`IntegerType`). `0`은 vehicle-agnostic sentinel |
| `comfort_score` | `DOUBLE PRECISION` | `NOT NULL`, `CHECK (comfort_score BETWEEN 0 AND 100)` | formula.py (`DoubleType`) |
| `confidence_score` | `DOUBLE PRECISION` | `NOT NULL`, `CHECK (confidence_score BETWEEN 0 AND 1)` | formula.py (`DoubleType`) |
| `sample_count` | `BIGINT` | `NOT NULL`, `CHECK (sample_count >= 0)` | formula.py (`LongType`) |
| `score_version` | `TEXT` | `NOT NULL` | formula.py `SCORE_VERSION` 상수 |
| `calculated_at` | `TIMESTAMPTZ` | `NOT NULL` | Gold Job이 `as_of`로 채움 |

**CHECK 범위의 근거는 타입이 아니라 코드다**:

- `comfort_score ∈ [0,100]`: `hourly_comfort.py::_component_score`가
  `F.greatest(0.0, F.least(1.0, ratio))`로 penalty를 `[0,1]`에 클램핑하므로
  `vertical/longitudinal/lateral_score`는 구조적으로 `[0,100]`이다.
  `comfort_score`는 이 값들의 가중 평균(가중치 합 1.0, `comfort_score.yaml`)과
  shrinkage 평균의 재가중 평균이라 같은 범위를 벗어나지 않는다.
- `confidence_score ∈ [0,1)`: `Confidence = N/(N+k)`, `N`(qualifying_hours)은
  `0`~`168`(168h 윈도우), `k`(shrinkage_k)는 `comfort_score.yaml`의 양수
  상수(현재 10.0). `k>0`이 유지되는 한 분모가 0이 될 수 없고 값은 항상
  `[0,1)`이다.

`DOUBLE PRECISION`을 택한 이유: `NUMERIC`은 `'NaN'::numeric`을 허용하지
않아 formula.py가 만에 하나 NaN을 내놓으면 캐스팅 단계에서 예외가 나고,
`DOUBLE PRECISION`은 Spark `DoubleType`과 JDBC 상에서 1:1로 대응돼
staging↔본 테이블 간 암묵적 캐스팅이 없다. `CHECK` 제약은 방어선이지 NaN을
막는 장치는 아니므로, MERGE 직전 검증(§5)에서 NaN/Inf도 함께 걷어낸다.

`data_period_start`/`data_period_end`/`reference_date`/`speed_band`는
제외한다 (`enriched_segment_reference` 미구현, schema-catalog가 이미 open
question으로 표시, #129 완료조건이 "#127 연산 결과와 일치하는 스키마"로
명시).

**의도된 제약**: `score_version`은 PK에 포함하지 않는다. "항상 활성 버전
하나만" 담는 최신 스냅샷이라는 전제이며, 버전 혼재 시나리오는 후속 이슈.

### 2. 마이그레이션 도구 — 번호 붙인 raw SQL 파일 + `batch-jobs` CLI

Alembic 대신 **번호 붙인 raw `.sql` 파일 + 짧은 실행기**를 택한다(이
저장소는 지금까지 ORM/SQLAlchemy를 쓰지 않음).

**실행기는 `db/`가 아니라 `services/batch-jobs` 패키지 모듈로 둔다.** 루트
`pyproject.toml`은 `package = false`이고 workspace members는
`libs/*`/`services/*`뿐이라(`tool.uv.workspace.members`), `db/` 아래에
독립 스크립트를 두면 그 스크립트가 쓸 `psycopg2` 의존성을 선언할 곳도, `uv
run --all-packages pytest`가 그 테스트를 찾을 경로도 없다. 이미 `psycopg2`를
의존성으로 갖는 `batch-jobs`에 실행기를 두면 이 문제가 자연히 사라진다.

- `db/migrations/*.sql` — 순수 SQL 데이터 파일만 둔다(코드 없음).
- `services/batch-jobs/src/batch_jobs/migrate.py` — 실행기 로직(§3).
- `cli.py`에 `migrate-database` 서브커맨드 추가 (`load-segment-comfort-score`와
  같은 자리).
- `Makefile`에 한 줄 추가: `MIGRATE_CMD ?= uv run --package batch-jobs
  batch-jobs migrate-database`. 기존에 `MIGRATE_CMD`를 넘겨 쓰던 사용자는
  그대로 override되므로 동작이 바뀌지 않는다.
- `db/README.md`(신규, 짧게) — `db/migrations/*.sql`이 무엇이고 왜 여기
  있는지, 실행은 `make migrate`로 한다는 것만 안내한다(실행기 자체의
  구현은 `services/batch-jobs`에 있다는 점도 명시).

파일명 규칙은 `NNNN_설명.sql`. 이미 적용된 파일은 절대 수정하지 않는다
(AGENTS.md) — §3에서 이를 코드로도 강제한다.

**의도된 단순화**: 이 실행기를 `batch-jobs` 전용 CLI에 두는 것은 현재
Postgres에 쓰는 서비스가 `batch-jobs`뿐이기 때문의 실용적 선택이다.
`gold-loader` 등 다른 서비스가 나중에 같은 스키마를 다뤄야 하면, 실행기를
공유 위치(`libs/de4-core` 등)로 옮기는 걸 그때 다시 논의한다(YAGNI, 후속
과제).

### 3. `batch_jobs.migrate` (마이그레이션 실행기)

1. **부트스트랩**: `CREATE TABLE IF NOT EXISTS schema_migrations (filename
   TEXT PRIMARY KEY, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT
   NULL)`.
2. **동시 실행 방지**: 시작 시 전용 advisory lock 키를 획득(§5 Gold Job의
   락 키와 다른 값 — 두 락 키는 한 곳에 상수로 모아 문서화해 향후 세 번째
   사용자가 값을 재사용해 충돌하는 걸 막는다), 실패 시 즉시 실패.
3. **적용 순서**: `db/migrations/*.sql`을 파일명 순으로 순회. 각 파일의
   `sha256` 체크섬을 계산해:
   - `schema_migrations`에 없으면 트랜잭션 안에서 실행 후 `(filename,
     checksum, now())`를 기록한다.
   - 있는데 체크섬이 다르면 **하드 실패** — "이미 적용된 마이그레이션
     파일이 수정됨"을 AGENTS.md 규칙 위반으로 코드가 직접 잡아낸다.
   - 있고 체크섬이 같으면 스킵한다(재실행이 안전).
4. 접속 정보는 `POSTGRES_HOST`/`PORT`/`DB`/`USER`/`PASSWORD`
   (`.env.example`에 이미 있으나 지금까지 미사용). `db/migrations` 경로는
   기본값(저장소 루트 기준 상대 경로)이되 `DB_MIGRATIONS_DIR`로 override
   가능하게 한다(다른 `*Config.from_env()`와 동일한 패턴).

### 4. 마이그레이션 파일 구성

- **`0001_create_vehicle_profile.sql`** — `vehicle_profile` 테이블
  (`context/data/schema-catalog.md` 149~162행 정의 그대로: `manufacturer`/
  `model_name`/`mass_kg`/`wheelbase_mm`/`suspension_type`은 카탈로그가 이미
  nullable로 정의; `profile_name`/`vehicle_class`/`vertical_weight`/
  `longitudinal_weight`/`lateral_weight`/`is_active`/`created_at`/
  `updated_at`은 `NOT NULL`). **`vehicle_profile_id`는 `GENERATED ALWAYS AS
  IDENTITY`/`SERIAL`이 아닌 plain `INTEGER NOT NULL PK`** — 실제 차량
  프로필은 Bronze 소스가 부여한 ID를 그대로 쓰므로, 다음 파일의
  `vehicle_profile_id = 0` 명시 INSERT가 `OVERRIDING SYSTEM VALUE` 없이
  통과한다.
- **`0002_create_segment_comfort_score.sql`** — §1의 컬럼/제약, PK
  `(segment_id, vehicle_profile_id)`, FK → `vehicle_profile`. **staging
  테이블도 같은 파일에서 명시 생성한다**(Spark 추론 타입에 맡기지 않음,
  §5에서 이유 설명):

  ```sql
  CREATE TABLE segment_comfort_score_staging (
      segment_id TEXT NOT NULL,
      vehicle_profile_id INTEGER NOT NULL,
      comfort_score DOUBLE PRECISION NOT NULL,
      confidence_score DOUBLE PRECISION NOT NULL,
      sample_count BIGINT NOT NULL,
      score_version TEXT NOT NULL,
      calculated_at TIMESTAMPTZ NOT NULL
  );
  -- 의도적으로 PK/UNIQUE 없음: 중복 유입을 여기서 막지 않고 MERGE 직전
  -- 애플리케이션 단(§5)에서 검증한다. UNIQUE 인덱스로 막으면 실패가 Spark
  -- write 단계의 JDBC 배치 에러로 나와 메시지가 나빠지기 때문 — 이
  -- 트레이드오프를 여기 명시해 재논의를 막는다.
  ```

- **`0003_seed_vehicle_profile_agnostic.sql`** — `vehicle_profile_id=0`
  sentinel 행:

  ```sql
  INSERT INTO vehicle_profile (
      vehicle_profile_id, profile_name, vehicle_class,
      vertical_weight, longitudinal_weight, lateral_weight,
      is_active, created_at, updated_at
  ) VALUES (
      0, 'ALL_VEHICLES', 'ALL',
      0.5, 0.3, 0.2,           -- comfort_score.yaml과 동일 (아래 설명)
      TRUE, '2026-08-16T00:00:00Z', '2026-08-16T00:00:00Z'
  )
  ON CONFLICT (vehicle_profile_id) DO NOTHING;  -- 재실행/이력 유실 시에도 안전
  ```

  이 sentinel 행은 "샘플 데이터"가 아니라 스키마 무결성의 일부라
  `db/seeds/`가 아닌 마이그레이션에 둔다 — 그래야 `make migrate` 한 번으로
  스키마와 함께 반드시 적용되고, formula.py의 vehicle-agnostic 행이 FK
  위반 없이 들어간다.

  `vertical_weight`/`longitudinal_weight`/`lateral_weight` = `0.5`/`0.3`/`0.2`는
  임의로 맞춘 값이 아니다. `formula.py::_combined_hourly_score`가 두
  grain(per-vehicle/vehicle-agnostic) 모두에 **동일한** `comfort_score.yaml`
  전역 가중치를 적용하고, vehicle-agnostic 경로는 어떤 차량별 보정도 하지
  않는다 — 즉 sentinel 행의 "가중치"는 정확히 그 전역 가중치와 같아야
  테이블이 실제 계산과 일치한다. **주의(drift 위험)**: 이 값은
  `services/batch-jobs/config/comfort_score.yaml`과 별개 파일에 하드코딩된
  중복 값이라, `comfort_score.yaml`의 가중치가 바뀌면 이 seed 파일도 같이
  갱신해야 한다 — 자동 동기화 장치는 없다(후속 과제). `vehicle_class='ALL'`은
  실제 차량 등급과 겹치지 않는 sentinel 전용 값임을 주석으로 남긴다.

### 5. UPSERT 구현 — Staging 테이블 + SQL MERGE

Spark JDBC는 네이티브 UPSERT를 지원하지 않는다. 순서는 다음과 같다 —
**advisory lock을 Spark write보다 먼저 잡는다.** 보호 대상은 MERGE가 아니라
staging 테이블 자체이므로, 잠그지 않은 채 write를 먼저 하면 두 실행이
겹칠 때 "A가 검증한 데이터와 A가 적재하는 데이터가 다른" 레이스가 생긴다:

```
psycopg2 connection open
  → pg_try_advisory_lock(<고정 정수>)  (실패 시 "이미 실행 중"으로 즉시 실패)
  → staging 테이블 존재 + 컬럼명/타입 일치 확인 (아래)
  → [Spark: staging에 truncate=true, mode=overwrite로 write]
  → 중복/NaN 검증 (아래)
  → MERGE 실행 (CTE + 집계 카운트)
  → 성공 시에만 staging TRUNCATE (아래)
  → pg_advisory_unlock, connection close
```

**staging 사전 확인**: Spark JDBC의 `truncate=true`는 **테이블이 이미
존재할 때만** TRUNCATE로 동작한다. 테이블이 없으면 Spark가 그냥 CREATE해
버리고 그 순간 타입은 Spark 추론값이 된다(§4에서 명시 DDL로 막아둔 문제가
되살아남). 그래서 advisory lock을 잡은 직후, 같은 커넥션에서
`information_schema.columns`로 `segment_comfort_score_staging`의 존재와
컬럼명/타입이 §4 DDL과 일치하는지 확인하고, 불일치면 "`make migrate`를
먼저 실행하라"는 명확한 메시지로 실패시킨다.

**중복/NaN 검증**: staging에 대해 `SELECT count(*), count(DISTINCT
(segment_id, vehicle_profile_id))`를 실행해 두 값이 다르면(formula.py가
계약을 어기고 같은 키를 중복 생성한 것이므로) 하드 실패시킨다 — DISTINCT
ON으로 조용히 dedup하지 않는다. `comfort_score`/`confidence_score`에
`'NaN'`/`'Infinity'`가 있는지도 같은 단계에서 확인한다.

**MERGE** — `RETURNING`을 CTE로 감싸 집계만 받는다(순수 `RETURNING`을
`cur.execute()`로 실행하면 segment × profile 수만큼(수백만 행 가능) 전체
행이 드라이버 메모리로 올라온다):

```sql
WITH upserted AS (
    INSERT INTO segment_comfort_score
      (segment_id, vehicle_profile_id, comfort_score, confidence_score,
       sample_count, score_version, calculated_at)
    SELECT segment_id, vehicle_profile_id, comfort_score, confidence_score,
           sample_count, score_version, calculated_at
    FROM segment_comfort_score_staging
    ON CONFLICT (segment_id, vehicle_profile_id) DO UPDATE SET
      comfort_score = EXCLUDED.comfort_score,
      confidence_score = EXCLUDED.confidence_score,
      sample_count = EXCLUDED.sample_count,
      score_version = EXCLUDED.score_version,
      calculated_at = EXCLUDED.calculated_at
    RETURNING (xmax = 0) AS inserted
)
SELECT count(*) FILTER (WHERE inserted)     AS inserted_count,
       count(*) FILTER (WHERE NOT inserted) AS updated_count
FROM upserted;
```

`xmax = 0`은 동시/서브트랜잭션 상황에서 부정확할 수 있는 휴리스틱이지만,
advisory lock으로 이미 직렬화돼 있어 실용적으로는 충분하다 — "정확한
카운트가 아니라 관측값"으로 취급한다.

**staging 정리**: TRUNCATE는 **MERGE 성공 시에만** 실행한다. 다음 실행이
어차피 `truncate=true`로 다시 비우므로 실패 시에도 지울 필요가 없고,
오히려 실패한 staging 내용을 남겨둬야 원인 조사가 가능하다.

**읽기 가용성 보장**: Gold Job이 쓰기를 직렬화(advisory lock)하는 동안에도
`segment_comfort_score`(서빙 테이블)에 대한 일반 `SELECT`는 **절대 차단되지
않는다.** PostgreSQL MVCC에서 잠금 없는 읽기는 어떤 쓰기 트랜잭션의 길이나
row-level lock과도 무관하게 즉시 반환된다. 이 설계에서 실제로 읽기를
차단하는 `ACCESS EXCLUSIVE` 락이 나오는 지점은 `TRUNCATE` 하나뿐이고, 그
대상은 항상 `segment_comfort_score_staging`(내부 스크래치 테이블)이지
서빙 테이블이 아니다 — `segment_comfort_score`에 대한 DDL/TRUNCATE는 최초
마이그레이션(테이블 생성) 이후 어떤 실행 경로에서도 발생하지 않는다.
`pg_advisory_lock`도 애플리케이션 레벨 뮤텍스일 뿐 테이블 읽기 잠금과는
무관하다. **제약**: 이후 마이그레이션이 `segment_comfort_score`에
`ALTER TABLE`을 추가하면 그 순간은 `ACCESS EXCLUSIVE`라 읽기가 잠깐
막힌다 — 이는 이번 이슈 범위의 마이그레이션(신규 테이블 생성)에는 해당하지
않지만, 후속 스키마 변경 시 반드시 재확인해야 할 불변식으로 남긴다.

## 전체 데이터 흐름

```
silver/hourly_comfort_score (Parquet)
  → loader.py (#117, 기존, 무수정)      168h 윈도우 + 최신 scoring_version
  → formula.py (#127, 기존, 무수정)     comfort_score/confidence_score
                                       (vehicle_profile_id=0 sentinel 포함)
  → gold_job.py (신규)                 as_of 검증 + calculated_at 리터럴 부착
                                       + persist + 빈 결과 처리
  → gold_writer.py (신규)              lock → staging 확인 → write → 검증
                                       → MERGE → (성공 시) truncate → unlock
```

## 컴포넌트

### 마이그레이션 실행기 (`services/batch-jobs`, `db/`)

§2~§4에 정리된 `db/migrations/0001~0003.sql`과
`services/batch-jobs/src/batch_jobs/migrate.py`(§3의 부트스트랩/락/체크섬
로직), `cli.py`의 `migrate-database` 서브커맨드, `Makefile`의
`MIGRATE_CMD ?= uv run --package batch-jobs batch-jobs migrate-database`.

### Gold Job (`services/batch-jobs`)

- `comfort_score/gold_job.py` — `SegmentComfortScoreJobConfig.from_env()`,
  `run_segment_comfort_score_job(spark, config, as_of)`:
  1. **`as_of` 검증**: `as_of.utcoffset() is None`이면 하드 실패 —
     `hourly_comfort.py::_validate_arguments`가 `processed_at`에 이미 쓰는
     것과 동일한 패턴("naive datetime 금지"). KST 로컬/UTC 컨테이너 환경이
     섞여 있어 naive datetime을 허용하면 `calculated_at`에 조용히 9시간
     오차가 들어간다(stale 판정에 직결).
  2. `loader.load_hourly_comfort_score_for_gold` → `formula.compute_segment_comfort_scores`
  3. `calculated_at` 부착 — `F.current_timestamp()`가 아니라 `F.lit(as_of)`로
     드라이버에서 한 번 계산한 값을 리터럴로 고정한다.
  4. 결과를 `persist(StorageLevel.MEMORY_AND_DISK)`한다 — 빈 결과 판정(다음
     단계)의 `count()`와 이어지는 write가 168h 윈도우 파이프라인을 두 번
     돌리지 않도록. `hourly_comfort_job.py`가 이미 쓰는 패턴과 동일. write
     완료 후 `finally`에서 `unpersist()`.
  5. **빈 결과 처리**: 0행이면 MERGE를 생략(→ 락도 획득하지 않음)하고
     `merged_count=0`을 담은 summary를 반환하며 경고 로그를 남긴다(하드
     실패 아님 — 최초 부트스트랩처럼 합법적으로 빈 윈도우일 수 있음).
     판단은 이 summary를 보는 호출자의 몫.
  6. `gold_writer` 호출.
  7. staging count, insert/update 건수(§5의 집계 쿼리 결과)를 로그와 결과
     JSON에 남긴다.
  - `build_spark_session()`은 gold_job.py 전용으로 새로 정의한다(기존
    `hourly_comfort_job.py`의 빌더 공유/수정 없음). `spark.sql.session.timeZone=UTC`를
    명시 설정한다 — `F.lit(as_of)` 바인딩이 세션 타임존의 영향을 받지 않게
    하기 위함, 다른 job 빌더들의 기존 관례와도 동일.
- `comfort_score/gold_writer.py` — §5의 lock/확인/write/검증/MERGE/정리
  전체를 담당.
- `cli.py`에 `load-segment-comfort-score` 서브커맨드 추가.

### Stale 데이터 정책

이번 168h 윈도우에 잡히지 않은 `(segment_id, vehicle_profile_id)`는 옛
`calculated_at`을 단 채 남는다(물리 삭제 없음). 소비자는 `calculated_at`이
일정 기간보다 오래된 행을 stale로 간주해 걸러야 한다. 실제 삭제/아카이빙
정책은 후속 이슈.

### 새 의존성

- `psycopg2-binary` → `services/batch-jobs/pyproject.toml`(MERGE 실행과
  마이그레이션 실행기 양쪽에서 재사용, 새 의존성은 한 번만 추가). *각주:
  배포 환경에서는 정적 링크 libpq 이슈로 비권장 — 운영 배포 전
  `psycopg[binary]`(psycopg3) 또는 소스 빌드로 교체를 후속 과제로 남긴다.*
- Postgres JDBC 드라이버(`org.postgresql:postgresql:42.7.4`로 버전 고정)는
  `gold_job.py`의 `build_spark_session()`에서 `spark.jars.packages`로
  지정한다. 운영 배포 이미지에는 미리 구워 넣는 것을 후속 과제로 남긴다
  (런타임 Maven fetch는 Airflow 환경에서 flakiness 원인).

## 알려진 특성 (조치 불필요, 각주로만 남김)

- `calculated_at`이 매 실행 바뀌므로 이번 윈도우에 잡힌 모든 행이 매번
  UPDATE되고, Postgres MVCC 특성상 그 행들이 실행마다 재작성된다(bloat +
  autovacuum 부하). MVP 규모에선 무시 가능.
- JDBC `batchsize` 기본값(1000)은 수십만 행 기준 라운드트립이 많다. 필요시
  10000 정도로 올리는 걸 구현 단계에서 검토한다.
- staging에 UNIQUE 인덱스를 걸지 않고 앱 단에서 검증하는 이유는 §4에
  명시(에러 가독성 트레이드오프).
- 마이그레이션 실행기를 `batch-jobs` 전용 CLI에 두는 것은 현재 이 스키마를
  다루는 서비스가 하나뿐이라는 전제 위의 의도된 단순화다(§2).

## 테스트 전략

- **`batch_jobs.migrate` 단위 테스트**: 새 파일 최초 적용 + 이력 기록,
  이미 적용된 파일 재실행 시 스킵(체크섬 동일), **적용된 파일 내용이
  바뀌면 하드 실패**하는지를 검증한다. DB 연결은 fake/in-memory로 대체.
- **Gold Job 단위 테스트**: MERGE SQL 문자열 생성, `calculated_at`이
  `as_of` 리터럴로 고정되는지, `as_of`가 naive면 하드 실패하는지, config
  파싱, **중복 키 입력 시 하드 실패하는지** — formula.py는 구조적으로
  중복을 안 내놓기 때문에 이 케이스는 `gold_writer`의 검증 함수에 손으로
  만든 중복 staging 데이터를 직접 넣어 돌린다(job 전체를 통해서가 아님),
  **빈 DataFrame 입력 시 summary가 `merged_count=0`으로 나오는지**를
  검증한다. psycopg2/Spark 세션은 mock/fake로 대체한다.
- **통합 테스트**: 로컬 Postgres(`make up-postgres`)에 실제로 붙어 (1)
  `migrate-database` 실행 후 실제 스키마 생성 + sentinel 행 확인, (2) 최초
  적재, (3) 같은 조합의 값이 바뀐 뒤 재실행 시 행 수는 그대로고 값만
  갱신되는지, (4) FK 제약, (5) staging 존재/타입 확인 로직이 실제로
  걸리는지(마이그레이션 안 돌린 상태 시뮬레이션), (6) **동시 실행 시 두
  번째 호출이 advisory lock 획득 실패로 즉시 실패하는지**, (7) **MERGE가
  진행되는 동안(큰 staging 데이터로 트랜잭션을 인위적으로 길게 만든 뒤)
  별도 커넥션의 일반 `SELECT * FROM segment_comfort_score`가 차단되지 않고
  즉시 반환되는지**(읽기 가용성 보장, 위 §5 참고)를 확인한다.
  - **스킵 정책**: `RUN_INTEGRATION` 미설정 → skip(로컬 편의).
    `RUN_INTEGRATION=1`인데 접속 실패 → skip이 아니라 fail.

## 제외 범위

- Gold 데이터를 Serving Store로 옮기는 작업 (`gold-loader` 서비스 영역)
- Gold 전용 Postgres 인스턴스 신설 (당장은 Airflow용 Postgres를 함께 사용)
- 속도 구간(speed band) 컬럼 정의
- Airflow DAG로 이 Job을 스케줄링하는 것
- `data_period_start`/`data_period_end`/`reference_date` 컬럼 (§1 참고)
- `road_segment` FK, stale 행 물리 삭제, `score_version`을 포함한 PK 확장,
  운영 배포용 JDBC 드라이버 사전 번들링, `comfort_score.yaml` ↔ seed
  가중치 자동 동기화, 마이그레이션 실행기의 다중 서비스 공유화 (모두 후속
  이슈)
