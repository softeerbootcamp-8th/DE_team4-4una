# db

이 디렉터리는 순수 `.sql` 데이터만 담는다 — 실행 로직은
`services/batch-jobs`의 `batch_jobs.migrate` 모듈에 있다 (루트는 uv
workspace member가 아니라 여기 직접 의존성/테스트를 둘 수 없기 때문).

## migrations/

번호 붙인 raw SQL 파일(`NNNN_설명.sql`). 파일명 순서대로 한 번씩만
적용되고, 적용 이력은 DB 안의 `schema_migrations` 테이블에 기록된다.

**이미 적용된 파일은 절대 수정하지 않는다.** 스키마를 바꾸려면 새 번호의
파일을 추가한다 — 내용이 바뀐 걸 실행기가 체크섬으로 감지해 하드 실패한다.

## 실행

```bash
make migrate
```

내부적으로 `uv run --package batch-jobs batch-jobs migrate-database`를
실행한다. 접속 정보는 저장소 루트 `.env`의 `POSTGRES_HOST`/`PORT`/`DB`/
`USER`/`PASSWORD`를 사용한다.
