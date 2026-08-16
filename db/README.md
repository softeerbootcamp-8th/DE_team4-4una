# db

실행에 필요한 SQL 마이그레이션은 설치된 wheel에서도 사용할 수 있도록
`services/batch-jobs/src/batch_jobs/resources/migrations`에 포함한다. 실행 로직은
같은 서비스의 `batch_jobs.migrate` 모듈에 있다.

번호 붙인 SQL 파일(`NNNN_설명.sql`)은 파일명 순서대로 한 번씩만 적용되고,
적용 이력은 DB 안의 `schema_migrations` 테이블에 기록된다.

**이미 적용된 파일은 절대 수정하지 않는다.** 스키마를 바꾸려면 새 번호의
파일을 추가한다 — 내용이 바뀐 걸 실행기가 체크섬으로 감지해 하드 실패한다.

## 실행

```bash
make migrate
```

내부적으로 `uv run --package batch-jobs batch-jobs migrate-database`를
실행한다. 접속 정보는 저장소 루트 `.env`의 `POSTGRES_HOST`/`PORT`/`DB`/
`USER`/`PASSWORD`를 사용한다.
