# AGENTS.md

이 문서는 저장소에서 작업하는 모든 AI 도구의 공통 지침입니다. 이 파일만 수정하고,
도구별 지침 파일에서는 이 문서를 참조합니다.

## 기본 원칙

- 비밀키·API 키 등 민감 정보는 어떤 경로로도 외부에 노출되지 않도록 한다.
- 루트는 배포 패키지가 아닌 워크스페이스 관리용 프로젝트다.
- 공통 데이터 모델과 서비스 간 계약은 `libs/de4-core`에서 관리한다.
- 각 `services/*` 디렉터리는 독립 실행·테스트가 가능한 워크스페이스 멤버로 유지한다.

## 디렉터리 구조

전체 구조와 데이터 흐름은 [context/architecture.md](context/architecture.md)를 참고한다.
다른 `services/*`와 `libs/de4-core`의 코드는 참고용으로 자유롭게 읽는다. 다만 현재
작업으로 요청받은 서비스 디렉터리 외의 코드는 수정하지 않는다. 여러 서비스에 걸친
변경이 필요하면 어떤 서비스가 왜 영향을 받는지 먼저 설명하고 승인을 받는다.

## 프로젝트 컨텍스트

- 작업을 시작하기 전에 `context/README.md`를 읽고, 작업 유형에 맞는 문서를 확인한다.
- `context/manifest.yaml`은 컨텍스트 문서의 색인이다.
- 확정된 요구사항, 제안, 미해결 사항을 구분한다. `context/open-questions.md`의
  미해결 사항을 임의로 확정하지 않는다.
- 컨텍스트 문서는 실행 가능한 계약을 복제하지 않는다. 데이터 모델과 서비스 간
  계약의 최종 정의는 `libs/de4-core`에서 관리하고 컨텍스트에서 이를 참조한다.
- 요구사항이나 아키텍처가 바뀌면 관련 코드와 함께 `context/`를 갱신한다.
- 다음 중 하나에 해당하면 ADR 작성을 제안한다.
  - 사용자가 ADR 작성을 요청할 때
  - `context/open-questions.md`의 미해결 질문이 해결되고 여러 컴포넌트에 영향을 줄 때
  - 결정이 아키텍처적 파급효과를 가질 때
  - 파일·테이블 포맷, 서빙 데이터베이스, AWS 서비스 매핑처럼 미확정 사항을 확정할 때
- ADR을 임의로 생성하지 않는다. [docs/adr/TEMPLATE.md](docs/adr/TEMPLATE.md)에 맞춰
  배경·결정·대안·결과를 정리해 제시하고 작성 여부를 사용자에게 확인한다. 승인을
  받은 뒤 [docs/adr/README.md](docs/adr/README.md)의 규칙에 따라 작성한다.

## 버전 및 의존성 관리

- Python 3.12와 `uv`를 사용한다.
- 새 의존성이 필요하면 패키지명과 추가 이유를 먼저 설명한다.
- 의존성은 해당 `services/<name>/pyproject.toml` 또는 `libs/de4-core/pyproject.toml`에 선언한다.
- 의존성을 변경한 후에는 루트의 `uv.lock`을 갱신한다 (`uv lock` 또는 `uv sync --all-packages`).
- `uv.lock`은 uv가 생성하는 파일이므로 직접 편집하지 않는다.

## 코드 스타일

코드 스타일은 [docs/code-style.md](docs/code-style.md)를 따른다 (PEP 8 기반).

## Git 및 GitHub

브랜치명, 커밋 메시지, PR 작성 규칙은 [CONTRIBUTING.md](CONTRIBUTING.md)를 따른다.

## 수정 및 실행 금지 사항

### 🚫 Never do

- `.env`와 실제 비밀값이 포함된 파일을 읽거나 수정하지 않는다.
- 비밀값을 코드, 로그, 테스트 데이터 또는 `.env.example`에 기록하지 않는다.
- 적용된 데이터베이스 마이그레이션 파일을 직접 수정하지 않는다.
- 데이터베이스 테이블이나 데이터를 임의로 삭제하지 않는다 (`DROP TABLE`, `TRUNCATE` 등).
- 담당 영역 밖의 코드나 설정을 불필요하게 수정하지 않는다.
- 원인을 파악하지 못한 상태에서 추측성 수정을 하지 않는다.
- 실패하는 테스트를 원인 파악 없이 삭제하거나 skip 처리하지 않는다.
- 데이터베이스 스키마를 마이그레이션 없이 직접 변경하지 않는다. 스키마 변경은
  마이그레이션 파일로 작성하고, 실행은 아래 Ask first 절차를 따른다.

### ⚠️ Ask first

- 운영 환경 배포 또는 데이터베이스 마이그레이션 실행
- `terraform apply` 또는 `terraform destroy` 실행

## 검증

변경 범위에 맞춰 저장소 루트에서 다음 명령을 실행한다.

```bash
uv sync --all-packages
uv run --all-packages ruff check .
uv run --all-packages pytest
```

`services/batch-jobs`의 Spark 테스트는 **JDK 21**이 필요하다. JDK 24 이상에서는
`Subject.getSubject`가 막혀 SparkSession 생성 단계에서
`UnsupportedOperationException: getSubject is not supported`로 실패한다. JDK 21을
설치하고(`brew install openjdk@21`) `JAVA_HOME`을 지정한 뒤 실행한다.

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --all-packages pytest
```

## 테스트 및 빌드

🚧 향후 작성 예정
