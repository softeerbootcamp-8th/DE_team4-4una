# Contributing Guide

이 문서는 프로젝트 개발 진행 시 지켜야 할 브랜치 전략, 커밋 메시지 컨벤션, Pull Request(PR) 규칙을 정의합니다.

## 1. 기본 원칙

- 모든 작업은 GitHub Issue를 먼저 생성한 후 시작합니다.
- 하나의 이슈는 하나의 작업 브랜치에서 처리합니다.
- `main`과 `develop` 브랜치에는 직접 push하지 않습니다.
- 모든 변경 사항은 PR과 코드 리뷰를 거쳐 병합합니다.
- 하나의 PR에는 하나의 목적에 해당하는 변경만 포함합니다.

## 2. 브랜치 전략

### 브랜치 역할

| 브랜치 | 역할 | 분기 기준 | 병합 대상 |
| --- | --- | --- | --- |
| `main` | 배포 가능한 안정 버전을 관리합니다. | - | - |
| `develop` | 다음 배포를 위한 변경 사항을 통합합니다. | `main` | `main` |
| 작업 브랜치 | 이슈 단위 작업을 수행합니다. | `develop` | `develop` |
| `hotfix/*` | 운영 환경의 긴급 문제를 수정합니다. | `main` | `main`, 이후 `develop`에 반영 |

### 작업 브랜치 이름

작업 브랜치는 다음 형식으로 작성합니다.

```text
<type>/<issue-number>-<short-description>
```

- `type`은 커밋 메시지에서 사용하는 type과 동일하게 작성합니다.
- `issue-number`에는 GitHub Issue 번호를 작성합니다.
- `short-description`은 작업 내용을 나타내는 짧은 영문 kebab-case로 작성합니다.

예시:

```text
feat/12-add-tlc-ingestion
fix/18-prevent-duplicate-records
refactor/23-optimize-spark-join
test/27-add-data-quality-check
docs/31-write-data-contract
```

### 작업 흐름

1. 작업할 GitHub Issue를 생성하거나 할당받습니다.
2. 최신 `develop` 브랜치에서 작업 브랜치를 생성합니다.
3. 작업 내용을 커밋하고 원격 저장소에 push합니다.
4. 작업 브랜치에서 `develop`을 대상으로 PR을 생성합니다.
5. CI, 리뷰 및 필수 검증을 통과한 후 Squash merge합니다.
6. 병합이 끝난 작업 브랜치는 삭제합니다.

```bash
git switch develop
git pull origin develop
git switch -c feat/12-add-tlc-ingestion
```

### Hotfix 흐름

운영 환경의 긴급 수정이 필요한 경우에만 사용합니다.

1. `main`에서 `hotfix/<issue-number>-<short-description>` 브랜치를 생성합니다.
2. 수정 후 `main`을 대상으로 PR을 생성합니다.
3. 리뷰와 검증을 거쳐 Squash merge합니다.
4. 수정 사항을 `develop`에도 즉시 반영합니다.

## 3. 커밋 메시지 컨벤션

### 기본 형식

커밋 메시지는 scope 없이 다음 형식으로 작성합니다.

```text
<type>: <subject>

<body>

<footer>
```

`body`와 `footer`는 필요한 경우에만 작성합니다.

### Type

| Type | 설명 |
| --- | --- |
| `feat` | 새로운 기능이나 데이터 파이프라인을 추가합니다. |
| `fix` | 버그 또는 잘못된 데이터 처리 로직을 수정합니다. |
| `docs` | 문서만 변경합니다. |
| `style` | 코드 동작에 영향을 주지 않는 포맷을 변경합니다. |
| `refactor` | 기능 변경 없이 코드 구조를 개선합니다. |
| `perf` | 실행 속도, 메모리, 쿼리 등 성능을 개선합니다. |
| `test` | 테스트 또는 데이터 품질 검증을 추가·수정합니다. |
| `build` | 빌드 시스템이나 외부 의존성을 변경합니다. |
| `ci` | CI/CD 설정과 스크립트를 변경합니다. |
| `chore` | 그 밖의 유지보수 작업을 수행합니다. |
| `revert` | 이전 커밋을 되돌립니다. |

### Subject

- 변경 내용을 명확하고 간결하게 작성합니다.
- 영어 소문자로 작성합니다.
- 명령형 현재 시제를 사용합니다. 과거형이나 3인칭 단수형 대신 `add`, `change`, `fix`와 같은 동사 원형으로 시작합니다.
- 50자 이내 작성을 권장합니다.
- 문장 끝에 마침표를 사용하지 않습니다.
- 하나의 커밋에는 하나의 논리적 변경만 포함합니다.

예시:

```text
feat: add tlc data ingestion pipeline
fix: prevent duplicate segment records
test: validate ride comfort score range
docs: add data contract guide
```

### Body

- 변경한 내용과 함께 변경한 이유를 설명합니다.
- 필요한 경우 기존 동작과 변경 후 동작의 차이를 설명합니다.
- 약 72자를 기준으로 줄바꿈합니다.

```text
fix: prevent duplicate segment records

파이프라인 재실행 시 동일 데이터가 중복되지 않도록
trip_id와 segment_id를 기준으로 멱등성을 보장한다.
```

### Footer

관련 이슈를 참조할 때 다음 형식을 사용합니다.

```text
Refs #12
Closes #18
```

- `Refs`는 관련 이슈를 참조할 때 사용합니다.
- `Closes`는 변경 사항이 기본 브랜치에 반영될 때 종료할 이슈에 사용합니다.
- 하위 호환성이 깨지는 변경은 `BREAKING CHANGE:`로 시작하여 영향과 마이그레이션 방법을 작성합니다.

```text
BREAKING CHANGE: 승차감 점수의 범위를 1~10에서 0~1로 변경한다.

기존 데이터를 사용하는 작업은 점수를 10으로 나누어 변환해야 한다.
```

### Revert

이전 커밋을 되돌릴 때는 되돌릴 커밋 제목을 작성하고, 본문에 커밋 해시를 명시합니다.

- Revert를 진행하기 전에 대상 커밋과 영향 범위를 팀원에게 고지합니다.
- 긴급한 장애 대응으로 사전 고지가 어려운 경우에는 Revert 직후 변경 내용과 사유를 팀원에게 공유합니다.

```text
revert: add tlc data ingestion pipeline

This reverts commit <commit-hash>.
```

## 4. Pull Request 규칙

### PR 생성

- 작업 브랜치의 PR 대상은 원칙적으로 `develop`입니다.
- PR 제목은 커밋 메시지와 동일하게 `<type>: <subject>` 형식으로 작성합니다.
- 제목은 영어 소문자 명령문으로 명확하고 간결하게 작성합니다.
- 아직 리뷰할 준비가 되지 않았다면 Draft PR로 생성합니다.
- PR 본문에 연관된 이슈와 작업 내용을 작성합니다.
- PR 본문의 마지막 줄에는 `Closes #이슈번호`를 작성합니다.
- 리뷰 가능한 크기로 유지하고 서로 다른 목적의 변경은 PR을 분리합니다.

PR 제목 예시:

```text
feat: add tlc data ingestion pipeline
```

### PR 본문 필수 내용

PR에는 최소한 다음 내용을 포함합니다.

1. 체크리스트 형식의 작업 내용
2. 본문 마지막 줄에 `Closes #이슈번호`

리뷰 요구사항과 참고 사항은 필요한 경우에만 작성합니다.

`develop`이 기본 브랜치가 아니라면 PR을 `develop`에 병합해도 GitHub Issue가 자동으로 종료되지 않을 수 있습니다. 이 경우 PR 병합 후 이슈 상태를 확인하고 수동으로 종료합니다.

### 리뷰 및 병합 조건

다음 조건을 모두 만족한 후 병합합니다.

- 최소 1명 이상의 승인을 받았습니다.
- 리뷰 의견과 대화가 모두 해결되었습니다.
- CI와 필수 테스트가 통과했습니다.
- 충돌이 없고 최신 `develop`의 변경 사항을 반영했습니다.
- 작성자는 자신의 PR을 직접 승인하지 않습니다.

## 5. 머지 전략

### 병합 방식

- 작업 브랜치에서 `develop`으로 병합할 때는 **Squash merge**를 사용합니다.
- Squash merge 시 최종 커밋 제목은 `<type>: <subject>` 형식의 커밋 컨벤션에 맞게 수정합니다.
- 배포 또는 릴리스 시 `develop`에서 `main`으로 병합할 때는 **Merge commit**을 사용합니다.
- 병합이 끝난 작업 브랜치는 삭제합니다.
- `main`은 항상 배포 가능한 상태를 유지합니다.

### 금지 사항

- `main` 또는 `develop`에 직접 push하지 않습니다.
- 보호 브랜치에 force push하지 않습니다.
- 다른 팀원이 함께 사용하는 브랜치를 임의로 rebase하지 않습니다.
- 충돌 내용을 확인하지 않고 `ours` 또는 `theirs`의 변경 사항을 일괄 적용하지 않습니다.
- 리뷰와 필수 검증을 생략하고 병합하지 않습니다.
- 서로 관련 없는 여러 이슈를 하나의 PR에 포함하지 않습니다.
