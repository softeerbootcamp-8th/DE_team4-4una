# Architecture Decision Records

아키텍처 결정이 확정될 때마다 `NNNN-결정-제목.md` 형식으로 문서를 추가합니다.

예: `0001-uv-workspace.md`

## 작성 방법

[TEMPLATE.md](TEMPLATE.md)을 사용합니다.

## 상태

| status | 의미 |
| --- | --- |
| `proposed` | 제안되었으나 아직 승인되지 않은 결정 |
| `accepted` | 승인되어 현재 유효한 결정 |
| `superseded` | 이후 ADR로 대체된 결정 |

## 결정을 번복할 때

기존 ADR을 삭제하지 않습니다. 이력을 추적할 수 있도록 다음과 같이 연결합니다.

1. 기존 ADR의 `status`를 `superseded`로 바꾸고 `superseded_by`에 새 ADR 번호를 적습니다.
2. 새 ADR의 `supersedes`에 기존 ADR 번호를 적습니다.
