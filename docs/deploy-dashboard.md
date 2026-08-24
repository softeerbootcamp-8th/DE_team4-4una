# Dashboard 배포

`develop`에 머지된 커밋을 Project EC2의 dashboard 컨테이너로 배포하는 절차와 사전
조건을 정리한다. 파이프라인은
[.github/workflows/deploy-dashboard.yml](../.github/workflows/deploy-dashboard.yml)과
[services/dashboard/deploy/deploy_on_instance.sh](../services/dashboard/deploy/deploy_on_instance.sh)
두 파일로 구성된다.

AWS OIDC provider, 배포 Role, EC2 인스턴스 자체의 계정 단위 설정은
[docs/deploy-serving-api.md](deploy-serving-api.md#aws-사전-준비)에서 이미 다뤘고
같은 계정·같은 인스턴스를 재사용한다. 이 문서는 dashboard에서 추가로 필요한 부분만
적는다.

## 흐름

```
develop에 머지 (경로 감지) → repository variables 확인
  → 이미지 빌드, ECR push (태그 = commit SHA)
  → SSH로 EC2에 배포 스크립트 전송
       인스턴스: pull → 컨테이너 교체 → /_stcore/health 확인
                 성공하면 현재와 직전 이미지만 남기고 정리
                 실패하면 이전 이미지로 되돌리고 실패 처리
  → job summary에 commit, image, host, url 기록
```

독립 워크플로다. `develop` push 중 아래 경로가 바뀌었을 때만 실행되고, Actions
탭에서 `Run workflow`로 수동 실행할 수도 있다.

```
services/dashboard/**   libs/de4-core/**   pyproject.toml   uv.lock
.github/workflows/deploy-dashboard.yml
```

**CI를 기다리지 않는다.** `develop` 병합은 branch protection의 required status
check(`CI Passed`)을 통과해야만 가능하므로, `develop`에 올라온 시점에 이미 검증된
커밋이다.

## GitHub 설정

`Settings > Secrets and variables > Actions`에서 등록한다.

### Variables — 필수

| 변수 | 예시 | 비고 |
| --- | --- | --- |
| `AWS_REGION`, `AWS_DEPLOY_ROLE_ARN`, `EC2_HOST` | — | 다른 배포와 공유 (같은 계정, 같은 인스턴스) |
| `DASHBOARD_ECR_REPOSITORY` | `de4/dashboard` | **리포지토리 이름만.** 전체 URI를 넣으면 push가 실패한다 |
| `DASHBOARD_ROAD_SEGMENT_S3_URI` | 아래 참고 | `s3://`로 시작해야 한다 (워크플로가 검사) |

Secret은 `EC2_SSH_PRIVATE_KEY` 하나이고 다른 배포와 공유한다.

### Variables — 선택

| 변수 | 기본값 | 비고 |
| --- | --- | --- |
| `DASHBOARD_ZONE_MASTER_S3_URI` | 없음 | 비우면 borough 필터가 뜨지 않는다. 사실상 필수 |
| `DASHBOARD_SERVING_API_URL` | `http://localhost:8000` | 아래 "네트워크" 참고 |
| `DASHBOARD_CONTAINER_NAME` | `dashboard` | |
| `EC2_USER` | `ec2-user` | |

### S3 URI는 객체 하나를 가리킨다

`DASHBOARD_ROAD_SEGMENT_S3_URI`와 `DASHBOARD_ZONE_MASTER_S3_URI`는 디렉터리나
프리픽스가 아니라 **Parquet 파일 하나**를 정확히 가리켜야 한다.

```
✅ s3://<reference-bucket>/normalized/road_segment/snapshot_date=2026-08-24/build_id=<id>/part-00000.parquet
❌ s3://<reference-bucket>/normalized/road_segment/
```

[road_geometry.py](../services/dashboard/src/dashboard/road_geometry.py)의
`load_road_segments`가 `list_objects`로 디렉터리를 훑지 않고 `get_object`로 키
하나를 읽은 뒤 그대로 파싱하기 때문이다. `parse_s3_uri`는 key가 비어 있으면
`ValueError`를 낸다. Hive 파티션으로 쪼개진 데이터셋이 아니라 파일 하나로 합쳐진
스냅샷이어야 한다.

Airflow의 `CURRENT_SCORE_ROAD_SEGMENT_URI`는 같은 데이터의 **루트 프리픽스**를
가리킨다. 형식이 다르므로 그 값을 그대로 복사하면 안 된다.

실제 경로는 이렇게 찾는다.

```bash
aws s3 ls s3://<reference-bucket>/normalized/road_segment/ --recursive \
  | grep '\.parquet$' | tail -5
```

> 스냅샷을 갱신할 때마다 이 변수를 손으로 바꾸고 재배포해야 한다.
> `snapshot_date`와 `build_id`가 경로에 박혀 있기 때문이다.

## env 파일을 쓰지 않는다

serving-api는 DB 접속 정보 때문에 인스턴스의 `/etc/serving-api/serving-api.env`를
읽지만, dashboard는 그런 파일이 없다. 필요한 설정이 S3 URI와 내부 주소뿐이라
비밀값이 하나도 없어서다. 값은 repository variables에서 내려와 `docker run --env`로
직접 꽂힌다. **인스턴스에 미리 만들어 둘 파일이 없다.**

S3 접근은 컨테이너가 인스턴스 role로 처리한다. AWS 자격증명을 컨테이너에 넘기지
않는다 (stream-processor와 같은 방식).

## 네트워크

컨테이너를 `--network host`로 띄운다. 이유가 둘이다.

**1. Serving API에 붙기 위해서.** serving-api가 같은 EC2에 8000으로 publish돼
있는데, 기본 브리지 네트워크에서는 `localhost`가 dashboard 컨테이너 자신을 가리켜
못 붙는다. 호스트 네트워크를 쓰면 `localhost:8000`이 그대로 serving-api다.

여기서 `localhost`는 **EC2 자신**이지 사용자의 브라우저가 아니다. Streamlit은 서버에서
렌더링하고 API 호출도 서버 안의 `httpx`가 하므로, 브라우저는 이 주소를 알지 못한다.

```
사용자 브라우저 ──▶ EC2:8501 (Streamlit)
                      └──▶ localhost:8000 (serving-api)   ← EC2 안에서 일어남
```

덕분에 serving-api의 8000을 외부에 열 필요가 없고, 지도 한 장에 수백 번 나가는
요청이 전부 루프백으로 처리된다.

**2. 포트 publish가 필요 없다.**
[dashboard/\_\_init\_\_.py](../services/dashboard/src/dashboard/__init__.py)가
`0.0.0.0:8501`에 고정으로 바인딩하므로 호스트 네트워크에서는 그대로 호스트의
8501이 된다. 포트를 바꾸려면 앱 코드를 함께 고쳐야 한다.

serving-api를 다른 인스턴스로 옮기면 `DASHBOARD_SERVING_API_URL`을 그 주소로
설정한다.

## AWS 사전 준비

1. **ECR 리포지토리**를 `DASHBOARD_ECR_REPOSITORY` 이름으로 생성한다. serving-api
   문서의 lifecycle policy(untagged 1일 후 삭제, 태그 10개만 유지)를 동일하게 붙인다.
2. **배포 Role의 ECR push 권한**과 **인스턴스 프로파일의 ECR pull 권한**에 이
   리포지토리 ARN을 추가한다.
3. **인스턴스 프로파일에 S3 읽기 권한**을 확인한다. 컨테이너가 인스턴스 role로
   `road_segment`/`zone_master` 객체를 읽는다.

## EC2 사전 조건

serving-api/stream-processor와 같은 인스턴스를 재사용하므로 docker, AWS CLI, curl은
이미 준비돼 있다. 추가로 필요한 것은 하나다.

- **보안그룹 8501번 인바운드** — 사용자가 브라우저로 접속한다. 접근 범위는 팀 정책에
  맞춰 좁힌다.

serving-api의 8000은 외부에 열지 않아도 된다. 위 "네트워크" 참고.

## 배포 동작

컨테이너를 지우고 새로 띄우는 recreate 방식이다. serving-api와 같은 구조이고,
무거운 pull을 컨테이너를 지우기 전에 해서 중단 구간을 앱 부팅 시간으로 줄인다.

### health check의 한계

`/_stcore/health`는 Streamlit이 제공하는 엔드포인트로, **서버가 스크립트를 받을
준비가 됐는지만** 본다. serving-api의 `/health`가 DB 접속까지 확인하는 것과 다르다.

S3를 못 읽거나 Serving API에 못 닿아도 health는 통과한다. 두 곳 모두 사용자가 화면을
열 때 처음 접근하므로 기동 시점에는 판정할 수 없다. 따라서 **배포 성공이 화면이
정상이라는 뜻은 아니다.** 배포 후 한 번 열어보는 것을 권한다.

기본 타임아웃은 90초다. 통과하지 못하면 이전 이미지로 되돌리고 워크플로를 실패로
끝낸다. 첫 배포에는 되돌릴 이미지가 없어 그대로 실패한다.

### 이미지 정리

health를 통과한 뒤 **현재 이미지와 rollback용 직전 이미지만 남기고** 이 리포지토리의
나머지 태그를 지운다. `--filter reference=<repo>:*`로 대상을 한정하므로 같은 EC2의
Kafka, Airflow, exporter 이미지는 건드리지 않는다.

직전 이미지를 남기는 것은 자동 rollback 때문이 아니다. 다음 배포의 rollback 대상은
언제나 "현재 이미지"라 그것만 있으면 된다. 직전 이미지는 되돌린 것마저 정상이 아닐
때 수동으로 한 단계 더 내려가기 위한 여지다.

## 실패했을 때

| 증상 | 원인 |
| --- | --- |
| `repository variables가 비어 있습니다` | 위 필수 variables 미설정 |
| `... 은 s3:// 로 시작해야 합니다` | S3 URI 형식 오류 |
| 자격증명 획득 실패 | 배포 Role trust policy의 `sub`가 `develop`이 아님 |
| `docker push` 실패 | ECR 리포지토리 미생성, Role 권한 누락, 또는 변수에 전체 URI를 넣음 |
| `docker pull` 실패 | 인스턴스 프로파일의 ECR pull 권한에 이 리포지토리 누락 |
| health 실패 후 rollback | 컨테이너 로그를 본다. `DASHBOARD_ROAD_SEGMENT_S3_URI` 미설정이면 `config.py`가 기동 시점에 죽는다 |
| 화면은 뜨는데 지도가 비어 있음 | health는 통과한 상태다. S3 객체 경로 또는 인스턴스 role의 S3 권한 확인 |
| 화면은 뜨는데 점수가 안 나옴 | `DASHBOARD_SERVING_API_URL` 확인. serving-api가 같은 EC2에 떠 있는지 본다 |
| borough 필터가 없음 | `DASHBOARD_ZONE_MASTER_S3_URI` 미설정 |
| 브라우저 접속 불가 | 보안그룹 8501번 인바운드 없음 |
