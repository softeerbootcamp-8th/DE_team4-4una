# Serving API 배포

`main`에 머지된 커밋을 EC2의 serving-api 컨테이너로 배포하는 절차와 사전 조건을
정리한다. 파이프라인은 [.github/workflows/deploy-serving-api.yml](../.github/workflows/deploy-serving-api.yml)과
[services/serving-api/deploy/deploy_on_instance.sh](../services/serving-api/deploy/deploy_on_instance.sh)
두 파일로 구성된다.

## 흐름

```
main에 머지 → CI 통과 → 배포 워크플로 시작
  → repository variables 확인
  → 이미지 빌드, ECR push (태그 = commit SHA)
  → SSM으로 EC2에 배포 스크립트 전송
       인스턴스: pull → 컨테이너 교체 → /health 확인
                 실패하면 이전 이미지로 되돌리고 실패 처리
  → job summary에 commit, image, instance, SSM command 기록
```

배포는 다음 세 조건을 모두 만족할 때만 시작한다.

| 조건 | 확인하는 것 |
| --- | --- |
| CI가 성공 | 검증되지 않은 코드가 나가지 않게 한다 |
| 이벤트가 push | `main`을 대상으로 한 PR의 CI로는 배포되지 않게 한다 |
| 브랜치가 main | 다른 브랜치의 CI로는 배포되지 않게 한다 |

배포 대상 커밋은 CI가 검증한 커밋(`workflow_run.head_sha`)이다. `github.sha`는 이
워크플로에서 `main`의 최신 커밋을 가리키므로 쓰지 않는다.

## repository variables

**GitHub 저장소** 설정의 `Settings > Secrets and variables > Actions > Variables`에
등록한다. 값은 AWS 리소스의 식별자이고 비밀값이 아니므로 Secrets가 아니라 Variables를
쓴다. AWS 리소스를 만든 뒤에 채운다.

### 필수

하나라도 비어 있으면 워크플로 첫 스텝이 비어 있는 이름을 알려주고 중단한다.

| 변수 | 예시 |
| --- | --- |
| `AWS_REGION` | `ap-northeast-2` |
| `AWS_DEPLOY_ROLE_ARN` | `arn:aws:iam::123456789012:role/github-actions-deploy` |
| `ECR_REPOSITORY` | `de4/serving-api` |
| `EC2_INSTANCE_ID` | `i-0abc123def4567890` |

`ECR_REPOSITORY`에는 **리포지토리 이름만** 넣는다. 워크플로가
`<레지스트리>/<이 값>:<커밋 SHA>`로 조립하고 레지스트리 주소는 ECR 로그인 스텝이
알려주므로, 전체 URI를 넣으면 주소가 중복되어 push가 실패한다.

### 선택

기본값이 있어 비워두어도 동작한다.

| 변수 | 기본값 |
| --- | --- |
| `SERVING_API_CONTAINER_NAME` | `serving-api` |
| `SERVING_API_PORT` | `8000` |
| `SERVING_API_ENV_FILE` | `/etc/serving-api/serving-api.env` |

## AWS 사전 준비

리소스 생성 자체는 인프라 이슈에서 다룬다. 여기서는 파이프라인이 요구하는 조건만
적는다. 아래 `<...>` 자리는 실제 값으로 바꾼다.

### 1. AWS 계정에 GitHub OIDC provider 등록

GitHub은 워크플로가 돌 때 "이건 저장소 X의 main 브랜치다"라고 서명한 토큰을 발급한다.
GitHub 쪽에 등록할 것은 없고, 워크플로의 `permissions: id-token: write` 한 줄이 전부다.

**그 토큰을 믿겠다는 설정은 AWS 쪽에서 한다.** IAM > Identity providers > Add provider
에서 아래 값으로 등록한다. 계정당 한 번만 하면 되고, 이후 Role을 여러 개 만들어도 이
provider를 재사용한다.

| 항목 | 값 |
| --- | --- |
| Provider type | OpenID Connect |
| Provider URL | `https://token.actions.githubusercontent.com` |
| Audience | `sts.amazonaws.com` |

### 2. 배포 Role (러너가 OIDC로 assume)

trust policy에 브랜치 조건을 반드시 넣는다. 이 조건이 없으면 다른 브랜치나 포크의
워크플로도 이 Role을 쓸 수 있다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
          "token.actions.githubusercontent.com:sub": "repo:softeerbootcamp-8th/DE_team4-4una:ref:refs/heads/main"
        }
      }
    }
  ]
}
```

배포 워크플로는 `workflow_run`으로 실행되어 기본 브랜치(`main`) 컨텍스트로 돌기
때문에 `sub`가 위 값이 된다.

권한 policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EcrAuth",
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Sid": "EcrPush",
      "Effect": "Allow",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage"
      ],
      "Resource": "arn:aws:ecr:<REGION>:<ACCOUNT_ID>:repository/<ECR_REPOSITORY>"
    },
    {
      "Sid": "SsmSendCommand",
      "Effect": "Allow",
      "Action": "ssm:SendCommand",
      "Resource": [
        "arn:aws:ec2:<REGION>:<ACCOUNT_ID>:instance/<EC2_INSTANCE_ID>",
        "arn:aws:ssm:<REGION>::document/AWS-RunShellScript"
      ]
    },
    {
      "Sid": "SsmReadResult",
      "Effect": "Allow",
      "Action": ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations"],
      "Resource": "*"
    }
  ]
}
```

`ssm:SendCommand`는 인스턴스와 document를 **둘 다** 리소스로 요구한다. 하나만 적으면
거부된다. `ssm:GetCommandInvocation`은 리소스 단위 제한을 지원하지 않아 `*`를 쓴다.

### 3. EC2 인스턴스 프로파일

러너의 배포 Role과 **다른 Role**이다. 인스턴스가 이미지를 직접 pull하므로 여기에도
ECR 권한이 필요하다. 이걸 빼면 배포가 `docker pull`에서 실패한다.

- 관리형 정책 `AmazonSSMManagedInstanceCore` — SSM Agent가 SSM과 통신한다
- ECR 읽기 권한

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"
      ],
      "Resource": "*"
    }
  ]
}
```

### 4. ECR 리포지토리

태그가 commit SHA라서 `main`에 머지될 때마다 이미지가 하나씩 쌓이고, ECR은 아무것도
자동으로 지우지 않는다. 리포지토리를 만들 때 lifecycle policy를 함께 붙인다.

```json
{
  "rules": [
    {
      "rulePriority": 1,
      "description": "untagged 이미지는 1일 뒤 삭제",
      "selection": {
        "tagStatus": "untagged",
        "countType": "sinceImagePushed",
        "countUnit": "days",
        "countNumber": 1
      },
      "action": { "type": "expire" }
    },
    {
      "rulePriority": 2,
      "description": "태그된 이미지는 최근 10개만 유지",
      "selection": {
        "tagStatus": "any",
        "countType": "imageCountMoreThan",
        "countNumber": 10
      },
      "action": { "type": "expire" }
    }
  ]
}
```

## EC2 사전 조건

인스턴스는 Amazon Linux 2023, `t4g.large`(Graviton, arm64)다. arm64 이미지가 필요하므로
워크플로는 arm 러너(`runs-on: ubuntu-24.04-arm`)에서 빌드한다.

배포 스크립트가 인스턴스에서 쓰는 것들은 다음과 같다.

| 항목 | 쓰는 곳 | Amazon Linux 2023 |
| --- | --- | --- |
| docker | 컨테이너 교체 | **설치 필요** (`dnf install docker`) |
| AWS CLI | ECR 로그인 (`aws ecr get-login-password`) | 기본 포함 |
| curl | `/health` 확인 | 기본 포함 |
| SSM Agent | 워크플로가 보낸 스크립트 수신 | 기본 포함 |

그 밖에:

- **인스턴스 프로파일** 부착 (위 3번)
- **아웃바운드 443** 허용. Agent가 AWS로 나간다. 인바운드는 열지 않아도 된다
- **env 파일** — 아래 참고

인스턴스가 SSM에 등록됐는지는 아래로 확인한다. 목록에 뜨면 정상이다.

```bash
aws ssm describe-instance-information
```

## env 파일

파이프라인은 비밀값을 다루지 않는다. DB 접속 정보는 이 파일에만 두고, 스크립트는
경로만 컨테이너에 넘긴다. 사람이 인스턴스에 직접 만든다.

기본 경로는 `/etc/serving-api/serving-api.env`이고, 필요한 키는 다음과 같다. 값은
저장소에 기록하지 않는다.

```
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DB=
POSTGRES_USER=
POSTGRES_PASSWORD=
```

`SERVING_API_POOL_MIN_SIZE`, `SERVING_API_POOL_MAX_SIZE`는 선택이다.
`SERVING_API_PORT`는 워크플로가 덮어쓰므로 여기 적어도 무시된다.

DB 비밀번호가 들어가므로 소유자와 권한을 제한한다.

```bash
sudo install -d -m 700 /etc/serving-api
sudo chown root:root /etc/serving-api/serving-api.env
sudo chmod 600 /etc/serving-api/serving-api.env
```

## 배포 동작

컨테이너를 지우고 새로 띄우는 recreate 방식이다. 인스턴스 1대에 컨테이너 1개이고
포트가 하나뿐이라, 구버전을 살려둔 채로 새 컨테이너가 같은 포트를 잡을 수 없다.

```
docker pull <새 이미지>     서비스 정상 (구버전이 계속 응답)
docker rm --force           여기서부터 중단
docker run <새 이미지>       앱 부팅
/health 200                 중단 끝
```

무거운 pull을 컨테이너를 지우기 전에 해서 중단 구간을 앱 부팅 시간으로 줄인다.
무중단(Blue/Green, rolling)은 리버스 프록시나 로드밸런서가 있어야 가능하고 별도
과제다.

`/health`는 DB에 닿지 못하면 503을 돌려주므로, 이 응답 하나로 앱과 DB 상태를 함께
확인한다. 제한 시간(기본 90초) 안에 통과하지 못하면 인스턴스가 이전 이미지로 다시
띄우고 워크플로를 실패로 끝낸다. 첫 배포에는 되돌릴 이미지가 없어 그대로 실패한다.

배포된 컨테이너에는 `org.opencontainers.image.revision` 라벨로 commit SHA가 남는다.

```bash
docker inspect --format \
  '{{index .Config.Labels "org.opencontainers.image.revision"}}' serving-api
```

## 실패했을 때

워크플로 로그에 인스턴스가 남긴 출력이 그대로 찍힌다. 먼저 그것을 본다.

| 증상 | 원인 |
| --- | --- |
| `repository variables가 비어 있습니다` | 위 필수 variables 미설정 |
| 자격증명 획득 실패 | OIDC provider 미등록, trust policy의 `sub` 불일치 |
| `docker push` 실패 | 배포 Role의 ECR 권한, 또는 `ECR_REPOSITORY`에 전체 URI를 넣음 |
| `InvalidInstanceId` | 인스턴스가 SSM에 등록되지 않음 (Agent, 인스턴스 프로파일, 아웃바운드 확인) |
| `exec format error` (컨테이너 로그) | 이미지가 arm64로 빌드되지 않았다 |
| `env 파일이 없습니다` | 인스턴스에 env 파일 미생성, 또는 `SERVING_API_ENV_FILE` 경로 불일치 |
| `docker pull` 실패 | 인스턴스 프로파일의 ECR 권한 누락 |
| health 실패 후 rollback | DB 접속 정보 오류가 흔하다. 함께 출력되는 컨테이너 로그를 본다 |

SSM command ID는 job summary에 남으므로, 필요하면 AWS 콘솔의 Systems Manager >
Run Command에서 같은 실행을 다시 확인할 수 있다.
