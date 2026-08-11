# 코드 스타일 가이드

Python 코드 스타일은 [PEP 8](https://peps.python.org/pep-0008/)을 따른다. 아래는 해당 가이드
속 주요 내용을 정리한 것이다.

## 들여쓰기

스페이스 4칸을 사용한다. 탭과 스페이스를 섞지 않는다.

```python
# Good
def long_function_name(
        var_one, var_two, var_three,
        var_four):
    print(var_one)

# Bad
def long_function_name(
    var_one, var_two, var_three,
    var_four):
    print(var_one)
```

## 한 줄 길이

한 줄은 99자를 넘지 않는다. 길어지는 경우 괄호를 이용해 줄바꿈한다.

```python
# Good
income = (gross_wages
          + taxable_interest
          + (dividends - qualified_dividends)
          - ira_deduction
          - student_loan_interest)
```

긴 표현식을 여러 줄로 나눌 때는 연산자를 다음 줄 앞에 둔다(`+`, `-` 등이 줄 끝이 아니라 시작에 온다).

## 빈 줄

- 최상위 함수·클래스 정의 사이는 빈 줄 2줄을 둔다.
- 클래스 내부 메서드 사이는 빈 줄 1줄을 둔다.
- 함수 내부에서 논리적으로 구분되는 부분에는 필요한 경우에만 빈 줄 1줄을 둔다.

## Import

- 표준 라이브러리 → 서드파티 → 로컬(프로젝트 내부) 순으로 그룹을 나누고, 그룹 사이에 빈 줄을 둔다.
- 한 줄에 여러 모듈을 import하지 않는다.
- `from module import *` 형태의 와일드카드 import는 사용하지 않는다. 네임스페이스에 어떤 이름이
  들어오는지 알 수 없게 만든다.

```python
# Good
import os
import sys

import pytest

from de4_core.models import Trip

# Bad
import os, sys
from de4_core.models import *
```

## 공백

- 연산자 앞뒤에는 공백 1칸을 둔다: `a + b`, `submitted += 1`.
- 괄호·대괄호·중괄호 바로 안쪽에는 공백을 넣지 않는다: `spam(ham[1], {eggs: 2})`.
- 콤마·세미콜론·콜론 앞에는 공백을 넣지 않는다: `foo(a, b)`, `if x == 4: print(x)`.
- 함수 호출 시 함수명과 괄호 사이에 공백을 넣지 않는다: `spam(1)` (`spam (1)`은 틀림).
- 슬라이스의 콜론은 이항 연산자처럼 취급한다: `ham[1:9]`, `ham[lower + offset : upper + offset]`.
- 키워드 인자·기본값에는 공백을 넣지 않는다: `def f(real, imag=0.0):`. 단, 타입 힌트가 있으면
  양쪽에 공백을 둔다: `def f(real, imag: float = 0.0):`.

## 네이밍

| 대상 | 규칙 | 예시 |
| --- | --- | --- |
| 모듈/패키지 | 소문자, 필요시 언더스코어 | `gold_loader` |
| 클래스 | CapWords | `TripLoader`, `ComfortScore` |
| 함수/변수 | snake_case | `calculate_score()`, `trip_id` |
| 상수 | UPPER_SNAKE_CASE | `MAX_BATCH_SIZE` |
| 내부 전용 | 앞에 언더스코어 1개 | `_helper()` |
| 예외 클래스 | CapWords + `Error` 접미사 | `ValidationError` |

숫자·문자와 헷갈리는 단일 문자 이름(`l`, `O`, `I`)은 변수명으로 사용하지 않는다.

## 문자열 따옴표

홑따옴표(`'`)와 쌍따옴표(`"`)는 동등하게 취급하되, 한 파일 안에서는 일관성 있게 사용한다.
문자열 내부에 따옴표가 포함되어야 하면 불필요한 이스케이프를 피하는 쪽을 선택한다.

```python
# Good
s = "Don't worry"
s = 'Say "hi"'

# Bad (불필요한 이스케이프)
s = 'Don\'t worry'
```

Docstring은 삼중 쌍따옴표(`"""`)를 사용한다.

## 주석과 Docstring

- 블록 주석은 코드와 같은 들여쓰기에 `#` + 공백 1칸으로 시작한다.
- 인라인 주석은 코드와 2칸 이상 띄우고, 코드만으로 알기 어려운 내용에만 사용한다.
- 여러 줄 docstring은 마지막 줄에 요약을 반복하지 않고, 닫는 `"""`은 별도 줄에 둔다.

```python
def complex_func():
    """Return a foobang.

    Optional plotz says to frobnicate the bizbaz first.
    """
```

## 타입 힌트

- 함수 인자·반환값에는 타입 힌트를 붙인다: `def load(path: str) -> pl.DataFrame:`.
- 콜론 앞에는 공백을 넣지 않고, 뒤에는 공백 1칸을 둔다: `name: int`, `code: int` (`code : int`,
  `code:int`는 틀림).
- 변수에 타입 힌트와 기본값을 함께 줄 때는 `=` 양쪽에 공백을 둔다: `result: int = 0`.

## 기타

- 한 줄에 여러 문장을 세미콜론(`;`)이나 콜론(`:`)으로 이어 쓰지 않는다.

  ```python
  # Good
  if foo == "blah":
      do_blah_thing()

  # Bad
  if foo == "blah": do_blah_thing()
  ```

- 단일 요소 튜플에는 후행 쉼표를 명시한다: `FILES = ("setup.cfg",)`.
