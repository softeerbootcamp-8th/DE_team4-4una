"""Offline performance collection and reporting for the comfort-score pipelines.

`services/*`가 아니라 `tools/`에 두는 이유는 런타임 경로가 아니기 때문이다. 이
패키지는 배포되지 않고, 사람이 손으로 돌려 베이스라인 리포트를 만든다(#460, #462).
"""

__version__ = "0.1.0"
