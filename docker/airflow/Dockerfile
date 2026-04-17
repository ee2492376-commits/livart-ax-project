# Apache Airflow 공식 이미지 확장 — 스케줄러/웹서버/태스크에서 공통 사용
ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER airflow

# 추가 패키지(dbt-postgres 등)는 공식 constraints에 없어 별도 설치
RUN pip install --no-cache-dir \
    dbt-postgres \
    anthropic \
    yfinance \
    pandas
