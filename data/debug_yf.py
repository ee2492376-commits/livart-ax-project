"""
yfinance 수집 디버그 (DAG livart_ingestion_dag.py 와 동일 파라미터)

컨테이너에서 실행:
  docker compose exec airflow-scheduler python /opt/airflow/data/debug_yf.py
"""
from __future__ import annotations

import yfinance as yf

# DAG와 동일
SYMBOLS =  ("079430.KS", "KRW=X") # X는 외환/통화 인스트루먼트 
# -- "005240.KS"는 없음음
PERIOD = "1y"
INTERVAL = "1d"
AUTO_ADJUST = False


def main() -> None:
    for symbol in SYMBOLS:
        print("=" * 60)
        print(f"symbol={symbol!r}  period={PERIOD!r}  interval={INTERVAL!r}  auto_adjust={AUTO_ADJUST}")
        t = yf.Ticker(symbol)
        hist = t.history(period=PERIOD, interval=INTERVAL, auto_adjust=AUTO_ADJUST)
        print(hist)
        print("*" * 60)
        if hist is None or hist.empty:
            print("(empty)")
            continue

        colmap = {c.lower(): c for c in hist.columns}
        print(colmap)
        # print("columns:", list(hist.columns))
        # print("shape:", hist.shape)
        # print("head:\n", hist.head(3))
        # print("tail:\n", hist.tail(3))
    print("=" * 60)
    print("done")


if __name__ == "__main__":
    main()
