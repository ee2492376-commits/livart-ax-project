"""
Livart 시장 데이터 수집 → Postgres(dbt) raw 적재 (ADF Copy Activity 유사 패턴)
"""
from __future__ import annotations
from airflow.operators.bash import BashOperator

from datetime import datetime
from typing import Final

import pandas as pd
import yfinance as yf
from airflow.decorators import dag, task
from sqlalchemy import bindparam, create_engine, text

DB_URL: Final[str] = "postgresql+psycopg2://airflow:airflow@postgres:5432/dbt"
SYMBOLS: Final[tuple[str, ...]] = ("079430.KS", "KRW=X")


def _history_to_frame(symbol: str) -> pd.DataFrame:
    """yfinance 일봉(최근 1년) → 표준 컬럼 DataFrame."""
    t = yf.Ticker(symbol)
    hist = t.history(period="1y", interval="1d", auto_adjust=False)  
    
    if hist is None or hist.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
            ]
        )

    df = hist.reset_index()
    date_col = df.columns[0]
    df = df.rename(columns={date_col: "trade_datetime"})
    df["trade_date"] = pd.to_datetime(df["trade_datetime"]).dt.normalize().dt.date
    df["symbol"] = symbol

    colmap = {c.lower(): c for c in df.columns}

    def pick(*names: str) -> pd.Series:
        for n in names:
            if n in colmap:
                return df[colmap[n]]
        return pd.Series(pd.NA, index=df.index)

    out = pd.DataFrame(
        {
            "trade_date": df["trade_date"],
            "symbol": df["symbol"],
            "open": pick("open"),
            "high": pick("high"),
            "low": pick("low"),
            "close": pick("close"),
            "adj_close": pick("adj close", "adj_close"),
            "volume": pick("volume"),
        }
    )
    if out["adj_close"].isna().all():
        out["adj_close"] = out["close"]

    numeric = ["open", "high", "low", "close", "adj_close", "volume"]
    out[numeric] = out[numeric].apply(pd.to_numeric, errors="coerce")
    out["volume"] = out["volume"].round().astype("Int64")

    return out[["trade_date", "symbol", "open", "high", "low", "close", "adj_close", "volume"]]


@dag(
    dag_id="livart_ingestion_pipeline",
    schedule=None,  # 한번만 실행
    start_date=datetime(2025, 1, 1), 
    catchup=False,
    tags=["livart", "ingestion", "market"],
    doc_md="현대리바트(005240.KS) 및 USD/KRW(KRW=X) 일봉 1년치를 수집해 `dbt.raw_market_data`에 적재합니다.",
)
def livart_ingestion_pipeline():
    @task(task_id="extract_load_market_data")
    def extract_load_market_data() -> int:
        frames = [_history_to_frame(s) for s in SYMBOLS]
        
        combined = pd.concat(frames, ignore_index=True)
        if combined.empty:
            return 0

        engine = create_engine(DB_URL, pool_pre_ping=True)
        ddl = text(
            """
            CREATE TABLE IF NOT EXISTS raw_market_data (
                trade_date DATE NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                open NUMERIC(18, 6),
                high NUMERIC(18, 6),
                low NUMERIC(18, 6),
                close NUMERIC(18, 6),
                adj_close NUMERIC(18, 6),
                volume BIGINT,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (trade_date, symbol)
            );
            """
        )
        with engine.begin() as conn:
            conn.execute(ddl)
            delete_stmt = text(
                "DELETE FROM raw_market_data WHERE symbol IN :symbols"
            ).bindparams(bindparam("symbols", expanding=True))
            conn.execute(delete_stmt, {"symbols": list(SYMBOLS)})
        combined.to_sql(
            "raw_market_data",
            engine,
            if_exists="append",
            index=False,
            chunksize=500,
        )
        return len(combined)

    extract_task = extract_load_market_data()

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project",
    )
    
    extract_task >> run_dbt_models
    


livart_ingestion_pipeline()
