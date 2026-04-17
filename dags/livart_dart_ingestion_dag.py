from __future__ import annotations

from datetime import datetime
import io
import json
import os
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

from airflow.decorators import dag, task
from sqlalchemy import create_engine, text

DART_API_KEY = os.getenv("DART_API_KEY", "bb8a84b56787b80cbde595387d2f0263c691e7d3")
TARGET_STOCK_CODE = "079430"  # 현대리바트
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/dbt"
FS_DIV = os.getenv("DART_FS_DIV", "CFS").upper()  # CFS: 연결 / OFS: 별도


def _http_get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get_corp_code_by_stock(stock_code: str, api_key: str) -> tuple[str, str]:
    """
    DART corpCode.xml ZIP에서 stock_code로 corp_code, corp_name 조회
    """
    if not api_key:
        raise ValueError("환경변수 DART_API_KEY가 비어 있습니다.")

    corp_url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
    with urllib.request.urlopen(corp_url, timeout=60) as resp:
        zip_bytes = resp.read()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # ZIP 안 XML 파일명은 보통 CORPCODE.xml
        xml_name = zf.namelist()[0]
        xml_bytes = zf.read(xml_name)

    root = ET.fromstring(xml_bytes)
    for elem in root.findall("list"):
        s_code = (elem.findtext("stock_code") or "").strip()
        if s_code == stock_code:
            corp_code = (elem.findtext("corp_code") or "").strip()
            corp_name = (elem.findtext("corp_name") or "").strip()
            return corp_code, corp_name

    raise ValueError(f"stock_code={stock_code} 에 해당하는 corp_code를 찾지 못했습니다.")


def _fetch_single_account(
    corp_code: str,
    api_key: str,
    bsns_year: str,
    reprt_code: str = "11011",  # 사업보고서
    fs_div: str = "CFS",        # 연결재무제표(CFS) / 별도재무제표(OFS)
) -> list[dict]:
    """
    DART fnlttSinglAcnt.json에서 단일회사 주요계정 조회
    """
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "fs_div": fs_div,
    }
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json?" + urllib.parse.urlencode(params)
    data = _http_get_json(url)
    status = data.get("status")
    if status == "013":
        return []
    if status != "000":
        raise RuntimeError(f"fnlttSinglAcnt API 오류: status={status}, message={data.get('message')}")
    return data.get("list", [])


def _to_statement_type(sj_nm: str) -> str | None:
    if "포괄손익계산서" in sj_nm:
        return "포괄손익계산서"
    if "손익계산서" in sj_nm:
        return "손익계산서"
    return None


def _to_amount(raw: str | None) -> int | None:
    amount_text = (raw or "").replace(",", "").strip()
    if not amount_text or amount_text == "-":
        return None
    try:
        return int(amount_text)
    except ValueError:
        return None


@dag(
    dag_id="livart_dart_financial_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # 수동 실행 (원하면 cron으로 변경)
    catchup=False,
    tags=["dart", "livart", "ingestion"],
)
def livart_dart_financial_ingestion():
    @task(task_id="fetch_and_load_dart_financial")
    def fetch_and_load_dart_financial() -> int:
        corp_code, corp_name = _get_corp_code_by_stock(TARGET_STOCK_CODE, DART_API_KEY)
        bsns_year = datetime.now().strftime("%Y")

        single_accounts = _fetch_single_account(
            corp_code=corp_code,
            api_key=DART_API_KEY,
            bsns_year=bsns_year,
            reprt_code="11011",
            fs_div=FS_DIV,
        )

        # 해당 연도 데이터 없으면 전년도 fallback
        if not single_accounts:
            bsns_year = str(int(bsns_year) - 1)
            single_accounts = _fetch_single_account(
                corp_code=corp_code,
                api_key=DART_API_KEY,
                bsns_year=bsns_year,
                reprt_code="11011",
                fs_div=FS_DIV,
            )

        # 응답 재필터: API가 간헐적으로 다른 fs_div를 섞어 줄 수 있어 강제 필터
        single_accounts = [
            r for r in single_accounts if (r.get("fs_div") or "").upper() == FS_DIV
        ]

        rows: list[dict] = []
        for item in single_accounts:
            statement_type = _to_statement_type((item.get("sj_nm") or "").strip())
            if statement_type is None:
                continue
            rows.append(
                {
                    "year": item.get("bsns_year", bsns_year),
                    "corp_code": item.get("corp_code", corp_code),
                    "corp_name": corp_name,
                    "account_nm": (item.get("account_nm") or "").strip(),
                    "amount": _to_amount(item.get("thstrm_amount")),
                    "statement_type": statement_type,
                    "fs_div": (item.get("fs_div") or FS_DIV).upper(),
                }
            )

        dedup_keys = (
            "year",
            "corp_code",
            "corp_name",
            "account_nm",
            "amount",
            "statement_type",
            "fs_div",
        )
        unique_rows = list(
            {
                tuple(row[key] for key in dedup_keys): row
                for row in rows
            }.values()
        )

        engine = create_engine(DB_URL, pool_pre_ping=True)

        create_sql = text(
            """
            CREATE TABLE IF NOT EXISTS raw_dart_financial_accounts (
                year            VARCHAR(4)   NOT NULL,
                corp_code       VARCHAR(8)   NOT NULL,
                corp_name       VARCHAR(255) NOT NULL,
                account_nm      VARCHAR(255) NOT NULL,
                amount          BIGINT,
                statement_type  VARCHAR(50)  NOT NULL,
                fs_div          VARCHAR(4)   NOT NULL,
                loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                PRIMARY KEY (year, corp_code, account_nm, statement_type, fs_div)
            );
            """
        )

        upsert_sql = text(
            """
            INSERT INTO raw_dart_financial_accounts (
                year, corp_code, corp_name, account_nm, amount, statement_type, fs_div
            ) VALUES (
                :year, :corp_code, :corp_name, :account_nm, :amount, :statement_type, :fs_div
            )
            ON CONFLICT (year, corp_code, account_nm, statement_type, fs_div)
            DO UPDATE SET
                corp_name = EXCLUDED.corp_name,
                amount = EXCLUDED.amount,
                loaded_at = NOW();
            """
        )

        with engine.begin() as conn:
            conn.execute(create_sql)

            for row in unique_rows:
                conn.execute(upsert_sql, row)

        return len(unique_rows)

    fetch_and_load_dart_financial()


livart_dart_financial_ingestion()