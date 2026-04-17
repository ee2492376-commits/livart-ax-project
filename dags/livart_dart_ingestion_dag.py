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

DART_API_KEY = os.getenv("DART_API_KEY", "")
TARGET_STOCK_CODE = "079430"  # 현대리바트
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/dbt"


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


def _fetch_quarterly_reports(corp_code: str, api_key: str, bgn_de: str, end_de: str) -> list[dict]:
    """
    DART list.json에서 분기보고서(A003) 목록 페이징 조회
    """
    all_items: list[dict] = []
    page_no = 1
    page_count = 100

    while True:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "pblntf_ty": "A",          # 정기공시
            "pblntf_detail_ty": "A003", # 분기보고서
            "page_no": str(page_no),
            "page_count": str(page_count),
        }
        url = "https://opendart.fss.or.kr/api/list.json?" + urllib.parse.urlencode(params)
        data = _http_get_json(url)

        status = data.get("status")
        if status == "013":  # 조회된 데이터 없음
            break
        if status != "000":
            raise RuntimeError(f"DART API 오류: status={status}, message={data.get('message')}")

        items = data.get("list", [])
        all_items.extend(items)

        total_count = int(data.get("total_count", 0))
        if page_no * page_count >= total_count:
            break
        page_no += 1

    return all_items


@dag(
    dag_id="livart_dart_quarterly_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # 수동 실행 (원하면 cron으로 변경)
    catchup=False,
    tags=["dart", "livart", "ingestion"],
)
def livart_dart_quarterly_ingestion():
    @task(task_id="fetch_and_load_dart_quarterly")
    def fetch_and_load_dart_quarterly() -> int:
        # 최근 넉넉히 5년 조회
        bgn_de = "20200101"
        end_de = datetime.now().strftime("%Y%m%d")

        corp_code, corp_name = _get_corp_code_by_stock(TARGET_STOCK_CODE, DART_API_KEY)
        reports = _fetch_quarterly_reports(corp_code, DART_API_KEY, bgn_de, end_de)

        engine = create_engine(DB_URL, pool_pre_ping=True)

        create_sql = text(
            """
            CREATE TABLE IF NOT EXISTS raw_dart_quarterly_reports (
                rcept_no        VARCHAR(14) PRIMARY KEY,
                corp_code       VARCHAR(8)  NOT NULL,
                corp_name       VARCHAR(255),
                stock_code      VARCHAR(6),
                report_nm       VARCHAR(255),
                rcept_dt        DATE,
                flr_nm          VARCHAR(255),
                rm              TEXT,
                loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        upsert_sql = text(
            """
            INSERT INTO raw_dart_quarterly_reports (
                rcept_no, corp_code, corp_name, stock_code, report_nm, rcept_dt, flr_nm, rm
            ) VALUES (
                :rcept_no, :corp_code, :corp_name, :stock_code, :report_nm, :rcept_dt, :flr_nm, :rm
            )
            ON CONFLICT (rcept_no)
            DO UPDATE SET
                corp_code = EXCLUDED.corp_code,
                corp_name = EXCLUDED.corp_name,
                stock_code = EXCLUDED.stock_code,
                report_nm = EXCLUDED.report_nm,
                rcept_dt = EXCLUDED.rcept_dt,
                flr_nm = EXCLUDED.flr_nm,
                rm = EXCLUDED.rm,
                loaded_at = NOW();
            """
        )

        with engine.begin() as conn:
            conn.execute(create_sql)

            for item in reports:
                conn.execute(
                    upsert_sql,
                    {
                        "rcept_no": item.get("rcept_no"),
                        "corp_code": item.get("corp_code", corp_code),
                        "corp_name": item.get("corp_name", corp_name),
                        "stock_code": item.get("stock_code", TARGET_STOCK_CODE),
                        "report_nm": item.get("report_nm"),
                        "rcept_dt": item.get("rcept_dt"),  # YYYYMMDD 문자열도 Postgres DATE로 캐스팅됨
                        "flr_nm": item.get("flr_nm"),
                        "rm": item.get("rm"),
                    },
                )

        return len(reports)

    fetch_and_load_dart_quarterly()


livart_dart_quarterly_ingestion()