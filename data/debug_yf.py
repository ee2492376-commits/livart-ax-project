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

def main() -> None:
    bgn_de = "20200101"
    end_de = datetime.now().strftime("%Y%m%d")

    corp_code, corp_name = _get_corp_code_by_stock(TARGET_STOCK_CODE, DART_API_KEY)
    reports = _fetch_quarterly_reports(corp_code, DART_API_KEY, bgn_de, end_de)
    print(hist)
    print("*" * 60)


if __name__ == "__main__":
    main()
