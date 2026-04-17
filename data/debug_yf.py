from __future__ import annotations
import pandas as pd
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
def _http_get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))
def _get_corp_code_by_stock(stock_code: str, api_key: str) -> tuple[str, str]:
    if not api_key:
        raise ValueError("환경변수 DART_API_KEY가 비어 있습니다.")
    corp_url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
    with urllib.request.urlopen(corp_url, timeout=60) as resp:
        zip_bytes = resp.read()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
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
    # "연결손익계산서", "연결포괄손익계산서" 같은 값도 흡수
    if "포괄손익계산서" in sj_nm:
        return "포괄손익계산서"
    if "손익계산서" in sj_nm:
        return "손익계산서"
    return None
def _build_financial_dataset(single_accounts: list[dict], corp_name: str) -> pd.DataFrame:
    rows: list[dict] = []
    for r in single_accounts:
        sj_nm = (r.get("sj_nm") or "").strip()
        statement_type = _to_statement_type(sj_nm)
        if statement_type is None:
            continue
        rows.append(
            {
                "year": r.get("bsns_year"),
                "corp_code": r.get("corp_code"),
                "corp_name": corp_name,
                "account_nm": (r.get("account_nm") or "").strip(),
                "amount": r.get("thstrm_amount"),   # 당기 금액
                "statement_type": statement_type,   # 손익계산서 / 포괄손익계산서
                "fs_div": r.get("fs_div"),          # CFS / OFS (필요시 유지)
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["amount"] = (
        df["amount"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"-": None, "": None, "nan": None})
    )
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.sort_values(
        ["year", "corp_name", "statement_type", "account_nm"],
        na_position="last",
    ).reset_index(drop=True)
    return df
def main() -> None:
    corp_code, corp_name = _get_corp_code_by_stock(TARGET_STOCK_CODE, DART_API_KEY)
    bsns_year = datetime.now().strftime("%Y")
    # 1) CFS 조회 (연결)
    single_accounts = _fetch_single_account(
        corp_code=corp_code,
        api_key=DART_API_KEY,
        bsns_year=bsns_year,
        reprt_code="11011",
        #fs_div="CFS",
    )
    # 해당 연도 데이터 없으면 전년도 fallback
    if not single_accounts:
        bsns_year = str(int(bsns_year) - 1)
        single_accounts = _fetch_single_account(
            corp_code=corp_code,
            api_key=DART_API_KEY,
            bsns_year=bsns_year,
            reprt_code="11011",
            #fs_div="CFS",
        )
    #single_accounts = [r for r in single_accounts if (r.get("fs_div") or "").upper() == "OFS"]

    df_dataset = _build_financial_dataset(single_accounts, corp_name=corp_name)
    dedup_keys = ["year", "corp_code", "corp_name", "account_nm", "amount", "statement_type", "fs_div"]
    before = len(df_dataset)
    df_dataset = df_dataset.drop_duplicates(subset=dedup_keys, keep="first").reset_index(drop=True)
    after = len(df_dataset)
    print(f"dedup: {before} -> {after}")

    print(f"corp_name={corp_name}, year={bsns_year}, rows={len(df_dataset)}")
    if df_dataset.empty:
        print("데이터가 없습니다.")
        return
        
    print(df_dataset.head(30))
    # 필요하면 저장
    # df_dataset.to_csv("data/financial_dataset.csv", index=False, encoding="utf-8-sig")
if __name__ == "__main__":
    main()