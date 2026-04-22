"""
fetchers/large_holder.py
抓取 EDGAR SC 13D / SC 13G 大戶持股申報（持股 > 5%）

資料來源：EDGAR ATOM/RSS feed（結構化索引，即時更新）
舊做法：EFTS 全文搜尋（SC 13D/13G 有 4 個月以上的延遲，已廢棄）

ATOM feed 端點：
  https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany
    &CIK={subject_cik}&type=SC+13&owner=include&count=40&output=atom

此端點搜尋的是「以此公司為申報標的」的 13D/13G，
從 accession number 開頭 10 碼可反查 filer CIK，
再透過 submissions API 取得 filer 名稱。
"""

import time
import requests
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from typing import List, Optional

from ..db.schema import get_conn

EDGAR_WWW   = "https://www.sec.gov"
EDGAR_DATA  = "https://data.sec.gov"
HEADERS     = {"User-Agent": "StockLab research@youremail.com"}
SLEEP_SEC   = 0.25

ATOM_NS     = {"atom": "http://www.w3.org/2005/Atom"}
FORM_TYPES  = {"SC 13D", "SC 13G", "SC 13D/A", "SC 13G/A"}

# 本地快取 filer_cik → 名稱，避免重複 API 呼叫
_filer_name_cache: dict = {}


def _get(url: str) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        time.sleep(SLEEP_SEC)
        return r
    except Exception as e:
        print(f"  [large_holder] 請求失敗 {url[:80]}: {e}")
        time.sleep(1)
        return None


def _ticker_to_cik(ticker: str) -> Optional[str]:
    """ticker → 10 位 CIK"""
    r = _get(f"{EDGAR_WWW}/files/company_tickers.json")
    if not r:
        return None
    for entry in r.json().values():
        if entry.get("ticker", "").upper() == ticker.upper():
            return str(entry["cik_str"]).zfill(10)
    return None


def _filer_name(filer_cik: str) -> str:
    """從 submissions API 取得 filer 名稱，帶本地快取"""
    if filer_cik in _filer_name_cache:
        return _filer_name_cache[filer_cik]
    r = _get(f"{EDGAR_DATA}/submissions/CIK{filer_cik}.json")
    name = r.json().get("name", filer_cik) if r else filer_cik
    _filer_name_cache[filer_cik] = name
    return name


def _fetch_via_atom(conn, ticker: str, cik: str, since: str) -> int:
    """
    用 EDGAR ATOM feed 取得以 cik 為標的的 SC 13D/13G 申報。
    accession number 格式：{filer_cik_10d}-{YY}-{seq}
    """
    url = (
        f"{EDGAR_WWW}/cgi-bin/browse-edgar?"
        f"action=getcompany&CIK={cik}&type=SC+13"
        f"&dateb=&owner=include&count=40&output=atom"
    )
    r = _get(url)
    if not r:
        return 0

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        print(f"  [large_holder] XML 解析失敗: {e}")
        return 0

    entries = root.findall("atom:entry", ATOM_NS)
    inserted = 0

    for entry in entries:
        content = entry.find("atom:content", ATOM_NS)
        if content is None:
            continue

        filed_date  = content.findtext("atom:filing-date", default="", namespaces=ATOM_NS)
        form_type   = content.findtext("atom:filing-type", default="", namespaces=ATOM_NS)
        accession   = content.findtext("atom:accession-number", default="", namespaces=ATOM_NS)

        # 過濾日期和表單類型
        if not filed_date or filed_date < since:
            continue
        if form_type not in FORM_TYPES:
            continue

        # 從 accession number 提取 filer CIK（前 10 位）
        filer_cik = accession.replace("-", "")[:10] if accession else ""
        if not filer_cik:
            filer_name = "Unknown"
        else:
            # 跳過公司自身申報（filer == subject，如股票回購計畫等不代表大戶持股）
            if filer_cik.lstrip("0") == cik.lstrip("0"):
                continue
            filer_name = _filer_name(filer_cik)

        acc_clean = accession.replace("-", "") if accession else None

        try:
            conn.execute("""
                INSERT INTO large_holders
                    (ticker, filed_date, form_type, filer_name, accession_number)
                VALUES (?,?,?,?,?)
                ON CONFLICT(accession_number) DO NOTHING
            """, (ticker, filed_date, form_type, filer_name, acc_clean))
            inserted += 1
        except Exception as e:
            print(f"  DB 寫入失敗: {e}")

    return inserted


def fetch_large_holders(tickers: List[str], days_back: int = 180, db_path=None):
    """
    從 EDGAR 搜尋各 ticker 近期的 SC 13D/13G 申報。
    days_back 建議 180 天：13G 最晚 45 天申報；
    大型股通常每年 2 月提交 13G/A 年度修正，180 天確保覆蓋。
    """
    conn  = get_conn(db_path) if db_path else get_conn()
    since = (date.today() - timedelta(days=days_back)).isoformat()

    for ticker in tickers:
        print(f"[large_holder] 處理 {ticker}...")
        cik = _ticker_to_cik(ticker)
        if not cik:
            print(f"  找不到 CIK，跳過")
            continue

        inserted = _fetch_via_atom(conn, ticker, cik, since)
        print(f"  {ticker}: {inserted} 筆大戶申報 inserted")

    conn.commit()
    conn.close()


def get_recent_large_holders(ticker: str, days_back: int = 180,
                              db_path=None) -> list:
    """查詢近期大戶申報紀錄，供 signal 模組使用"""
    since = (date.today() - timedelta(days=days_back)).isoformat()
    conn  = get_conn(db_path) if db_path else get_conn()
    rows  = conn.execute("""
        SELECT filed_date, form_type, filer_name
        FROM large_holders
        WHERE ticker=? AND filed_date >= ?
        ORDER BY filed_date DESC
    """, (ticker, since)).fetchall()
    conn.close()
    return [dict(r) for r in rows]
