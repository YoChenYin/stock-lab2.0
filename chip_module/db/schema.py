"""
chip_module/db/schema.py
初始化 SQLite schema，可獨立執行或在 stock lab 啟動時呼叫。
"""

import os
import sqlite3
from pathlib import Path

# 支援 env var，方便 Zeabur Persistent Volume 掛載到統一路徑
DB_PATH = Path(os.environ.get("CHIP_DB_PATH", Path(__file__).parent.parent / "chip.db"))


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # 允許讀寫並發
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path = DB_PATH):
    conn = get_conn(db_path)
    cur = conn.cursor()

    # ── 1. 每日股價 ──────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_prices (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker      TEXT    NOT NULL,
        date        TEXT    NOT NULL,   -- YYYY-MM-DD
        open        REAL,
        high        REAL,
        low         REAL,
        close       REAL,
        volume      INTEGER,
        -- 預計算技術指標（省去每次查詢重算）
        obv         REAL,
        obv_signal  REAL,               -- OBV 20日EMA
        cmf_20      REAL,               -- Chaikin Money Flow
        mfi_14      REAL,               -- Money Flow Index
        avg_vol_20  REAL,               -- 20日平均成交量
        vol_ratio   REAL,               -- volume / avg_vol_20
        UNIQUE(ticker, date)
    )""")

    # ── 2. 內部人交易 Form 4 ──────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS insider_trades (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker              TEXT    NOT NULL,
        report_date         TEXT    NOT NULL,   -- 申報日
        trade_date          TEXT,               -- 實際交易日
        insider_name        TEXT,
        insider_title       TEXT,
        transaction_type    TEXT,               -- P=買, S=賣, A=授予
        shares              REAL,
        price_per_share     REAL,
        total_value         REAL,
        shares_owned_after  REAL,
        accession_number    TEXT    UNIQUE,     -- SEC 唯一識別碼，防重複
        fetched_at          TEXT    DEFAULT (datetime('now'))
    )""")

    # ── 3. 空頭興趣（FINRA 每半月）────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS short_interest (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker          TEXT    NOT NULL,
        settlement_date TEXT    NOT NULL,   -- FINRA 結算日
        short_volume    INTEGER,
        avg_daily_vol   REAL,
        short_float_pct REAL,               -- short / float
        days_to_cover   REAL,               -- short_volume / avg_daily_vol
        prev_short_vol  INTEGER,            -- 上期空頭量（算變化率用）
        chg_pct         REAL,               -- 空頭量變化%
        UNIQUE(ticker, settlement_date)
    )""")

    # ── 4. 選擇權情緒（CBOE 每日 P/C Ratio）──────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS options_sentiment (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        date            TEXT    NOT NULL,
        scope           TEXT    NOT NULL,   -- 'equity', 'index', 'total'
        pc_ratio        REAL,
        -- 統計衍生欄位（批次計算後回填）
        pc_ma10         REAL,               -- 10日移動平均
        pc_ma20         REAL,
        pc_zscore_20    REAL,               -- z-score vs 20日均值
        UNIQUE(date, scope)
    )""")

    # ── 5. 機構持倉（13F，每季）──────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS institutional_holders (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker          TEXT    NOT NULL,
        report_date     TEXT    NOT NULL,   -- 13F 申報日
        institution     TEXT,
        shares_held     REAL,
        pct_out         REAL,               -- % of shares outstanding
        value_usd       REAL,
        prev_shares     REAL,               -- 上季持倉（算增減用）
        chg_pct         REAL,
        UNIQUE(ticker, report_date, institution)
    )""")

    # ── 6. 個股選擇權流量快照（每日）────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS options_flow (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker                  TEXT    NOT NULL,
        date                    TEXT    NOT NULL,
        underlying_price        REAL,
        -- call 匯總
        call_volume             INTEGER,
        call_oi                 INTEGER,
        -- put 匯總
        put_volume              INTEGER,
        put_oi                  INTEGER,
        -- OTM call（strike > price * 1.05）異常追蹤
        otm_call_volume         INTEGER,
        otm_call_oi             INTEGER,
        -- 異常大單統計
        unusual_call_strikes    INTEGER,  -- volume/OI > 3 的 strike 數量
        unusual_put_strikes     INTEGER,
        max_call_vol_oi_ratio   REAL,
        max_put_vol_oi_ratio    REAL,
        -- 隱含波動率
        avg_call_iv             REAL,
        avg_put_iv              REAL,
        UNIQUE(ticker, date)
    )""")

    # ── 7. 大戶申報 13D/13G（持股 >5%）──────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS large_holders (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker          TEXT    NOT NULL,
        filed_date      TEXT    NOT NULL,
        form_type       TEXT,               -- SC 13D / SC 13G / SC 13G/A 等
        filer_name      TEXT,
        accession_number TEXT   UNIQUE,
        UNIQUE(ticker, filed_date, filer_name)
    )""")

    # ── 8. 籌碼綜合分數（每日批次計算結果）──────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chip_scores (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker              TEXT    NOT NULL,
        date                TEXT    NOT NULL,
        -- 各維度分數 0-100
        insider_score       REAL,   -- 內部人 cluster 信號
        short_score         REAL,   -- 空頭動能（空頭↓ = 分數↑）
        volume_score        REAL,   -- OBV/CMF 量能信號
        options_flow_score  REAL,   -- 個股選擇權異常
        options_mkt_score   REAL,   -- 市場情緒 SPY P/C
        institutional_score REAL,   -- 機構持倉變化
        -- 加權綜合分數
        composite_short     REAL,   -- 短線版（1-5天）
        composite_swing     REAL,   -- 波段版（1-4週）
        composite_mid       REAL,   -- 中線版（1-3月）
        -- 巨鯨信號
        whale_alert         INTEGER DEFAULT 0,  -- 1=觸發
        entry_timing        INTEGER DEFAULT 0,  -- 1=早期進場信號
        -- metadata
        signal_flags        TEXT,   -- JSON 字串，存觸發的特殊信號
        calc_version        TEXT    DEFAULT '2.0',
        updated_at          TEXT    DEFAULT (datetime('now')),
        UNIQUE(ticker, date)
    )""")

    # ── 9. 市場環境指標（每日）──────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS market_environment (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        date                TEXT    NOT NULL UNIQUE,
        -- VIX 恐慌指數
        vix                 REAL,
        vix_ma5             REAL,
        vix_level           TEXT,   -- 'low'|'medium'|'high'
        -- 10Y 美債殖利率
        tnx_10y             REAL,
        tnx_slope_20d       REAL,   -- 線性回歸斜率（每日變化率）
        -- Mag 7 乖離率
        mag7_avg_deviation  REAL,   -- 七巨頭相對 50MA 平均乖離%
        mag7_deviations     TEXT,   -- JSON {ticker: deviation%}
        mag7_risk           TEXT,   -- 'ok'|'caution'（乖離 > 15%）
        -- 板塊 ETF（XLK 科技、XLF 金融、XLP 防禦）
        xlk_chg_1d          REAL,
        xlf_chg_1d          REAL,
        xlp_chg_1d          REAL,
        xlk_chg_5d          REAL,
        xlf_chg_5d          REAL,
        xlp_chg_5d          REAL,
        updated_at          TEXT    DEFAULT (datetime('now'))
    )""")

    # ── 10. 技術面進出場信號（每日批次計算）─────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tech_signals (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker              TEXT    NOT NULL,
        date                TEXT    NOT NULL,
        -- 進場指標旗標（0/1）
        ma_aligned          INTEGER DEFAULT 0,  -- 20MA > 50MA > 200MA 且收盤 > 200MA
        rsi_reversal        INTEGER DEFAULT 0,  -- RSI 從 <35 回升至 ≥40
        macd_golden         INTEGER DEFAULT 0,  -- MACD 柱狀體由負轉正
        bb_breakout         INTEGER DEFAULT 0,  -- 收盤 > 布林上軌 + 量能 1.5x
        double_bottom       INTEGER DEFAULT 0,  -- 雙重底型態
        vcp_pattern         INTEGER DEFAULT 0,  -- 波動收斂型態
        -- 出場指標旗標（0/1）
        hard_stop           INTEGER DEFAULT 0,  -- 近 20 日高點回落 > 7%
        below_20ma_3d       INTEGER DEFAULT 0,  -- 連續 3 日 < 20MA
        rsi_divergence      INTEGER DEFAULT 0,  -- 頂背離 或 RSI 超買掉頭
        atr_trailing_stop   INTEGER DEFAULT 0,  -- 跌破 ATR×2 動態止盈線
        -- 原始指標數值（透明化）
        rsi_14              REAL,
        macd_hist           REAL,
        bb_upper            REAL,
        bb_lower            REAL,
        ma_20               REAL,
        ma_50               REAL,
        ma_200              REAL,
        atr_14              REAL,
        vol_ratio           REAL,   -- 當日成交量 / 20 日均量
        -- 綜合分數
        entry_score         REAL,   -- 0-100
        exit_risk           REAL,   -- 0-100
        signal_detail       TEXT,   -- JSON（各旗標 + 數值，完整明細）
        updated_at          TEXT    DEFAULT (datetime('now')),
        UNIQUE(ticker, date)
    )""")

    # ── 11. Factor MAX 歷史（每日，每因子一筆）──────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS factor_max_history (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        date            TEXT    NOT NULL,
        market          TEXT    NOT NULL,   -- 'US' | 'TW'
        factor_name     TEXT    NOT NULL,
        factor_label    TEXT,
        max_ret         REAL,              -- 因子組合單日最大報酬 (%)
        max_date        TEXT,              -- 哪一天達到 MAX
        days_ago        INTEGER,           -- 距今幾個交易日
        avg_ret_20d     REAL,              -- 近 20 日平均報酬
        momentum_score  REAL,              -- 綜合動能評分 0-100
        stock_count     INTEGER,
        top_stocks      TEXT,              -- JSON list，MAX 當日漲最多的個股
        lottery_stocks  TEXT,              -- JSON list，個股層面過熱（>5% 單日）
        updated_at      TEXT    DEFAULT (datetime('now')),
        UNIQUE(date, market, factor_name)
    )""")

    # ── 索引 ─────────────────────────────────────────────────────
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_prices_ticker_date   ON daily_prices(ticker, date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_insider_ticker_date  ON insider_trades(ticker, report_date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_short_ticker_date    ON short_interest(ticker, settlement_date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_options_flow_ticker  ON options_flow(ticker, date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_large_holders_ticker ON large_holders(ticker, filed_date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_scores_ticker_date   ON chip_scores(ticker, date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_scores_composite     ON chip_scores(date DESC, composite_swing DESC)",
        "CREATE INDEX IF NOT EXISTS idx_scores_whale         ON chip_scores(date DESC, whale_alert DESC)",
        "CREATE INDEX IF NOT EXISTS idx_tech_ticker_date     ON tech_signals(ticker, date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_tech_entry_score     ON tech_signals(date DESC, entry_score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_market_env_date      ON market_environment(date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_factor_max_date      ON factor_max_history(date DESC, market, momentum_score DESC)",
    ]
    for idx in indexes:
        cur.execute(idx)

    conn.commit()
    conn.close()
    print(f"[schema] DB initialized at {db_path}")


if __name__ == "__main__":
    init_db()
