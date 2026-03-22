# Zeabur 部署指南

## 1. 前置準備（本機）

### 確認專案結構
```
stock_lab/
├── main.py                 ← Zeabur 自動偵測為 Streamlit entry
├── requirements.txt        ← 鎖定版本號
├── zbpack.json             ← 告訴 Zeabur 用 Python 3.11
├── .gitignore              ← 確保 secrets.toml 不上傳
├── .streamlit/
│   └── config.toml         ← headless=true，Zeabur 必須
│   └── secrets.toml        ← 本機用，不上傳 GitHub
├── engine/
├── tabs/
└── ui/
```

### 確認 .streamlit/secrets.toml 不在 git 裡
```bash
cat .gitignore | grep secrets   # 應顯示 .streamlit/secrets.toml
git status                      # 確認 secrets.toml 不在 staged 清單
```

---

## 2. Push 到 GitHub

```bash
cd stock_lab

git init
git add .
git commit -m "init: stock lab deployment"

# 在 GitHub 建立新 repo（建議設為 Private）
git remote add origin https://github.com/你的帳號/stock-lab.git
git push -u origin main
```

---

## 3. 在 Zeabur 建立專案

1. 前往 https://zeabur.com 登入
2. 點擊 **New Project**
3. 選擇 **Deploy from GitHub**
4. 授權 Zeabur 存取你的 repository
5. 選擇 `stock-lab` repo → **Deploy**

Zeabur 會自動偵測到 `streamlit` 在 requirements.txt，
並且 entry 是 `main.py`，直接以 Streamlit 模式啟動。

---

## 4. 選擇方案（重要）

| 方案 | 費用 | CPU | RAM | 建議 |
|---|---|---|---|---|
| Free | $0 | 1 vCPU | 2 GB | ❌ XGBoost 會 OOM |
| **Developer** | **$5/mo + 用量** | **2 vCPU** | **4 GB** | **✅ 此專案推薦** |
| Team | $80/mo | 4 vCPU | 8 GB | 多人同時使用才需要 |

**費用估算（Developer）：**
- 記憶體 $0.00025/GB-min，CPU 免費
- Streamlit 大部分時間 idle，平均用 1–2 GB
- 預估月費：$5–15

升級方式：Zeabur 控制台 → Settings → Billing → Upgrade to Developer

---

## 5. 設定環境變數（Secrets）

在 Zeabur 控制台：
**Service → Variables → Add Variable**

```
FINMIND_TOKEN     = 你的 FinMind token
GEMINI_API_KEY    = 你的 Gemini API key
```

> ⚠️ 不要用 `.streamlit/secrets.toml` 上傳 — 那個只用於本機開發。
> Zeabur 上一律用環境變數。

`main.py` 和 `wall_street_engine.py` 已透過 `st.secrets.get()` 自動讀取，
Zeabur 的環境變數會被 Streamlit 當成 secrets 處理，無需修改代碼。

---

## 6. 掛載 Persistent Volume（SQLite 快取）

**這步驟非常重要。** 不掛載的話，每次 Zeabur redeploy 或重啟，
SQLite 快取就會消失，每次啟動都要重新從 FinMind 抓資料。

在 Zeabur 控制台：
1. 進入你的 Service
2. 點擊 **Storage** → **Add Volume**
3. Mount Path 填入：`/data`
4. 大小：建議 1 GB（SQLite 很輕量）

掛載後，`engine/cache.py` 會自動把 DB 存到 `/data/finmind_cache.db`。

---

## 7. 設定自訂域名（選用）

1. Zeabur 控制台 → **Networking** → **Add Domain**
2. 輸入你的域名，例如 `stock.yourdomain.com`
3. 照指示在 DNS 加上 CNAME record
4. 等待 SSL 自動申請（通常 5 分鐘內）

或直接用 Zeabur 提供的免費子域名：`your-app.zeabur.app`

---

## 8. 驗證部署成功

部署完成後，確認以下幾點：

```
✅ 瀏覽器可以開啟 Zeabur 給的 URL
✅ 側欄顯示「📖 使用指南」「1. 選股」等選項
✅ 選股頁點擊「開始掃描」可以運行（不報 API key 錯誤）
✅ 個股頁 AI 分析可以顯示（不報 Gemini 錯誤）
✅ Zeabur Logs 沒有 OOM / MemoryError
```

---

## 9. 設定每日資料排程（選用）

目前排程是 in-app banner（用戶手動點更新）。
若要完全自動，可在 Zeabur 加一個 Cron Service：

1. **New Service** → **Prebuilt** → 搜尋 `cron`
2. 或直接在同個服務加環境變數觸發 scheduler：

```bash
# 在 Zeabur 的 Build Command 設定（每天 14:35 台北時間）：
# 注意：Zeabur 不支援內建 cron，建議用外部 cron 打 webhook
# 或搭配 GitHub Actions 排程：

# .github/workflows/daily_refresh.yml
name: Daily data refresh
on:
  schedule:
    - cron: '35 6 * * 1-5'   # UTC 06:35 = Taipei 14:35
jobs:
  refresh:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger Zeabur redeploy
        run: |
          curl -X POST ${{ secrets.ZEABUR_DEPLOY_HOOK }}
```

---

## 10. 常見問題

| 問題 | 原因 | 解法 |
|---|---|---|
| Build 失敗：`No module named xxx` | requirements.txt 版本衝突 | 檢查 Zeabur Build Logs，鎖定版本號 |
| App 啟動後空白 | `config.toml` 缺 `headless=true` | 確認 `.streamlit/config.toml` 已上傳 |
| API key 讀不到 | 用了 secrets.toml 但沒設環境變數 | 在 Zeabur Variables 加 `FINMIND_TOKEN` |
| 每次重啟快取消失 | 沒有掛載 Persistent Volume | 步驟 6，掛載 `/data` |
| OOM / 崩潰 | Free plan 記憶體不足 | 升級 Developer plan |
| XGBoost 訓練很慢 | 10 年資料計算 | 正常，第一次會較慢，之後有 cache |
