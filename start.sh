#!/bin/bash
# 資料抓取由 engine/background.py 的 schedule 排程負責
# 台股：UTC 06:30（台灣 14:30），美股：UTC 15:00（台灣 23:00）
streamlit run main.py --server.port $PORT --server.address 0.0.0.0
