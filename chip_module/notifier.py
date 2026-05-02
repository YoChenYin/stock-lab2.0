"""
chip_module/notifier.py
Gmail SMTP 執行報告通知。

環境變數（在 Zeabur Variables 設定）：
  GMAIL_USER         — 寄件 Gmail 帳號，例如 yourname@gmail.com
  GMAIL_APP_PASSWORD — Gmail App 密碼（16碼，Google 帳號 > 安全性 > 應用程式密碼）

若未設定環境變數則靜默略過，不影響主流程。
"""

import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = logging.getLogger(__name__)

TO_EMAIL = "chenyin.yo@gmail.com"

_STATUS_EMOJI  = {"success": "✅", "partial": "⚠️", "failed": "❌"}
_STATUS_LABEL  = {"success": "成功", "partial": "部分失敗", "failed": "失敗"}


def send_run_report(status: str, steps: dict, elapsed_s: int, run_date: str) -> None:
    """
    steps: {step_name: {"ok": bool, "elapsed_s": float, "error": str}}
    """
    gmail_user = os.environ.get("GMAIL_USER", "").strip()
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    if not gmail_user or not gmail_pass:
        log.info("[notifier] 未設定 GMAIL_USER/GMAIL_APP_PASSWORD，跳過寄信")
        return

    emoji = _STATUS_EMOJI.get(status, "📊")
    label = _STATUS_LABEL.get(status, status)
    subject = f"{emoji} Stock Lab 每日更新 {label} — {run_date}"

    rows = ""
    for name, info in steps.items():
        ok_cell  = "✅ 成功" if info["ok"] else "❌ 失敗"
        elapsed  = f"{info.get('elapsed_s', 0):.0f}s"
        err_html = ""
        if not info["ok"] and info.get("error"):
            err_html = f'<br><span style="color:#b91c1c;font-size:11px">{info["error"]}</span>'
        rows += (
            f"<tr>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #f1f5f9'>{name}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #f1f5f9'>{ok_cell}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #f1f5f9'>{elapsed}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #f1f5f9'>{err_html}</td>"
            f"</tr>"
        )

    body = f"""
<html><body style="font-family:sans-serif;color:#1e293b;max-width:640px;margin:0 auto">
  <h2 style="margin-bottom:4px">{emoji} Stock Lab 每日資料更新</h2>
  <p style="color:#64748b;margin-top:0">
    日期：<strong>{run_date}</strong> ｜
    狀態：<strong style="color:{'#16a34a' if status=='success' else '#d97706' if status=='partial' else '#dc2626'}">{label}</strong> ｜
    耗時：{elapsed_s}s
  </p>
  <table style="border-collapse:collapse;width:100%;margin-top:16px;font-size:14px">
    <thead>
      <tr style="background:#f8fafc;font-weight:600;text-align:left">
        <th style="padding:8px 12px">步驟</th>
        <th style="padding:8px 12px">狀態</th>
        <th style="padding:8px 12px">耗時</th>
        <th style="padding:8px 12px">錯誤訊息</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  <p style="margin-top:24px;font-size:12px;color:#94a3b8">Stock Lab 2.0 — 自動發送，請勿回覆</p>
</body></html>
"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = gmail_user
        msg["To"]      = TO_EMAIL
        msg.attach(MIMEText(body, "html", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
            smtp.login(gmail_user, gmail_pass)
            smtp.sendmail(gmail_user, TO_EMAIL, msg.as_string())
        log.info(f"[notifier] 報告已寄至 {TO_EMAIL}")
    except Exception as e:
        log.warning(f"[notifier] 寄信失敗（不影響主流程）: {e}")
