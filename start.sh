#!/bin/sh
# Make runtime env vars (e.g. FINMIND_TOKEN) visible to cron
printenv >> /etc/environment

# Start cron daemon in background
cron

# Start FastAPI server (foreground)
exec uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080} --loop asyncio
