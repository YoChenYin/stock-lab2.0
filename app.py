"""
app.py — Stock Lab 2.0 entry point (FastAPI)

Run locally:
    uvicorn app:app --reload --port 8080

Deploy (Zeabur):
    Dockerfile CMD handles this automatically
"""

import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse

from api.tw import router as tw_router
from api.us import router as us_router

app = FastAPI(title="Stock Lab 2.0")

templates = Jinja2Templates(directory="templates")

# Mount static files if directory exists
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# Include routers
app.include_router(tw_router, prefix="/tw")
app.include_router(us_router, prefix="/us")


@app.get("/")
async def root():
    return RedirectResponse(url="/tw")


# VS Code Streamlit extension health check shims（消除 404 log 噪音）
@app.get("/_stcore/health")
async def health():
    return {"status": "ok"}

@app.get("/_stcore/host-config")
async def host_config():
    return {}
