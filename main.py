import asyncio
import time
import re
import io
import random
from typing import Dict

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import os
from dotenv import load_dotenv

# ================= LOAD ENV =================
load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = os.getenv("ACTOR_ID")

if not APIFY_TOKEN or not ACTOR_ID:
    raise ValueError("APIFY_TOKEN or ACTOR_ID missing")

# 🚀 SYNC RUN (FAST)
APIFY_SYNC_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/run-sync-get-dataset-items?token={APIFY_TOKEN}"

# ================= TELEGRAM =================
TELEGRAM_BOT_TOKEN = "8495512623:AAF6lpsd0vAAfcbCABre05IJ_-_WAdzItYk"
TELEGRAM_CHAT_ID = "5029478739"

# ================= SETTINGS =================
REQUEST_TIMEOUT = 60
CACHE_TTL = 300
NEGATIVE_CACHE_TTL = 600

# ================= APP =================
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Instagram Profile API", version="3.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= GLOBAL CLIENT =================
client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)

# ================= CACHE =================
CACHE: Dict[str, dict] = {}
LOCK = asyncio.Lock()

# ================= TELEGRAM =================
async def notify_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        await client.post(url, json=payload)
    except:
        pass

# ================= UTILS =================
def validate_username(username: str) -> bool:
    return bool(re.match(r"^[a-zA-Z0-9._]{1,30}$", username))

def get_random_headers():
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    ]
    return {
        "User-Agent": random.choice(agents),
        "Referer": "https://www.instagram.com/",
    }

def format_profile(profile: dict) -> dict:
    return {
        "username": profile.get("username"),
        "real_name": profile.get("fullName"),
        "profile_pic": profile.get("profilePicUrl"),
        "followers": profile.get("followersCount"),
        "following": profile.get("followsCount"),
        "post_count": profile.get("postsCount"),
        "bio": profile.get("biography"),
    }

# ================= SCRAPER =================
async def fetch_from_apify(username: str) -> dict:
    payload = {"usernames": [username]}

    try:
        res = await client.post(APIFY_SYNC_URL, json=payload)
    except Exception as e:
        await notify_telegram(f"🚨 APIFY DOWN\n@{username}\n{str(e)}")
        raise HTTPException(503, "APIFY_UNREACHABLE")

    if res.status_code != 200:
        await notify_telegram(f"⚠ APIFY ERROR {res.status_code}\n@{username}")
        raise HTTPException(502, "APIFY_ERROR")

    items = res.json()

    if not items:
        raise HTTPException(404, "PROFILE_NOT_FOUND")

    profile = items[0]

    if profile.get("error") == "not_found":
        raise HTTPException(404, "PROFILE_NOT_FOUND")

    return profile

# ================= MAIN ROUTE =================
@app.get("/scrape/{username}")
@limiter.limit("30/minute")
async def get_user(username: str, request: Request):

    if not validate_username(username):
        raise HTTPException(400, "INVALID_USERNAME")

    # 🔥 CHECK CACHE
    async with LOCK:
        cached = CACHE.get(username)
        if cached and cached["expiry"] > time.time():

            if cached["data"].get("error") == "PROFILE_NOT_FOUND":
                return JSONResponse(
                    status_code=404,
                    content={"error": "PROFILE_NOT_FOUND"}
                )

            return cached["data"]

    # 🔥 FETCH
    try:
        raw_profile = await fetch_from_apify(username)

    except HTTPException as e:
        # 🔥 NEGATIVE CACHE
        if e.status_code == 404:
            async with LOCK:
                CACHE[username] = {
                    "data": {"error": "PROFILE_NOT_FOUND"},
                    "expiry": time.time() + NEGATIVE_CACHE_TTL
                }

            return JSONResponse(
                status_code=404,
                content={"error": "PROFILE_NOT_FOUND"}
            )

        raise

    except Exception as e:
        await notify_telegram(f"🚨 INTERNAL ERROR\n@{username}\n{str(e)}")
        raise HTTPException(500, "INTERNAL_ERROR")

    formatted = format_profile(raw_profile)

    # 🔥 SAVE CACHE
    async with LOCK:
        CACHE[username] = {
            "data": formatted,
            "expiry": time.time() + CACHE_TTL
        }

    return formatted

# ================= PROXY IMAGE =================
@app.get("/proxy-image/")
@limiter.limit("50/minute")
async def proxy_image(request: Request, url: str = Query(...)):

    try:
        headers = get_random_headers()
        resp = await client.get(url, headers=headers)

        if resp.status_code == 200:
            return StreamingResponse(
                io.BytesIO(resp.content),
                media_type=resp.headers.get("content-type", "image/jpeg")
            )

        if resp.status_code == 404:
            raise HTTPException(404, "Image not found")

        raise HTTPException(502, "IMAGE_FETCH_FAILED")

    except Exception as e:
        await notify_telegram(f"🚨 IMAGE ERROR\n{url}\n{str(e)}")
        raise HTTPException(502, "IMAGE_FETCH_FAILED")

# ================= HEALTH =================
@app.get("/health")
async def health():
    return {"status": "healthy", "time": time.time()}
