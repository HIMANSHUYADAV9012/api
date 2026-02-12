
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

if not APIFY_TOKEN:
    raise ValueError("APIFY_TOKEN not found in environment variables")

APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs?token={APIFY_TOKEN}"
APIFY_DATASET_URL = "https://api.apify.com/v2/datasets/{dataset_id}/items?token={token}"

# ================= TELEGRAM (HARDCODED) =================
TELEGRAM_BOT_TOKEN = "8495512623:AAF6lpsd0vAAfcbCABre05IJ_-_WAdzItYk"
TELEGRAM_CHAT_ID = "5029478739"

# ================= SETTINGS =================
REQUEST_TIMEOUT = 60
POLL_INTERVAL = 1
MAX_WAIT_TIME = 15
CACHE_TTL = 300

# ================= RATE LIMIT =================
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Instagram Profile API", version="2.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ================= CORS =================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= CACHE =================
CACHE: Dict[str, dict] = {}
STATS = {"hits": 0, "misses": 0, "last_alerts": []}
LOCK = asyncio.Lock()

# ================= TELEGRAM =================
async def notify_telegram(message: str):
    STATS["last_alerts"].append({"time": time.time(), "msg": message})
    STATS["last_alerts"] = STATS["last_alerts"][-10:]

    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(telegram_url, json=payload)
    except Exception as e:
        print("Telegram send failed:", str(e))

# ================= UTILS =================
def validate_username(username: str) -> bool:
    return bool(re.match(r"^[a-zA-Z0-9._]{1,30}$", username))

def get_random_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Referer": "https://www.instagram.com/",
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
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
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        payload = {"usernames": [username]}

        try:
            run_res = await client.post(APIFY_RUN_URL, json=payload)
        except Exception as e:
            await notify_telegram(f"🚨 APIFY UNREACHABLE\n@{username}\n{str(e)}")
            raise HTTPException(503, "APIFY_UNREACHABLE")

        if run_res.status_code != 201:
            await notify_telegram(f"⚠ APIFY RUN FAILED\n@{username}\nHTTP {run_res.status_code}")
            raise HTTPException(502, "APIFY_RUN_FAILED")

        run_data = run_res.json()
        run_id = run_data["data"]["id"]
        dataset_id = run_data["data"]["defaultDatasetId"]

        status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
        elapsed = 0

        while elapsed < MAX_WAIT_TIME:
            status_res = await client.get(status_url)
            status = status_res.json()["data"]["status"]

            if status == "SUCCEEDED":
                break

            if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                await notify_telegram(f"⚠ APIFY RUN FAILED\n@{username}\nStatus: {status}")
                raise HTTPException(502, "APIFY_RUN_FAILED")

            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        else:
            await notify_telegram(f"⏳ APIFY TIMEOUT\n@{username}")
            raise HTTPException(504, "APIFY_TIMEOUT")

        dataset_url = APIFY_DATASET_URL.format(dataset_id=dataset_id, token=APIFY_TOKEN)
        data_res = await client.get(dataset_url)

        if data_res.status_code != 200:
            await notify_telegram(f"⚠ DATASET FETCH FAILED\n@{username}")
            raise HTTPException(502, "DATASET_FETCH_FAILED")

        items = data_res.json()

        if not items:
            await notify_telegram(f"❌ PROFILE NOT FOUND\n@{username}")
            raise HTTPException(404, "PROFILE_NOT_FOUND")

        profile = items[0]

        if profile.get("error") == "not_found":
            await notify_telegram(f"❌ PROFILE NOT FOUND\n@{username}")
            raise HTTPException(404, "PROFILE_NOT_FOUND")

        return profile

# ================= MAIN SCRAPE =================
@app.get("/scrape/{username}")
@limiter.limit("30/minute")
async def get_user(username: str, request: Request):

    if not validate_username(username):
        raise HTTPException(400, "INVALID_USERNAME")

    async with LOCK:
        cached = CACHE.get(username)
        if cached and cached["expiry"] > time.time():
            STATS["hits"] += 1
            return cached["data"]

    STATS["misses"] += 1

    try:
        raw_profile = await fetch_from_apify(username)
    except HTTPException:
        raise
    except Exception as e:
        await notify_telegram(f"🚨 INTERNAL ERROR\n@{username}\n{str(e)}")
        raise HTTPException(500, "INTERNAL_ERROR")

    formatted = format_profile(raw_profile)

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
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers)

        if resp.status_code == 200:
            return StreamingResponse(
                io.BytesIO(resp.content),
                media_type=resp.headers.get("content-type", "image/jpeg")
            )

        if resp.status_code == 404:
            raise HTTPException(404, "Image not found")

        await notify_telegram(f"⚠ IMAGE FETCH FAILED\n{url}\nHTTP {resp.status_code}")
        raise HTTPException(502, "IMAGE_FETCH_FAILED")

    except Exception as e:
        await notify_telegram(f"🚨 PROXY IMAGE ERROR\n{url}\n{str(e)}")
        raise HTTPException(502, "IMAGE_FETCH_FAILED")

# ================= HEALTH =================
@app.get("/health")
async def health():
    return {"status": "healthy", "time": time.time()}

