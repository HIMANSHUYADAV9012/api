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

APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs?token={APIFY_TOKEN}"
APIFY_DATASET_URL = "https://api.apify.com/v2/datasets/{dataset_id}/items?token={token}"

# ================= TELEGRAM =================
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

# ================= SETTINGS =================
REQUEST_TIMEOUT = 60
CACHE_TTL = 300
NEGATIVE_CACHE_TTL = 600
POLL_INTERVAL = 1
MAX_WAIT_TIME = 15

# ================= APP =================
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Instagram Profile API", version="3.1.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)

CACHE: Dict[str, dict] = {}
LOCK = asyncio.Lock()

# ================= TELEGRAM =================
async def notify_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message})
    except:
        pass

# ================= UTILS =================
def validate_username(username: str) -> bool:
    return bool(re.match(r"^[a-zA-Z0-9._]{1,30}$", username))

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

    run_res = await client.post(APIFY_RUN_URL, json=payload)

    # Apify run always returns 201 when created
    if run_res.status_code != 201:
        await notify_telegram(f"⚠ APIFY RUN FAILED\n{run_res.text}")
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
            raise HTTPException(502, "APIFY_RUN_FAILED")

        await asyncio.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    else:
        raise HTTPException(504, "APIFY_TIMEOUT")

    dataset_url = APIFY_DATASET_URL.format(
        dataset_id=dataset_id,
        token=APIFY_TOKEN
    )

    data_res = await client.get(dataset_url)

    if data_res.status_code != 200:
        raise HTTPException(502, "DATASET_FETCH_FAILED")

    items = data_res.json()

    if not items:
        raise HTTPException(404, "PROFILE_NOT_FOUND")

    profile = items[0]

    if profile.get("error") == "not_found":
        raise HTTPException(404, "PROFILE_NOT_FOUND")

    return profile

# ================= ROUTE =================
@app.get("/scrape/{username}")
@limiter.limit("30/minute")
async def get_user(username: str, request: Request):

    if not validate_username(username):
        raise HTTPException(400, "INVALID_USERNAME")

    async with LOCK:
        cached = CACHE.get(username)
        if cached and cached["expiry"] > time.time():

            if cached["data"].get("error"):
                return JSONResponse(status_code=404, content=cached["data"])

            return cached["data"]

    try:
        raw_profile = await fetch_from_apify(username)

    except HTTPException as e:
        if e.status_code == 404:
            async with LOCK:
                CACHE[username] = {
                    "data": {"error": "PROFILE_NOT_FOUND"},
                    "expiry": time.time() + NEGATIVE_CACHE_TTL
                }
            return JSONResponse(status_code=404, content={"error": "PROFILE_NOT_FOUND"})
        raise

    formatted = format_profile(raw_profile)

    async with LOCK:
        CACHE[username] = {
            "data": formatted,
            "expiry": time.time() + CACHE_TTL
        }

    return formatted

# ================= HEALTH =================
@app.get("/health")
async def health():
    return {"status": "healthy", "time": time.time()}
