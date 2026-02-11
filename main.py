import asyncio
import time
import re
import io
import random
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ================= CONFIG =================
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = os.getenv("ACTOR_ID")

if not APIFY_TOKEN:
    raise ValueError("APIFY_TOKEN not found in environment variables")

APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs?token={APIFY_TOKEN}"
APIFY_DATASET_URL = "https://api.apify.com/v2/datasets/{dataset_id}/items?token={token}"


REQUEST_TIMEOUT = 60
POLL_INTERVAL = 1           # seconds – faster polling
MAX_WAIT_TIME = 15         # seconds – actor must finish within 15s
CACHE_TTL = 300           # 5 minutes cache

# ================= RATE LIMITING =================
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Instagram Profile API", version="2.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ================= CORS =================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # restrict in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= CACHE & STATS =================
CACHE: Dict[str, dict] = {}
STATS = {"hits": 0, "misses": 0, "last_alerts": []}
LOCK = asyncio.Lock()  # for thread‑safe cache updates

# ================= UTILS =================
def validate_username(username: str) -> bool:
    return bool(re.match(r"^[a-zA-Z0-9._]{1,30}$", username))

def format_error_message(context: str, attempt: int, error: str, status_code: int = None):
    msg = f"[{context}] Attempt {attempt} failed"
    if status_code:
        msg += f" | HTTP {status_code}"
    msg += f" | Error: {error}"
    return msg

# ================= TELEGRAM NOTIFIER (optional) =================
async def notify_telegram(message: str):
    """Implement this if you want alerts. For now just log."""
    STATS["last_alerts"].append({"time": time.time(), "msg": message})
    STATS["last_alerts"] = STATS["last_alerts"][-10:]  # keep last 10
    print(message)

# ================= RANDOM HEADERS FOR PROXY =================
def get_random_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) ...",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ...",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.instagram.com/",
    }

# ================= MAIN SCRAPING LOGIC =================
async def fetch_from_apify(username: str) -> dict:
    """Call Apify actor, wait for result, return raw profile dict."""
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        # ---------- 1. Start actor ----------
        payload = {"usernames": [username]}
        try:
            run_res = await client.post(APIFY_RUN_URL, json=payload)
        except httpx.RequestError as e:
            raise HTTPException(503, {"error": "APIFY_UNREACHABLE", "detail": str(e)})

        if run_res.status_code != 201:
            raise HTTPException(502, {
                "error": "APIFY_RUN_FAILED",
                "detail": run_res.text
            })

        run_data = run_res.json()
        run_id = run_data["data"]["id"]
        dataset_id = run_data["data"]["defaultDatasetId"]

        # ---------- 2. Poll for completion ----------
        status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
        elapsed = 0
        while elapsed < MAX_WAIT_TIME:
            status_res = await client.get(status_url)
            status_data = status_res.json()
            status = status_data["data"]["status"]

            if status == "SUCCEEDED":
                break
            if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                raise HTTPException(502, {
                    "error": "APIFY_RUN_FAILED",
                    "detail": f"Actor run status: {status}"
                })

            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        else:
            raise HTTPException(504, {"error": "APIFY_TIMEOUT"})

        # ---------- 3. Fetch dataset ----------
        dataset_url = APIFY_DATASET_URL.format(dataset_id=dataset_id, token=APIFY_TOKEN)
        data_res = await client.get(dataset_url)

        if data_res.status_code != 200:
            raise HTTPException(502, {
                "error": "DATASET_FETCH_FAILED",
                "detail": data_res.text
            })

        try:
            items = data_res.json()
        except Exception:
            raise HTTPException(502, {"error": "INVALID_JSON"})

        if not isinstance(items, list) or len(items) == 0:
            raise HTTPException(404, {
                "error": "PROFILE_NOT_FOUND",
                "message": f"No data returned for @{username}"
            })

        profile = items[0]

        # ---------- 4. Detect Apify "not_found" error ----------
        if profile.get("error") == "not_found":
            raise HTTPException(404, {
                "error": "PROFILE_NOT_FOUND",
                "message": f"Instagram user @{username} does not exist"
            })

        return profile

def format_profile(profile: dict) -> dict:
    """Extract only the fields you need, in the format you want."""
    return {
        "username": profile.get("username"),
        "real_name": profile.get("fullName"),
        "profile_pic": profile.get("profilePicUrl"),  # non-HD version
        "followers": profile.get("followersCount"),
        "following": profile.get("followsCount"),
        "post_count": profile.get("postsCount"),
        "bio": profile.get("biography"),
    }

# ================= ENDPOINTS =================
@app.get("/scrape/{username}")
@limiter.limit("30/minute")   # adjust per your plan
async def get_user(username: str, request: Request):
    # ---------- Username validation ----------
    if not validate_username(username):
        raise HTTPException(400, {
            "error": "INVALID_USERNAME",
            "message": "Instagram username format invalid"
        })

    # ---------- Cache check ----------
    async with LOCK:
        cached = CACHE.get(username)
        if cached and cached["expiry"] > time.time():
            STATS["hits"] += 1
            return cached["data"]

    STATS["misses"] += 1

    # ---------- Fetch from Apify ----------
    try:
        raw_profile = await fetch_from_apify(username)
    except HTTPException:
        raise  # re-raise our properly formatted 404/502/...
    except Exception as e:
        # Unexpected error – log and return 500
        msg = format_error_message(username, 1, str(e))
        await notify_telegram(msg)
        raise HTTPException(500, {"error": "INTERNAL_ERROR", "detail": str(e)})

    # ---------- Format & cache ----------
    formatted = format_profile(raw_profile)
    async with LOCK:
        CACHE[username] = {
            "data": formatted,
            "expiry": time.time() + CACHE_TTL
        }

    return formatted

@app.get("/proxy-image/")
@limiter.limit("50/minute")
async def proxy_image(
    request: Request,
    url: str = Query(..., description="Full image URL"),
    max_retries: int = 2
):
    """Fetch and return an image with browser‑like headers."""
    for attempt in range(max_retries):
        headers = get_random_headers()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, headers=headers)

            if resp.status_code == 200:
                content_type = resp.headers.get("content-type", "image/jpeg")
                return StreamingResponse(io.BytesIO(resp.content), media_type=content_type)

            if resp.status_code == 404:
                raise HTTPException(404, "Image not found")

            msg = format_error_message("proxy-image", attempt + 1,
                                       "Image fetch failed", resp.status_code)
            await notify_telegram(msg)

        except httpx.RequestError as e:
            msg = format_error_message("proxy-image", attempt + 1, str(e))
            await notify_telegram(msg)
        except HTTPException:
            raise
        except Exception as e:
            msg = format_error_message("proxy-image", attempt + 1, str(e))
            await notify_telegram(msg)

    raise HTTPException(502, "All attempts failed for image fetch")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

@app.head("/health")
async def health_check_head():
    return JSONResponse(content=None, status_code=200)

@app.get("/stats")
async def get_stats():
    async with LOCK:
        cache_size = len(CACHE)
    return {
        "cache_size": cache_size,
        "cache_hits": STATS["hits"],
        "cache_misses": STATS["misses"],
        "last_alerts": STATS["last_alerts"]
    }