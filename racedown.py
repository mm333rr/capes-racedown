#!/usr/bin/env python3
"""
capes-racedown  v0.2.0
Race-download daemon for Sonarr + Radarr.

On every on_grab webhook:
  1. Identify Sonarr/Radarr's original torrent hash in qBit.
  2. Search for the top N releases by seeder count (same quality, skip traps).
  3. Add each as a race competitor (tagged capes-race-<key>).
  4. Monitor every MONITOR_INTERVAL seconds:
       winner = first torrent to complete OR sustain >= SPEED_THRESHOLD_BPS.
  5. Delete all losers (files included). Sweep orphaned race-tagged extras.
  6. Strip race tag from winner.
  7. If the winner was NOT the original arr grab:
       - Remove original from arr queue.
       - Trigger DownloadedEpisodesScan / DownloadedMoviesScan.

Webhook URLs:
  POST http://localhost:6789/webhook/sonarr
  POST http://localhost:6789/webhook/radarr

Author: Capes homelab / mm333rr
"""

import asyncio
import logging
import os
import re
import sys
import time
from typing import Dict, List, Optional

import httpx
from fastapi import BackgroundTasks, FastAPI, Request

# ── Config ────────────────────────────────────────────────────────────────────
SONARR_URL     = os.getenv("SONARR_URL",     "http://localhost:8989")
SONARR_KEY     = os.getenv("SONARR_API_KEY", "")
RADARR_URL     = os.getenv("RADARR_URL",     "http://localhost:7878")
RADARR_KEY     = os.getenv("RADARR_API_KEY", "")
QBIT_URL       = os.getenv("QBIT_URL",       "http://localhost:5555")
QBIT_USER      = os.getenv("QBIT_USER",      "admin")
QBIT_PASS      = os.getenv("QBIT_PASS",      "")
RACE_COUNT     = int(os.getenv("RACE_COUNT",              "3"))    # total incl. original
SPEED_THRESH   = float(os.getenv("SPEED_THRESHOLD_GBH",  "1.0"))  # GB/hr
SPEED_BPS      = int(SPEED_THRESH * 1024**3 / 3600)                # bytes/sec
MIN_SCORE      = int(os.getenv("MIN_SCORE",               "0"))
RACE_TIMEOUT   = int(os.getenv("RACE_TIMEOUT_SEC",        str(4 * 3600)))
TV_PATH        = os.getenv("TV_SAVE_PATH",    "/tank/qb/downloads/tv")
MOVIE_PATH     = os.getenv("MOVIE_SAVE_PATH", "/tank/qb/downloads/movies")
MONITOR_SECS   = 30
RACE_TAG_PFX   = "capes-race"

# Sweet-spot size range for candidate scoring (prefers files that complete in ~1hr)
SIZE_LOW_BYTES  = int(700  * 1024**2)   # 700 MB
SIZE_HIGH_BYTES = int(2500 * 1024**2)   # 2.5 GB

# Trap format patterns — releases matching any of these are skipped
TRAP_PATTERNS = re.compile(
    r"\.(zip|rar|scr|bat|exe|cmd|msi|vbs|ps1|pif|jar|apk)(\b|\.|[ ]|$)",
    re.IGNORECASE,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("racedown")

# ── State ─────────────────────────────────────────────────────────────────────
app = FastAPI(title="capes-racedown", version="0.2.0")
active_races: Dict[str, dict] = {}


# ── qBit helpers ──────────────────────────────────────────────────────────────

async def qbit_login(client: httpx.AsyncClient) -> bool:
    """Login to qBit; bypass_local_auth=True means this is usually a no-op."""
    if not QBIT_PASS:
        return True
    try:
        r = await client.post(f"{QBIT_URL}/api/v2/auth/login",
                              data={"username": QBIT_USER, "password": QBIT_PASS},
                              timeout=10)
        return r.text.strip() == "Ok."
    except Exception as e:
        log.warning(f"qbit_login: {e}")
        return False


async def qbit_add(client: httpx.AsyncClient, url: str, category: str,
                   save_path: str, tags: str) -> bool:
    """Add a torrent by URL/magnet to qBit.

    qBit returns HTTP 200 with body 'Ok.' for .torrent files, and HTTP 202
    (Accepted) with an empty body for magnets (async resolution). Accept both.
    """
    try:
        r = await client.post(f"{QBIT_URL}/api/v2/torrents/add",
                              data={"urls": url, "category": category,
                                    "savepath": save_path, "tags": tags},
                              timeout=30)
        ok = r.status_code in (200, 202) or r.text.strip() == "Ok."
        if not ok:
            log.warning(f"qbit_add unexpected {r.status_code}: {r.text[:120]!r}")
        return ok
    except Exception as e:
        log.warning(f"qbit_add failed: {e}")
        return False


async def qbit_add_tags(client: httpx.AsyncClient, hashes: List[str], tags: str):
    """Tag torrents by hash list."""
    try:
        await client.post(f"{QBIT_URL}/api/v2/torrents/addTags",
                          data={"hashes": "|".join(hashes), "tags": tags},
                          timeout=10)
    except Exception as e:
        log.warning(f"qbit_add_tags: {e}")


async def qbit_remove_tags(client: httpx.AsyncClient, hashes: List[str], tags: str):
    """Remove tags from torrents."""
    try:
        await client.post(f"{QBIT_URL}/api/v2/torrents/removeTags",
                          data={"hashes": "|".join(hashes), "tags": tags},
                          timeout=10)
    except Exception as e:
        log.warning(f"qbit_remove_tags: {e}")


async def qbit_info(client: httpx.AsyncClient,
                    tag: Optional[str] = None,
                    hashes: Optional[List[str]] = None) -> List[dict]:
    """Get torrent info list, optionally filtered by tag or hash list.
    Passing no arguments returns ALL torrents.
    """
    params: dict = {}
    if tag:
        params["tag"] = tag
    if hashes:
        params["hashes"] = "|".join(hashes)
    try:
        r = await client.get(f"{QBIT_URL}/api/v2/torrents/info",
                             params=params, timeout=15)
        return r.json() if r.status_code == 200 else []
    except Exception as e:
        log.warning(f"qbit_info: {e}")
        return []


async def qbit_delete(client: httpx.AsyncClient, hashes: List[str],
                      delete_files: bool = True):
    """Delete torrents by hash list."""
    if not hashes:
        return
    try:
        await client.post(f"{QBIT_URL}/api/v2/torrents/delete",
                          data={"hashes": "|".join(hashes),
                                "deleteFiles": "true" if delete_files else "false"},
                          timeout=15)
        log.info(f"Deleted {len(hashes)} torrent(s): {[h[:12] for h in hashes]}")
    except Exception as e:
        log.warning(f"qbit_delete: {e}")


async def qbit_sweep_orphans(client: httpx.AsyncClient, race_key: str,
                              keeper_hashes: List[str]):
    """Find any race-tagged torrents for this race not in keeper_hashes and delete them."""
    race_tag = f"{RACE_TAG_PFX}-{race_key}"
    tagged = await qbit_info(client, tag=race_tag)
    orphans = [t["hash"].lower() for t in tagged
               if t["hash"].lower() not in keeper_hashes]
    if orphans:
        log.warning(f"[{race_key}] Sweeping {len(orphans)} orphaned extra(s)")
        await qbit_delete(client, orphans, delete_files=True)


# ── arr helpers ───────────────────────────────────────────────────────────────

def arr_headers(app_name: str) -> dict:
    key = SONARR_KEY if app_name == "sonarr" else RADARR_KEY
    return {"X-Api-Key": key, "Content-Type": "application/json"}


def arr_base(app_name: str) -> str:
    return SONARR_URL if app_name == "sonarr" else RADARR_URL


async def arr_get_queue_hash(client: httpx.AsyncClient,
                              app_name: str, media_id: int) -> Optional[str]:
    """Find the qBit download hash for this media item in arr's queue."""
    base = arr_base(app_name)
    id_field = "episodeId" if app_name == "sonarr" else "movieId"
    try:
        r = await client.get(f"{base}/api/v3/queue",
                             params={"pageSize": 200},
                             headers=arr_headers(app_name), timeout=15)
        if r.status_code != 200:
            return None
        for item in r.json().get("records", []):
            if item.get(id_field) == media_id:
                h = item.get("downloadId", "")
                if h:
                    return h.lower()
    except Exception as e:
        log.warning(f"arr_get_queue_hash: {e}")
    return None


async def arr_remove_from_queue(client: httpx.AsyncClient,
                                 app_name: str, media_id: int,
                                 torrent_hash: str):
    """Remove the original arr queue entry without blocklisting."""
    base = arr_base(app_name)
    id_field = "episodeId" if app_name == "sonarr" else "movieId"
    try:
        r = await client.get(f"{base}/api/v3/queue",
                             params={"pageSize": 200},
                             headers=arr_headers(app_name), timeout=15)
        for item in r.json().get("records", []):
            if (item.get(id_field) == media_id and
                    item.get("downloadId", "").lower() == torrent_hash):
                qid = item["id"]
                await client.delete(
                    f"{base}/api/v3/queue/{qid}",
                    params={"removeFromClient": "false", "blocklist": "false"},
                    headers=arr_headers(app_name), timeout=15)
                log.info(f"Removed queue entry {qid} from {app_name}")
                return
    except Exception as e:
        log.warning(f"arr_remove_from_queue: {e}")


async def arr_trigger_import(client: httpx.AsyncClient,
                              app_name: str, path: str):
    """Tell arr to scan a completed download path and import it."""
    base = arr_base(app_name)
    cmd = "DownloadedEpisodesScan" if app_name == "sonarr" else "DownloadedMoviesScan"
    try:
        await client.post(f"{base}/api/v3/command",
                          json={"name": cmd, "path": path},
                          headers=arr_headers(app_name), timeout=15)
        log.info(f"{app_name}: triggered {cmd} on {path}")
    except Exception as e:
        log.warning(f"arr_trigger_import: {e}")


def _size_score(size_bytes: int) -> float:
    """
    Score a release by file size proximity to the 1–2 GB/hr sweet spot.
    Files in SIZE_LOW–SIZE_HIGH get 1.0; outside that range score < 1.0.
    This is a secondary sort key — seeders always rank first.
    """
    if size_bytes <= 0:
        return 0.5   # unknown size: neutral
    if SIZE_LOW_BYTES <= size_bytes <= SIZE_HIGH_BYTES:
        return 1.0
    if size_bytes < SIZE_LOW_BYTES:
        return size_bytes / SIZE_LOW_BYTES
    return SIZE_HIGH_BYTES / size_bytes   # oversized: penalty


async def arr_get_candidates(client: httpx.AsyncClient, app_name: str,
                              media_id: int, grabbed_title: str,
                              quality_name: str) -> List[dict]:
    """
    Search arr for race candidate releases:
      - Same quality as the original grab
      - Score >= MIN_SCORE
      - No trap formats
      - Different title than the original (different source)
      - Has a downloadUrl

    Sort: seeders DESC (primary), size sweet-spot score DESC (secondary).
    """
    base = arr_base(app_name)
    id_param = "episodeId" if app_name == "sonarr" else "movieId"
    try:
        r = await client.get(f"{base}/api/v3/release",
                             params={id_param: media_id},
                             headers=arr_headers(app_name), timeout=90)
        if r.status_code != 200:
            log.warning(f"arr release search returned {r.status_code}")
            return []
        releases = r.json()
    except Exception as e:
        log.warning(f"arr_get_candidates [{type(e).__name__}]: {e}")
        return []

    candidates = []
    for rel in releases:
        title    = rel.get("title", "")
        score    = rel.get("customFormatScore", 0)
        quality  = rel.get("quality", {}).get("quality", {}).get("name", "")
        seeders  = rel.get("seeders", 0) or 0
        leechers = rel.get("leechers", 0) or 0
        size     = rel.get("size", 0) or 0
        url      = rel.get("downloadUrl") or rel.get("magnetUrl", "")

        if not url:
            continue
        if TRAP_PATTERNS.search(title):
            log.debug(f"Skipping trap: {title}")
            continue
        if score < MIN_SCORE:
            log.debug(f"Skipping score {score}: {title}")
            continue
        if quality != quality_name:
            continue
        if title == grabbed_title:
            continue

        candidates.append({
            "title":    title,
            "seeders":  seeders,
            "leechers": leechers,
            "score":    score,
            "size":     size,
            "url":      url,
        })

    # Primary: seeders DESC; secondary: size proximity to sweet-spot DESC
    candidates.sort(
        key=lambda x: (x["seeders"], _size_score(x["size"])),
        reverse=True,
    )
    log.info(f"Found {len(candidates)} candidates for media_id={media_id}")
    for c in candidates[:5]:
        sz_mb = c["size"] / 1024**2
        log.info(f"  → {c['seeders']}s/{c['leechers']}l  {sz_mb:.0f}MB  "
                 f"[score={c['score']}] {c['title'][:60]}")
    return candidates


# ── Race logic ────────────────────────────────────────────────────────────────

async def monitor_race(race_key: str, app_name: str, media_id: int,
                        original_hash: str, race_hashes: List[str],
                        save_path: str):
    """
    Poll qBit every MONITOR_SECS until a winner emerges or timeout.
    Winner criteria:
      - state in DONE_STATES (completed download)
      - OR dlspeed >= SPEED_BPS for two consecutive checks
    """
    all_hashes  = [original_hash] + race_hashes
    speed_hits: Dict[str, int] = {}
    deadline    = time.monotonic() + RACE_TIMEOUT
    DONE_STATES = {"uploading", "stalledup", "forcedup", "pausedup", "checkingup"}

    log.info(f"[{race_key}] Monitoring {len(all_hashes)} torrent(s) (timeout {RACE_TIMEOUT}s)")

    async with httpx.AsyncClient() as client:
        await qbit_login(client)

        while time.monotonic() < deadline:
            await asyncio.sleep(MONITOR_SECS)
            torrents    = await qbit_info(client, hashes=all_hashes)
            torrent_map = {t["hash"].lower(): t for t in torrents}
            winner_hash: Optional[str] = None

            for h in all_hashes:
                t = torrent_map.get(h)
                if not t:
                    continue
                state = t.get("state", "").lower()
                speed = t.get("dlspeed", 0) or 0
                name  = t.get("name", h[:8])

                if state in DONE_STATES:
                    log.info(f"[{race_key}] Winner by completion: {name}")
                    winner_hash = h
                    break

                if speed >= SPEED_BPS:
                    speed_hits[h] = speed_hits.get(h, 0) + 1
                    if speed_hits[h] >= 2:
                        log.info(f"[{race_key}] Winner by speed ({speed // 1024}KB/s): {name}")
                        winner_hash = h
                        break
                else:
                    speed_hits[h] = 0

            if winner_hash:
                await declare_winner(client, race_key, app_name, media_id,
                                     original_hash, all_hashes, winner_hash,
                                     torrent_map, save_path)
                break
        else:
            log.warning(f"[{race_key}] Race timed out — keeping original, deleting extras")
            async with httpx.AsyncClient() as c2:
                await qbit_login(c2)
                await qbit_remove_tags(c2, [original_hash],
                                       f"{RACE_TAG_PFX}-{race_key}")
                await qbit_delete(c2, race_hashes, delete_files=True)
                await qbit_sweep_orphans(c2, race_key, [])

    active_races.pop(race_key, None)
    log.info(f"[{race_key}] Race complete")


async def declare_winner(client: httpx.AsyncClient,
                          race_key: str, app_name: str, media_id: int,
                          original_hash: str, all_hashes: List[str],
                          winner_hash: str, torrent_map: dict, save_path: str):
    """
    Keep winner, delete all tracked losers, sweep untracked orphans,
    strip race tag from winner, then trigger arr import if winner ≠ original.
    """
    race_tag     = f"{RACE_TAG_PFX}-{race_key}"
    loser_hashes = [h for h in all_hashes if h != winner_hash]

    # 1. Delete tracked losers
    await qbit_delete(client, loser_hashes, delete_files=True)
    log.info(f"[{race_key}] Deleted {len(loser_hashes)} tracked loser(s)")

    # 2. Sweep any untracked race-tagged extras (extras added but not in race_hashes)
    await qbit_sweep_orphans(client, race_key, [winner_hash])

    # 3. Remove race tag from winner so it stays clean
    await qbit_remove_tags(client, [winner_hash], race_tag)
    log.info(f"[{race_key}] Race tag stripped from winner")

    if winner_hash != original_hash:
        log.info(f"[{race_key}] Winner is a race extra — triggering arr import")
        await arr_remove_from_queue(client, app_name, media_id, original_hash)
        winner_t = torrent_map.get(winner_hash, {})
        winner_content_path = (winner_t.get("content_path")
                               or winner_t.get("save_path")
                               or save_path)
        await arr_trigger_import(client, app_name, winner_content_path)
    else:
        log.info(f"[{race_key}] Original arr grab won — import flows normally")


async def start_race(app_name: str, media_id: int, race_key: str,
                      grabbed_title: str, quality_name: str):
    """
    Background task: wait for arr to register the grab, then kick off the race.
    All extras are added first; a single timed wait+query resolves their hashes.
    """
    save_path = TV_PATH   if app_name == "sonarr" else MOVIE_PATH
    category  = "tv"      if app_name == "sonarr" else "movies"
    race_tag  = f"{RACE_TAG_PFX}-{race_key}"

    active_races[race_key] = {"app": app_name, "media_id": media_id,
                               "started": time.time(), "status": "starting"}

    log.info(f"[{race_key}] Race starting — waiting for arr queue entry")
    await asyncio.sleep(10)

    async with httpx.AsyncClient() as client:
        await qbit_login(client)

        # Retry up to 3× to find the original hash
        original_hash: Optional[str] = None
        for attempt in range(3):
            original_hash = await arr_get_queue_hash(client, app_name, media_id)
            if original_hash:
                break
            log.warning(f"[{race_key}] Hash not in queue (attempt {attempt+1}/3), retrying…")
            await asyncio.sleep(15)

        if not original_hash:
            log.error(f"[{race_key}] Could not find original hash — aborting race")
            active_races.pop(race_key, None)
            return

        await qbit_add_tags(client, [original_hash], race_tag)
        log.info(f"[{race_key}] Original hash: {original_hash}")

        candidates    = await arr_get_candidates(client, app_name, media_id,
                                                  grabbed_title, quality_name)
        extras_needed = RACE_COUNT - 1
        added_count   = 0

        # Step 1: add all extras to qBit in one pass
        for cand in candidates[:extras_needed]:
            log.info(f"[{race_key}] Adding extra: {cand['title']} "
                     f"({cand['seeders']}s/{cand['leechers']}l  "
                     f"{cand['size']//1024**2:.0f}MB)")
            ok = await qbit_add(client, cand["url"], category, save_path, race_tag)
            if ok:
                added_count += 1
            else:
                log.warning(f"[{race_key}] Failed to queue extra: {cand['title']}")

        # Step 2: single wait for qBit to process all adds, then resolve hashes by tag
        race_hashes: List[str] = []
        if added_count > 0:
            wait_secs = 3 + added_count * 2   # scale with count
            log.info(f"[{race_key}] Waiting {wait_secs}s for qBit to index {added_count} extra(s)…")
            await asyncio.sleep(wait_secs)
            tagged = await qbit_info(client, tag=race_tag)
            for t in tagged:
                h = t["hash"].lower()
                if h != original_hash and h not in race_hashes:
                    race_hashes.append(h)
                    log.info(f"[{race_key}] Extra registered: {t.get('name','?')[:45]}  {h[:12]}…")

        log.info(f"[{race_key}] Race set: 1 original + {len(race_hashes)} extra(s)")
        active_races[race_key].update({"status": "racing",
                                        "original": original_hash,
                                        "extras": race_hashes})

    await monitor_race(race_key, app_name, media_id,
                       original_hash, race_hashes, save_path)


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_orphan_sweep():
    """
    On (re)start, strip any stale race tags from torrents left over from a
    prior run. We strip rather than delete so in-progress downloads survive.
    """
    await asyncio.sleep(5)   # let qBit settle after daemon start
    try:
        async with httpx.AsyncClient() as client:
            await qbit_login(client)
            all_torrents = await qbit_info(client)   # no filter = all
            stale = [t for t in all_torrents
                     if RACE_TAG_PFX in t.get("tags", "")]
            if stale:
                log.warning(f"[startup] Stripping stale race tags from "
                            f"{len(stale)} torrent(s)")
                for t in stale:
                    for tag in t.get("tags", "").split(","):
                        tag = tag.strip()
                        if tag.startswith(RACE_TAG_PFX):
                            await qbit_remove_tags(client, [t["hash"].lower()], tag)
            else:
                log.info("[startup] No stale race-tagged torrents found")
    except Exception as e:
        log.warning(f"[startup] Orphan sweep failed: {e}")


# ── Webhook endpoints ─────────────────────────────────────────────────────────

@app.post("/webhook/{app_name}")
async def webhook(app_name: str, request: Request, bg: BackgroundTasks):
    """Receive Sonarr / Radarr on_grab webhook."""
    if app_name not in ("sonarr", "radarr"):
        return {"status": "error", "detail": f"Unknown app: {app_name}"}

    payload = await request.json()
    event   = payload.get("eventType", "")

    if event == "Test":
        log.info(f"[{app_name}] Webhook test received")
        return {"status": "test ok"}

    if event != "Grab":
        return {"status": "ignored", "eventType": event}

    try:
        if app_name == "sonarr":
            episodes = payload.get("episodes", [])
            if not episodes:
                return {"status": "error", "detail": "no episodes in payload"}
            media_id = episodes[0]["id"]
        else:
            media_id = payload["movie"]["id"]

        grabbed_title = payload["release"]["releaseTitle"]
        quality_name  = payload["release"]["quality"]
    except KeyError as e:
        log.error(f"[{app_name}] Missing field in payload: {e}")
        return {"status": "error", "detail": str(e)}

    if TRAP_PATTERNS.search(grabbed_title):
        log.warning(f"[{app_name}] Original grab is a trap format — skipping: {grabbed_title}")
        return {"status": "skipped", "reason": "trap format"}

    race_key = f"{app_name}-{media_id}"
    if race_key in active_races:
        log.info(f"[{race_key}] Race already active — ignoring duplicate webhook")
        return {"status": "duplicate", "race_key": race_key}

    log.info(f"[{race_key}] Grab event: {grabbed_title!r} ({quality_name})")
    bg.add_task(start_race, app_name, media_id, race_key, grabbed_title, quality_name)
    return {"status": "race queued", "race_key": race_key,
            "title": grabbed_title, "quality": quality_name}


@app.get("/status")
async def status():
    return {"active_races": len(active_races),
            "races": {k: {**v, "age_s": int(time.time() - v.get("started", time.time()))}
                      for k, v in active_races.items()}}


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.2.0"}
