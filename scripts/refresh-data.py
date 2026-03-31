#!/usr/bin/env python3
"""
UpperDecky Data Refresh Script
Fetches live MLB data for the current season and generates all JSON files
needed by the site. Designed to run in GitHub Actions daily.

Data sources:
- MLB Stats API (statsapi.mlb.com) - basic stats, rosters, player info
- Baseball Savant (baseballsavant.mlb.com) - Statcast data (EV, barrel rate, etc.)
"""

import json
import os
import sys
import csv
import io
import math
import time
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

SEASON = 2026
MIN_PA = 1  # Minimum plate appearances to include (1 for early season, bump to ~20 mid-season)
API_BASE = "https://statsapi.mlb.com/api/v1"
SAVANT_BASE = "https://baseballsavant.mlb.com"
OUT_DIR = os.environ.get("OUT_DIR", "api")

# MLB API abbreviation -> site abbreviation (fix mismatches)
ABBR_OVERRIDES = {
    "ATH": "OAK",   # Oakland Athletics
    "AZ": "ARI",    # Arizona Diamondbacks
}

def fetch_json(url, retries=3):
    """Fetch JSON from a URL with retries."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "UpperDecky/1.0"})
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except (URLError, HTTPError) as e:
            print(f"  Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    print(f"  FAILED: {url}")
    return None

def fetch_csv(url, retries=3):
    """Fetch CSV from a URL and return list of dicts."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "UpperDecky/1.0"})
            with urlopen(req, timeout=30) as resp:
                text = resp.read().decode("utf-8-sig")
                reader = csv.DictReader(io.StringIO(text))
                return list(reader)
        except (URLError, HTTPError) as e:
            print(f"  Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    print(f"  FAILED: {url}")
    return []

def safe_float(val, default=None):
    try:
        v = float(val)
        return v if not math.isnan(v) else default
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

def write_json(path, data):
    full = os.path.join(OUT_DIR, path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"  Wrote {full}")

# ============================================================
# 1. TEAMS
# ============================================================
def fetch_teams():
    print("\n[1/6] Fetching teams...")
    data = fetch_json(f"{API_BASE}/teams?sportId=1&season={SEASON}")
    if not data:
        return []
    teams = []
    for t in data.get("teams", []):
        raw_abbr = t.get("abbreviation", "")
        abbr = ABBR_OVERRIDES.get(raw_abbr, raw_abbr)
        teams.append({
            "team_id": t["id"],
            "abbreviation": abbr,
            "name": t.get("teamName", ""),
            "full_name": t.get("name", ""),
            "league": t.get("league", {}).get("abbreviation", ""),
            "division": t.get("division", {}).get("name", "").replace("American League ", "").replace("National League ", ""),
            "player_count": 0  # Updated after fetching batters
        })
    print(f"  Found {len(teams)} teams")
    return teams

# ============================================================
# 2. BATTERS - Basic stats from MLB Stats API
# ============================================================
def fetch_batters(teams):
    print("\n[2/6] Fetching batter stats from MLB API...")

    # Build team_id -> abbreviation lookup from teams data
    team_abbr_map = {t["team_id"]: t["abbreviation"] for t in teams}

    # Get player pool first
    players_url = f"{API_BASE}/sports/1/players?season={SEASON}&gameType=R"
    players_data = fetch_json(players_url)
    player_info = {}
    if players_data:
        for p in players_data.get("people", []):
            pid = p["id"]
            pos = p.get("primaryPosition", {}).get("abbreviation", "")
            # Skip pitchers
            if pos == "P":
                continue
            team_id = p.get("currentTeam", {}).get("id", 0)
            team_abbr = team_abbr_map.get(team_id, "UNK")
            player_info[pid] = {
                "batter_id": pid,
                "full_name": p.get("fullFMLName", p.get("fullName", "")),
                "team": team_abbr,
                "position": pos,
                "bats": p.get("batSide", {}).get("code", ""),
                "headshot_url": f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/{pid}/headshot/67/current",
                "height_display": p.get("height", ""),
                "height_inches": 0,
                "weight_lbs": safe_int(p.get("weight", 0)),
                "age": safe_int(p.get("currentAge", 0)),
            }
            # Parse height to inches
            h = p.get("height", "")
            if "'" in h:
                parts = h.replace('"', '').split("'")
                try:
                    feet = int(parts[0].strip())
                    inches = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else 0
                    player_info[pid]["height_inches"] = feet * 12 + inches
                except ValueError:
                    pass

    print(f"  Found {len(player_info)} position players in roster")

    # Now get season hitting stats for all players
    batters = {}
    offset = 0
    while True:
        stats_url = (f"{API_BASE}/stats?stats=season&season={SEASON}&group=hitting"
                     f"&sportId=1&limit=500&offset={offset}&gameType=R")
        data = fetch_json(stats_url)
        if not data or not data.get("stats"):
            break
        splits = data["stats"][0].get("splits", [])
        if not splits:
            break
        for sp in splits:
            pid = sp.get("player", {}).get("id")
            if pid not in player_info:
                continue
            s = sp.get("stat", {})
            pa = safe_int(s.get("plateAppearances", 0))
            if pa < MIN_PA:
                continue
            info = player_info[pid].copy()
            ab = safe_int(s.get("atBats", 0))
            info.update({
                "pa": pa,
                "ab": ab,
                "hits": safe_int(s.get("hits", 0)),
                "doubles": safe_int(s.get("doubles", 0)),
                "triples": safe_int(s.get("triples", 0)),
                "home_runs": safe_int(s.get("homeRuns", 0)),
                "rbi": safe_int(s.get("rbi", 0)),
                "walks": safe_int(s.get("baseOnBalls", 0)),
                "strikeouts": safe_int(s.get("strikeOuts", 0)),
                "hbp": safe_int(s.get("hitByPitch", 0)),
                "ba": safe_float(s.get("avg", "0").replace(".---", "0"), 0),
                "obp": safe_float(s.get("obp", "0").replace(".---", "0"), 0),
                "slg": safe_float(s.get("slg", "0").replace(".---", "0"), 0),
                "ops": safe_float(s.get("ops", "0").replace(".---", "0"), 0),
            })
            batters[pid] = info
        offset += 500
        if len(splits) < 500:
            break
        time.sleep(0.5)

    print(f"  Got basic stats for {len(batters)} batters")
    return batters

# ============================================================
# 3. STATCAST DATA from Baseball Savant
# ============================================================
def fetch_statcast(batters):
    print("\n[3/6] Fetching Statcast data from Baseball Savant...")
    url = (f"{SAVANT_BASE}/leaderboard/statcast"
           f"?type=batter&year={SEASON}&position=&team=&min={MIN_PA}&csv=true")
    rows = fetch_csv(url)
    print(f"  Got {len(rows)} Statcast entries")

    statcast_map = {}
    for row in rows:
        pid = safe_int(row.get("player_id", 0))
        if pid:
            statcast_map[pid] = {
                "avg_launch_speed": safe_float(row.get("avg_hit_speed")),
                "hard_hit_rate": safe_float(row.get("ev95percent")),
                "barrel_rate": safe_float(row.get("brl_percent")),
                "sweet_spot_rate": safe_float(row.get("anglesweetspotpercent")),
                "avg_launch_angle": safe_float(row.get("avg_hit_angle")),
            }

    # Also get expected stats (xBA, xSLG, xwOBA)
    xstats_url = (f"{SAVANT_BASE}/leaderboard/expected_statistics"
                  f"?type=batter&year={SEASON}&position=&team=&min={MIN_PA}&csv=true")
    xrows = fetch_csv(xstats_url)
    print(f"  Got {len(xrows)} expected stats entries")

    for row in xrows:
        pid = safe_int(row.get("player_id", 0))
        if pid and pid in statcast_map:
            statcast_map[pid]["xba"] = safe_float(row.get("est_ba"))
            statcast_map[pid]["xslg"] = safe_float(row.get("est_slg"))
            statcast_map[pid]["xwoba"] = safe_float(row.get("est_woba"))
        elif pid:
            statcast_map[pid] = {
                "xba": safe_float(row.get("est_ba")),
                "xslg": safe_float(row.get("est_slg")),
                "xwoba": safe_float(row.get("est_woba")),
            }

    # Merge Statcast into batters
    merged = 0
    for pid, sc in statcast_map.items():
        if pid in batters:
            batters[pid].update({k: v for k, v in sc.items() if v is not None})
            merged += 1

    # Set defaults for batters missing Statcast data
    for pid in batters:
        for key in ["avg_launch_speed", "hard_hit_rate", "barrel_rate",
                     "sweet_spot_rate", "avg_launch_angle", "xba", "xslg", "xwoba"]:
            if key not in batters[pid]:
                batters[pid][key] = None

    print(f"  Merged Statcast data for {merged} batters")
    return batters

# ============================================================
# 4. COMPUTE DERIVED STATS (leaderboards, discipline, etc.)
# ============================================================
def compute_derived(batters):
    print("\n[4/6] Computing derived stats...")

    for pid, b in batters.items():
        ab = b.get("ab", 0)
        hr = b.get("home_runs", 0)
        hbp = b.get("hbp", 0)
        weight = b.get("weight_lbs", 0)

        # Career stats not available from this API - use current season only
        b["career_ab"] = ab
        b["career_hr"] = hr
        b["career_hbp"] = hbp
        b["career_ab_per_hr"] = round(ab / hr, 2) if hr > 0 else 0
        b["abs_since_last_hr"] = 0  # Would need game log data
        b["abs_since_last_hbp"] = 0

        # Due-for status
        barrel_rate = b.get("barrel_rate") or 0
        if barrel_rate >= 10 and hr == 0 and ab >= 10:
            b["due_for_status"] = "OVERDUE"
            b["due_for_ratio"] = round(barrel_rate / max(ab, 1), 3)
        elif barrel_rate >= 8:
            b["due_for_status"] = "ON PACE"
            b["due_for_ratio"] = round(barrel_rate / 100, 3)
        else:
            b["due_for_status"] = "COLD STREAK"
            b["due_for_ratio"] = 0

        # Thicc boy index: HR power relative to weight
        if weight > 0 and hr > 0:
            b["thicc_boy_index"] = round((hr / weight) * 1000, 2)
        else:
            b["thicc_boy_index"] = 0

        # Plate discipline
        chase = b.get("chase_rate") or 30  # Default if not available
        whiff = b.get("whiff_rate") or 25
        bb_rate = (b.get("walks", 0) / b.get("pa", 1)) * 100 if b.get("pa", 0) > 0 else 0
        k_rate = (b.get("strikeouts", 0) / b.get("pa", 1)) * 100 if b.get("pa", 0) > 0 else 0
        discipline = max(0, 100 - chase - (whiff * 0.5) + (bb_rate * 2) - (k_rate * 0.5))
        b["chase_rate"] = chase
        b["whiff_rate"] = whiff
        b["plate_discipline_score"] = round(discipline, 2)
        if discipline >= 80:
            b["plate_discipline_grade"] = "A"
        elif discipline >= 65:
            b["plate_discipline_grade"] = "B"
        elif discipline >= 50:
            b["plate_discipline_grade"] = "C"
        elif discipline >= 35:
            b["plate_discipline_grade"] = "D"
        else:
            b["plate_discipline_grade"] = "F"

    print(f"  Computed derived stats for {len(batters)} batters")

# ============================================================
# 5. GENERATE OUTPUT FILES
# ============================================================
def generate_outputs(batters, teams):
    print("\n[5/6] Generating output files...")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    batter_list = sorted(batters.values(), key=lambda x: x.get("ops", 0), reverse=True)

    # Update team player counts
    team_counts = {}
    for b in batter_list:
        t = b.get("team", "UNK")
        team_counts[t] = team_counts.get(t, 0) + 1
    for t in teams:
        t["player_count"] = team_counts.get(t["abbreviation"], 0)

    # --- dashboard.json ---
    write_json("dashboard.json", {
        "total_batters": len(batter_list),
        "total_teams": len(teams),
        "season": SEASON,
        "last_updated": now
    })

    # --- teams.json ---
    write_json("teams.json", teams)

    # --- batters.json (summary list for players page) ---
    summary_fields = ["batter_id", "full_name", "team", "position", "bats",
                       "headshot_url", "pa", "ab", "hits", "home_runs", "rbi",
                       "ba", "obp", "slg", "ops", "avg_launch_speed",
                       "hard_hit_rate", "barrel_rate", "xwoba", "sweet_spot_rate"]
    batters_summary = []
    for b in batter_list:
        row = {k: b.get(k) for k in summary_fields}
        batters_summary.append(row)
    write_json("batters.json", batters_summary)

    # --- Individual player files ---
    for b in batter_list:
        pid = b["batter_id"]
        write_json(f"batters/{pid}/summary.json", b)

    # --- Season summary ---
    write_json(f"batters/{SEASON}/summary.json", {
        "season": SEASON,
        "total_batters": len(batter_list),
        "last_updated": now
    })

    # --- Leaderboards ---
    # Due-for (barrel drought)
    due_for = sorted(
        [b for b in batter_list if (b.get("barrel_rate") or 0) > 0],
        key=lambda x: (x.get("barrel_rate", 0) + x.get("hard_hit_rate", 0)) / max(x.get("home_runs", 1), 1),
        reverse=True
    )[:20]
    due_for_out = []
    for b in due_for:
        due_for_out.append({
            "batter_id": b["batter_id"], "full_name": b["full_name"], "team": b["team"],
            "barrel_rate": b.get("barrel_rate"), "hard_hit_rate": b.get("hard_hit_rate"),
            "home_runs": b.get("home_runs", 0), "ab": b.get("ab", 0),
            "headshot_url": b["headshot_url"],
            "due_for_status": b.get("due_for_status"), "due_for_ratio": b.get("due_for_ratio")
        })
    write_json("leaderboards/due-for.json", due_for_out)

    # Thicc boy
    thicc = sorted(
        [b for b in batter_list if b.get("weight_lbs", 0) > 0 and b.get("home_runs", 0) > 0],
        key=lambda x: x.get("thicc_boy_index", 0), reverse=True
    )[:50]
    thicc_out = []
    for i, b in enumerate(thicc):
        thicc_out.append({
            "rank": i + 1, "batter_id": b["batter_id"], "full_name": b["full_name"],
            "team": b["team"], "headshot_url": b["headshot_url"],
            "weight_lbs": b["weight_lbs"], "home_runs": b["home_runs"],
            "thicc_boy_index": b["thicc_boy_index"]
        })
    write_json("leaderboards/thicc-boy.json", thicc_out)

    # Plate discipline
    disc = sorted(batter_list, key=lambda x: x.get("plate_discipline_score", 0), reverse=True)[:50]
    disc_out = []
    for i, b in enumerate(disc):
        disc_out.append({
            "rank": i + 1, "batter_id": b["batter_id"], "full_name": b["full_name"],
            "team": b["team"], "headshot_url": b["headshot_url"],
            "plate_discipline_score": b["plate_discipline_score"],
            "plate_discipline_grade": b["plate_discipline_grade"],
            "walks": b.get("walks", 0), "strikeouts": b.get("strikeouts", 0),
            "chase_rate": b.get("chase_rate"), "whiff_rate": b.get("whiff_rate")
        })
    write_json("leaderboards/plate-discipline.json", disc_out)

    # HBP
    hbp_sorted = sorted(
        [b for b in batter_list if b.get("hbp", 0) > 0],
        key=lambda x: x.get("hbp", 0), reverse=True
    )[:50]
    hbp_out = []
    for i, b in enumerate(hbp_sorted):
        hbp_out.append({
            "rank": i + 1, "batter_id": b["batter_id"], "full_name": b["full_name"],
            "team": b["team"], "headshot_url": b["headshot_url"],
            "hbp": b["hbp"], "pa": b["pa"]
        })
    write_json("leaderboards/hbp.json", hbp_out)

    # Full moon (placeholder - needs game-by-game lunar data)
    # For now, just copy all batters with basic data
    moon_out = []
    for b in batter_list:
        moon_out.append({
            "batter_id": b["batter_id"], "full_name": b["full_name"],
            "team": b["team"], "headshot_url": b["headshot_url"],
            "full_moon_ab": 0, "full_moon_hits": 0, "full_moon_ba": 0,
            "normal_ba": b.get("ba", 0), "ba_diff": 0
        })
    write_json("leaderboards/full-moon.json", moon_out)

    # --- Scatter data ---
    height_hr = [{"batter_id": b["batter_id"], "full_name": b["full_name"], "team": b["team"],
                   "x": b.get("height_inches", 0), "y": b.get("home_runs", 0),
                   "headshot_url": b["headshot_url"]}
                  for b in batter_list if b.get("height_inches", 0) > 0]
    write_json("scatter/height-hr.json", height_hr)

    weight_hr = [{"batter_id": b["batter_id"], "full_name": b["full_name"], "team": b["team"],
                   "x": b.get("weight_lbs", 0), "y": b.get("home_runs", 0),
                   "headshot_url": b["headshot_url"]}
                  for b in batter_list if b.get("weight_lbs", 0) > 0]
    write_json("scatter/weight-hr.json", weight_hr)

    print(f"  Generated all output files for {len(batter_list)} batters")
    return len(batter_list)

# ============================================================
# 6. HIT OR SPIT — DAILY PICKS + PREDICTION TRACKING
# ============================================================
import hashlib
import random as _random

def _seeded_rng(date_str):
    """Create a seeded RNG from a date string so picks are deterministic per day."""
    seed = int(hashlib.md5(date_str.encode()).hexdigest()[:8], 16)
    return _random.Random(seed)

def get_todays_schedule():
    """Get team abbreviations playing today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"{API_BASE}/schedule?sportId=1&date={today}"
    data = fetch_json(url)
    if not data:
        return set(), today
    playing = set()
    teams_data = fetch_json(f"{API_BASE}/teams?sportId=1&season={SEASON}")
    id_to_abbr = {}
    if teams_data:
        for t in teams_data.get("teams", []):
            raw = t.get("abbreviation", "")
            id_to_abbr[t["id"]] = ABBR_OVERRIDES.get(raw, raw)
    for d in data.get("dates", []):
        for g in d.get("games", []):
            for side in ["away", "home"]:
                tid = g.get("teams", {}).get(side, {}).get("team", {}).get("id", 0)
                abbr = id_to_abbr.get(tid, "")
                if abbr:
                    playing.add(abbr)
    return playing, today

def get_yesterdays_hr_hitters():
    """Get set of player IDs who hit a HR yesterday."""
    from datetime import timedelta
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    url = f"{API_BASE}/schedule?sportId=1&date={yesterday}"
    data = fetch_json(url)
    hr_pids = set()
    if not data:
        return hr_pids, yesterday
    for d in data.get("dates", []):
        for g in d.get("games", []):
            gpk = g.get("gamePk")
            status = g.get("status", {}).get("codedGameState", "")
            if status != "F":  # Only final games
                continue
            box = fetch_json(f"{API_BASE}/game/{gpk}/boxscore")
            if not box:
                continue
            for side in ["away", "home"]:
                players = box.get("teams", {}).get(side, {}).get("players", {})
                for pid_key, pdata in players.items():
                    hr = safe_int(pdata.get("stats", {}).get("batting", {}).get("homeRuns", 0))
                    if hr > 0:
                        hr_pids.add(pdata.get("person", {}).get("id", 0))
    return hr_pids, yesterday

def compute_hr_probability(b):
    """Estimate HR probability for a batter (0-100 scale). Used to make the AI pick."""
    barrel = b.get("barrel_rate") or 0
    hard_hit = b.get("hard_hit_rate") or 0
    hr = b.get("home_runs", 0)
    ab = max(b.get("ab", 0), 1)
    hr_rate = hr / ab  # Season HR/AB rate
    # Weighted formula: barrel rate is best predictor, then HR rate, then hard hit
    score = (barrel * 0.5) + (hr_rate * 200) + (hard_hit * 0.15)
    return min(round(score, 1), 99)

def fetch_prev_season_stats(player_ids):
    """Fetch previous season (2025) stats for specific players."""
    print(f"  Fetching 2025 stats for {len(player_ids)} players...")
    prev = SEASON - 1
    stats_2025 = {}
    for pid in player_ids:
        url = f"{API_BASE}/people/{pid}/stats?stats=season&season={prev}&group=hitting&sportId=1"
        data = fetch_json(url)
        if not data:
            continue
        for st in data.get("stats", []):
            for sp in st.get("splits", []):
                s = sp.get("stat", {})
                pa = safe_int(s.get("plateAppearances", 0))
                if pa < 30:  # Need at least 30 PA to be meaningful
                    continue
                stats_2025[pid] = {
                    "prev_hr": safe_int(s.get("homeRuns", 0)),
                    "prev_ba": safe_float(s.get("avg", "0").replace(".---", "0"), 0),
                    "prev_ops": safe_float(s.get("ops", "0").replace(".---", "0"), 0),
                    "prev_ab": safe_int(s.get("atBats", 0)),
                    "prev_pa": pa,
                    "prev_rbi": safe_int(s.get("rbi", 0)),
                    "prev_season": prev,
                }
        time.sleep(0.1)  # Be polite to the API
    print(f"  Got 2025 stats for {len(stats_2025)} of {len(player_ids)} players")
    return stats_2025

def generate_hos_picks(batters):
    """Generate Hit or Spit daily picks, grade yesterday's picks, maintain record."""
    print("\n[6/6] Generating Hit or Spit picks...")
    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    # Load existing HoS tracking file
    hos_path = os.path.join(OUT_DIR, "hos-tracker.json")
    tracker = {"season_record": {"wins": 0, "losses": 0}, "history": [], "current_picks": [], "last_updated": ""}
    if os.path.exists(hos_path):
        try:
            with open(hos_path) as f:
                tracker = json.load(f)
        except:
            pass

    # --- STEP 1: Grade yesterday's picks ---
    prev_picks = tracker.get("current_picks", [])
    if prev_picks and prev_picks[0].get("date") != today_str:
        hr_pids, yesterday = get_yesterdays_hr_hitters()
        print(f"  Yesterday ({yesterday}): {len(hr_pids)} players hit HRs")
        day_w = 0
        day_l = 0
        graded = []
        for pick in prev_picks:
            pid = pick["batter_id"]
            prediction = pick["prediction"]  # "HIT" or "SPIT"
            hit_hr = pid in hr_pids
            # HIT prediction = we predicted HR. Correct if they hit one.
            # SPIT prediction = we predicted no HR. Correct if they didn't.
            correct = (prediction == "HIT" and hit_hr) or (prediction == "SPIT" and not hit_hr)
            if correct:
                day_w += 1
            else:
                day_l += 1
            graded.append({
                **pick,
                "actual_hr": hit_hr,
                "result": "W" if correct else "L"
            })
        tracker["season_record"]["wins"] += day_w
        tracker["season_record"]["losses"] += day_l
        # Add to history (keep last 30 days)
        tracker["history"].append({
            "date": prev_picks[0].get("date", yesterday),
            "picks": graded,
            "day_record": f"{day_w}-{day_l}"
        })
        tracker["history"] = tracker["history"][-30:]
        print(f"  Graded yesterday: {day_w}W-{day_l}L (Season: {tracker['season_record']['wins']}-{tracker['season_record']['losses']})")
    elif prev_picks and prev_picks[0].get("date") == today_str:
        print(f"  Today's picks already generated, skipping...")

    # --- STEP 2: Generate today's picks ---
    playing_teams, _ = get_todays_schedule()
    print(f"  Teams playing today: {len(playing_teams)} — {', '.join(sorted(playing_teams))}")

    # Filter to batters on teams playing today
    batter_list = list(batters.values())
    if playing_teams:
        candidates = [b for b in batter_list if b.get("team") in playing_teams]
    else:
        # Off day — pick from full pool
        candidates = batter_list

    if not candidates:
        candidates = batter_list

    # Score all candidates and pick top 15 interesting matchups
    # Interesting = mix of high-HR-probability and low-HR-probability batters
    for c in candidates:
        c["_hr_prob"] = compute_hr_probability(c)

    candidates.sort(key=lambda x: x["_hr_prob"], reverse=True)

    # Pick 8 from top tier (likely HIT picks) and 7 from bottom tier (likely SPIT picks)
    rng = _seeded_rng(today_str + "hos")
    top_pool = candidates[:max(len(candidates)//3, 15)]
    bot_pool = candidates[len(candidates)//2:]
    rng.shuffle(top_pool)
    rng.shuffle(bot_pool)
    deck = top_pool[:8] + bot_pool[:7]
    rng.shuffle(deck)  # Mix them up

    # Fetch 2025 stats for the 15 picked players (so cards don't show tiny sample size junk)
    pick_ids = [b["batter_id"] for b in deck[:15]]
    prev_stats = fetch_prev_season_stats(pick_ids)

    # Make predictions
    picks = []
    for b in deck[:15]:
        prob = b.get("_hr_prob", 0)
        # Threshold for HIT prediction (higher = more selective)
        prediction = "HIT" if prob >= 12 else "SPIT"
        pid = b["batter_id"]
        pa_2026 = b.get("pa", 0)
        prev = prev_stats.get(pid, {})

        # Card display stats: use 2025 full-season when 2026 sample is tiny (<50 PA)
        # Once a batter has 50+ PA in 2026, switch to current season
        if pa_2026 >= 50 or not prev:
            card_hr = b.get("home_runs", 0)
            card_ba = b.get("ba", 0)
            card_ops = b.get("ops", 0)
            card_barrel = b.get("barrel_rate")
            card_label = str(SEASON)
        else:
            card_hr = prev.get("prev_hr", 0)
            card_ba = prev.get("prev_ba", 0)
            card_ops = prev.get("prev_ops", 0)
            card_barrel = None  # Statcast barrel not in basic stats
            card_label = str(SEASON - 1)

        picks.append({
            "date": today_str,
            "batter_id": pid,
            "full_name": b.get("full_name", ""),
            "team": b.get("team", ""),
            "position": b.get("position", ""),
            "headshot_url": b.get("headshot_url", ""),
            "prediction": prediction,
            "hr_prob": prob,
            # Card display stats (may be 2025 or 2026 depending on sample size)
            "card_hr": card_hr,
            "card_ba": card_ba,
            "card_ops": card_ops,
            "card_barrel": card_barrel,
            "card_season_label": card_label,
            # Raw current season stats (always 2026)
            "home_runs": b.get("home_runs", 0),
            "ba": b.get("ba", 0),
            "ops": b.get("ops", 0),
            "barrel_rate": b.get("barrel_rate"),
            "hard_hit_rate": b.get("hard_hit_rate"),
            "avg_launch_speed": b.get("avg_launch_speed"),
        })

    tracker["current_picks"] = picks
    tracker["last_updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Clean temp keys
    for b in batters.values():
        b.pop("_hr_prob", None)

    # Write the tracker file (persistent — committed to repo)
    write_json("hos-tracker.json", tracker)

    # Write today's picks as a separate file for the front-end
    write_json("hos-picks.json", {
        "date": today_str,
        "picks": picks,
        "season_record": tracker["season_record"],
        "last_5_days": [{
            "date": h["date"],
            "day_record": h["day_record"]
        } for h in tracker["history"][-5:]]
    })

    print(f"  Generated {len(picks)} picks for {today_str}")
    win = tracker['season_record']['wins']
    loss = tracker['season_record']['losses']
    total = win + loss
    pct = round(win / total * 100, 1) if total > 0 else 0
    print(f"  Season prediction record: {win}-{loss} ({pct}%)")

# ============================================================
# 7. PRESS BOX — AI ANALYST PICKS + RECORDS
# ============================================================
ANALYSTS = [
    {"id": "sarah", "name": "Sarah Jenkins", "role": "The Analyst", "title": "Data Scientist",
     "style": "data"},  # Favors strong Statcast + advanced stats
    {"id": "ace", "name": '"Ace" Martinez', "role": "The Pitching Guru", "title": "Former Pitcher",
     "style": "pitching"},  # Focuses on pitching matchups, unders
    {"id": "pop", "name": '"Pop" Sullivan', "role": "The Old School Scout", "title": "40 Yrs in Baseball",
     "style": "scout"},  # Eye-test, big-name favorites
    {"id": "liam", "name": "Liam Chen", "role": "The Algorithm", "title": "Machine Learning",
     "style": "algo"},  # Pure numbers, highest confidence
    {"id": "mouth", "name": '"The Mouth"', "role": "The Hot Take Machine", "title": "Sports Radio Energy",
     "style": "chaos"},  # Underdogs, longshots, chaos
]

def generate_pressbox_picks(batters):
    """Generate daily Press Box analyst picks and track records."""
    print("\n[7/7] Generating Press Box analyst picks...")
    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    # Load existing tracker
    pb_path = os.path.join(OUT_DIR, "pressbox-tracker.json")
    tracker = {"analysts": {a["id"]: {"wins": 0, "losses": 0} for a in ANALYSTS},
               "current_day": "", "current_picks": {}, "history": []}
    if os.path.exists(pb_path):
        try:
            with open(pb_path) as f:
                tracker = json.load(f)
        except:
            pass

    # Get today's schedule
    url = f"{API_BASE}/schedule?sportId=1&date={today_str}&hydrate=probablePitcher"
    sched = fetch_json(url)
    games = []
    teams_data = fetch_json(f"{API_BASE}/teams?sportId=1&season={SEASON}")
    id_to_abbr = {}
    if teams_data:
        for t in teams_data.get("teams", []):
            raw = t.get("abbreviation", "")
            id_to_abbr[t["id"]] = ABBR_OVERRIDES.get(raw, raw)

    if sched:
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                away = g.get("teams", {}).get("away", {})
                home = g.get("teams", {}).get("home", {})
                away_abbr = id_to_abbr.get(away.get("team", {}).get("id", 0), "???")
                home_abbr = id_to_abbr.get(home.get("team", {}).get("id", 0), "???")
                away_pitcher = away.get("probablePitcher", {}).get("fullName", "TBD")
                home_pitcher = home.get("probablePitcher", {}).get("fullName", "TBD")
                games.append({
                    "gamePk": g.get("gamePk"),
                    "away": away_abbr, "home": home_abbr,
                    "away_pitcher": away_pitcher, "home_pitcher": home_pitcher
                })

    if not games:
        print("  No games today, skipping Press Box")
        write_json("pressbox.json", {"date": today_str, "analysts": [], "games_today": 0})
        return

    print(f"  {len(games)} games today")

    # --- Grade yesterday's picks ---
    prev_day = tracker.get("current_day", "")
    prev_picks = tracker.get("current_picks", {})
    if prev_day and prev_day != today_str and prev_picks:
        from datetime import timedelta
        yesterday = prev_day
        ysched = fetch_json(f"{API_BASE}/schedule?sportId=1&date={yesterday}")
        # Build results: {gamePk: {away_score, home_score}}
        results = {}
        if ysched:
            for d in ysched.get("dates", []):
                for g in d.get("games", []):
                    if g.get("status", {}).get("codedGameState") == "F":
                        gpk = g.get("gamePk")
                        away_score = g.get("teams", {}).get("away", {}).get("score", 0)
                        home_score = g.get("teams", {}).get("home", {}).get("score", 0)
                        away_id = g.get("teams", {}).get("away", {}).get("team", {}).get("id", 0)
                        home_id = g.get("teams", {}).get("home", {}).get("team", {}).get("id", 0)
                        results[gpk] = {
                            "away_abbr": id_to_abbr.get(away_id, "?"),
                            "home_abbr": id_to_abbr.get(home_id, "?"),
                            "away_score": away_score, "home_score": home_score,
                            "total_runs": away_score + home_score
                        }

        day_results = {}
        for aid, picks in prev_picks.items():
            w = 0
            l = 0
            for pick in picks:
                gpk = pick.get("gamePk")
                res = results.get(gpk)
                if not res:
                    continue
                ptype = pick.get("type")  # "winner" or "total"
                if ptype == "winner":
                    picked_team = pick.get("pick")
                    if res["away_score"] > res["home_score"]:
                        actual_winner = res["away_abbr"]
                    elif res["home_score"] > res["away_score"]:
                        actual_winner = res["home_abbr"]
                    else:
                        continue  # Tie / suspended
                    if picked_team == actual_winner:
                        w += 1
                    else:
                        l += 1
                elif ptype == "total":
                    line = pick.get("line", 8.5)
                    direction = pick.get("direction")  # "over" or "under"
                    actual = res["total_runs"]
                    if direction == "over" and actual > line:
                        w += 1
                    elif direction == "under" and actual < line:
                        w += 1
                    elif actual == line:
                        continue  # Push
                    else:
                        l += 1
            day_results[aid] = {"wins": w, "losses": l}
            if aid not in tracker["analysts"]:
                tracker["analysts"][aid] = {"wins": 0, "losses": 0}
            tracker["analysts"][aid]["wins"] += w
            tracker["analysts"][aid]["losses"] += l

        graded_summary = ", ".join([f"{a}: {day_results.get(a,{}).get('wins',0)}-{day_results.get(a,{}).get('losses',0)}" for a in day_results])
        print(f"  Graded {yesterday}: {graded_summary}")

    # --- Generate today's picks ---
    rng = _seeded_rng(today_str + "pressbox")
    batter_list = list(batters.values())

    all_picks = {}
    for analyst in ANALYSTS:
        aid = analyst["id"]
        style = analyst["style"]
        picks = []
        game_pool = list(games)
        rng.shuffle(game_pool)

        for g in game_pool[:3]:
            gpk = g["gamePk"]
            away = g["away"]
            home = g["home"]

            if style == "data":
                # Data-driven: pick home team more often (home advantage in stats)
                pick_team = home if rng.random() > 0.4 else away
                pick_text = f"{pick_team} ML vs {away if pick_team == home else home}"
                picks.append({"gamePk": gpk, "type": "winner", "pick": pick_team, "text": pick_text})
            elif style == "pitching":
                # Pitching guru: mix of winners and unders
                if len(picks) < 1:
                    pick_team = home if rng.random() > 0.45 else away
                    pick_text = f"{pick_team} ML vs {away if pick_team == home else home}"
                    picks.append({"gamePk": gpk, "type": "winner", "pick": pick_team, "text": pick_text})
                else:
                    line = rng.choice([7.0, 7.5, 8.0, 8.5])
                    pick_text = f"Under {line} \u2014 {away}/{home}"
                    picks.append({"gamePk": gpk, "type": "total", "direction": "under", "line": line, "text": pick_text})
            elif style == "scout":
                # Old school: always picks winners, favors big-market teams
                pick_team = home if rng.random() > 0.35 else away
                pick_text = f"{pick_team} -1.5 vs {away if pick_team == home else home}"
                picks.append({"gamePk": gpk, "type": "winner", "pick": pick_team, "text": pick_text})
            elif style == "algo":
                # Algorithm: mix of overs and winners, high confidence
                if len(picks) % 2 == 0:
                    pick_team = home if rng.random() > 0.42 else away
                    pick_text = f"{pick_team} ML vs {away if pick_team == home else home}"
                    picks.append({"gamePk": gpk, "type": "winner", "pick": pick_team, "text": pick_text})
                else:
                    line = rng.choice([8.0, 8.5, 9.0, 9.5])
                    direction = rng.choice(["over", "under"])
                    pick_text = f"{direction.capitalize()} {line} \u2014 {away}/{home}"
                    picks.append({"gamePk": gpk, "type": "total", "direction": direction, "line": line, "text": pick_text})
            elif style == "chaos":
                # Chaos: always picks underdogs (away teams), overs
                pick_team = away
                if len(picks) < 2:
                    pick_text = f"{pick_team} ML vs {home}"
                    picks.append({"gamePk": gpk, "type": "winner", "pick": pick_team, "text": pick_text})
                else:
                    line = rng.choice([9.5, 10.0, 10.5, 11.0])
                    pick_text = f"Over {line} \u2014 {away}/{home}"
                    picks.append({"gamePk": gpk, "type": "total", "direction": "over", "line": line, "text": pick_text})

        all_picks[aid] = picks

    tracker["current_day"] = today_str
    tracker["current_picks"] = all_picks

    # Write tracker (persists across days)
    write_json("pressbox-tracker.json", tracker)

    # Build front-end JSON
    conf_labels = {"data": "87% conf", "pitching": "72% conf", "algo": "94% conf", "scout": "91% conf", "chaos": "YOLO"}
    conf_emojis = {"data": "\U0001F525\U0001F525\U0001F525", "pitching": "\U0001F9CA\U0001F9CA",
                   "scout": "\U0001F4AA\U0001F4AA\U0001F4AA", "algo": "\U0001F916\U0001F916\U0001F916",
                   "chaos": "\U0001F92A\U0001F92A\U0001F92A"}
    output_analysts = []
    for analyst in ANALYSTS:
        aid = analyst["id"]
        rec = tracker["analysts"].get(aid, {"wins": 0, "losses": 0})
        w = rec["wins"]
        l = rec["losses"]
        total = w + l
        pct = f".{round(w / total * 1000):03d}" if total > 0 else ".000"
        output_analysts.append({
            "id": aid,
            "name": analyst["name"],
            "role": analyst["role"],
            "title": analyst["title"],
            "record": f"{w}-{l}",
            "pct": pct,
            "picks": [p["text"] for p in all_picks.get(aid, [])],
            "conf": conf_emojis.get(analyst["style"], "") + " " + conf_labels.get(analyst["style"], "")
        })

    write_json("pressbox.json", {
        "date": today_str,
        "analysts": output_analysts,
        "games_today": len(games)
    })

    for a in output_analysts:
        try:
            picks_preview = ', '.join(a['picks'][:2])
            print(f"  {a['name']:20s} {a['record']:>6s} ({a['pct']}) -- {picks_preview}")
        except UnicodeEncodeError:
            print(f"  {a['name']:20s} {a['record']:>6s} ({a['pct']}) -- [picks generated]")

# ============================================================
# 8. THE DUGOUT — DAILY HOT TAKE PROMPTS
# ============================================================
def generate_dugout_prompt(batters, teams):
    """Generate a daily hot take prompt based on current stats and matchups."""
    print("\n[8/8] Generating Dugout prompt...")
    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")
    rng = _seeded_rng(today_str + "dugout")

    batter_list = sorted(batters.values(), key=lambda x: x.get("ops", 0), reverse=True)

    # Get today's schedule for matchup-based prompts
    sched = fetch_json(f"{API_BASE}/schedule?sportId=1&date={today_str}")
    teams_data = fetch_json(f"{API_BASE}/teams?sportId=1&season={SEASON}")
    id_to_abbr = {}
    if teams_data:
        for t in teams_data.get("teams", []):
            raw = t.get("abbreviation", "")
            id_to_abbr[t["id"]] = ABBR_OVERRIDES.get(raw, raw)
    matchups = []
    if sched:
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                away = id_to_abbr.get(g.get("teams",{}).get("away",{}).get("team",{}).get("id",0), "")
                home = id_to_abbr.get(g.get("teams",{}).get("home",{}).get("team",{}).get("id",0), "")
                if away and home:
                    matchups.append((away, home))

    # Build prompt templates
    templates = []

    # Hot hitters
    hot = [b for b in batter_list if b.get("ba", 0) > 0.350 and b.get("pa", 0) >= 5]
    if hot:
        p = rng.choice(hot[:10])
        templates.append(f"{p['full_name'].split()[-1]} is hitting {p['ba']:.3f} so far. Legit or small sample size fraud?")

    # Cold stars
    cold = [b for b in batter_list if b.get("ba", 0) < 0.150 and b.get("pa", 0) >= 5]
    if cold:
        p = rng.choice(cold[:10])
        templates.append(f"{p['full_name'].split()[-1]} is hitting {p['ba']:.3f}. Time to panic or nah?")

    # HR leaders
    hr_leaders = sorted(batter_list, key=lambda x: x.get("home_runs", 0), reverse=True)
    if hr_leaders and hr_leaders[0].get("home_runs", 0) > 0:
        p = hr_leaders[0]
        templates.append(f"{p['full_name'].split()[-1]} leads the league with {p['home_runs']} HR. Will he hit 40+ this year?")

    # Matchup-based
    if matchups:
        m = rng.choice(matchups)
        templates.append(f"{m[0]} vs {m[1]} today. Who you got and why?")
        templates.append(f"Bold prediction for {m[0]} at {m[1]} tonight. Drop your hottest take.")

    # Evergreen baseball takes
    evergreen = [
        "What's the most overrated stat in baseball? Wrong answers only.",
        "If you could add one rule to baseball, what would it be?",
        "Who's winning MVP this year? Lock it in now.",
        "What team is going to surprise everyone this season?",
        "Most underrated player in baseball right now. Go.",
        "Pitch clock: best thing to happen to baseball or ruining the game?",
        "What's your earliest baseball memory? Drop it below.",
        "Name a player who's about to have a breakout season.",
        "Hot take: the DH should be eliminated. Agree or disagree?",
        "If you could watch one current player for the rest of their career, who?",
        "What ballpark has the best food? Don't say yours just because.",
        "Robot umps: ready or not, they're coming. Thoughts?",
        "Who's the most fun player to watch in baseball right now?",
        "Worst baseball take you've ever heard? Share the pain.",
        "If baseball had a trade deadline for fans, which fanbase would you join?",
    ]
    templates.extend(rng.sample(evergreen, min(3, len(evergreen))))

    # Pick today's prompt
    prompt = rng.choice(templates)

    # Write dugout prompt file
    write_json("dugout-prompt.json", {
        "date": today_str,
        "prompt": prompt,
        "comment_count": 0,  # Will be updated by the live function
    })

    print(f"  Prompt: {prompt}")

# ============================================================
# 9. CRUDE BARRELS — OIL PRICE + BARREL RATE SYSTEM
# ============================================================
EIA_API_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
EIA_API_KEY = "DEMO_KEY"  # Free demo key, 1000 requests/day
SAVANT_SEARCH = "https://baseballsavant.mlb.com/statcast_search/csv"

def fetch_oil_prices():
    """Fetch recent WTI crude oil prices from EIA."""
    print("\n[9a] Fetching WTI crude oil prices...")
    url = (f"{EIA_API_URL}?frequency=daily&data[0]=value"
           f"&facets[series][]=RWTC&sort[0][column]=period"
           f"&sort[0][direction]=desc&length=365&api_key={EIA_API_KEY}")
    data = fetch_json(url)
    if not data or "response" not in data:
        print("  Failed to fetch oil prices")
        return {}
    prices = {}
    rows = data.get("response", {}).get("data", [])
    prev_price = None
    # Sort chronologically
    rows.sort(key=lambda r: r.get("period", ""))
    for row in rows:
        date = row.get("period", "")
        price = safe_float(row.get("value"))
        if not date or price is None:
            continue
        delta = round(price - prev_price, 2) if prev_price is not None else 0
        pct_change = round(delta / prev_price, 4) if prev_price and prev_price != 0 else 0
        # Regime bucket
        if price < 60:
            regime = "cheap"
        elif price > 90:
            regime = "expensive"
        else:
            regime = "normal"
        is_spike = delta >= 5 or pct_change >= 0.05
        is_crash = delta <= -5 or pct_change <= -0.05
        prices[date] = {
            "date": date, "price": price, "delta": delta,
            "pct_change": pct_change, "regime": regime,
            "is_spike": is_spike, "is_crash": is_crash
        }
        prev_price = price
    print(f"  Got {len(prices)} daily oil prices")
    if prices:
        latest = list(prices.values())[-1]
        print(f"  Latest: ${latest['price']} ({latest['regime']}) on {latest['date']}")
    return prices

def fetch_barrel_events():
    """Fetch individual batted ball events with barrel flag from Savant."""
    print("\n[9b] Fetching barrel events from Baseball Savant...")
    # Get events from season start
    season_start = f"{SEASON}-03-20"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = (f"{SAVANT_SEARCH}?all=true&type=detail"
           f"&game_date_gt={season_start}&game_date_lt={today}"
           f"&player_type=batter&min_pitches=0&min_results=0")
    rows = fetch_csv(url)
    print(f"  Got {len(rows)} batted ball events")
    
    # Parse into structured data
    events = []
    for r in rows:
        batter_id = safe_int(r.get("batter", 0))
        game_date = r.get("game_date", "")
        lsa = r.get("launch_speed_angle", "")
        if not batter_id or not game_date:
            continue
        is_barrel = (lsa == "6")  # launch_speed_angle 6 = Barrel
        events.append({
            "batter_id": batter_id,
            "game_date": game_date,
            "is_barrel": is_barrel,
            "ev": safe_float(r.get("launch_speed")),
            "la": safe_float(r.get("launch_angle")),
        })
    
    barrels = sum(1 for e in events if e["is_barrel"])
    print(f"  {barrels} barrels out of {len(events)} batted balls")
    return events

def compute_crude_barrels(batters, oil_prices, barrel_events):
    """Compute Crude Barrel (Petro-Barrel) stats per hitter."""
    print("\n[9c] Computing Crude Barrel stats...")
    
    # Group events by batter
    batter_events = {}
    for e in barrel_events:
        pid = e["batter_id"]
        if pid not in batter_events:
            batter_events[pid] = []
        batter_events[pid].append(e)
    
    # Get the latest oil price for the widget
    sorted_prices = sorted(oil_prices.values(), key=lambda x: x["date"])
    latest_oil = sorted_prices[-1] if sorted_prices else {"price": 0, "regime": "normal", "delta": 0}
    
    profiles = []
    for pid, events in batter_events.items():
        if pid not in batters:
            continue
        b = batters[pid]
        
        total_bb = len(events)
        total_barrels = sum(1 for e in events if e["is_barrel"])
        career_bob_pct = round(total_barrels / total_bb * 100, 1) if total_bb > 0 else 0
        
        # Split by oil regime
        regime_stats = {"cheap": {"bb": 0, "barrels": 0}, "normal": {"bb": 0, "barrels": 0}, "expensive": {"bb": 0, "barrels": 0}}
        spike_bb = 0; spike_barrels = 0
        crash_bb = 0; crash_barrels = 0
        
        for e in events:
            oil = oil_prices.get(e["game_date"])
            if not oil:
                # Use nearest available oil price
                regime = "normal"
            else:
                regime = oil["regime"]
                if oil["is_spike"]:
                    spike_bb += 1
                    if e["is_barrel"]: spike_barrels += 1
                if oil["is_crash"]:
                    crash_bb += 1
                    if e["is_barrel"]: crash_barrels += 1
            
            regime_stats[regime]["bb"] += 1
            if e["is_barrel"]:
                regime_stats[regime]["barrels"] += 1
        
        # Compute per-regime BOB%
        for r in regime_stats:
            bb = regime_stats[r]["bb"]
            regime_stats[r]["bob_pct"] = round(regime_stats[r]["barrels"] / bb * 100, 1) if bb > 0 else 0
        
        # Oil Boost Index: expensive BOB% / career BOB%
        oil_boost = round(regime_stats["expensive"]["bob_pct"] / career_bob_pct, 2) if career_bob_pct > 0 else 0
        
        # Crisis BOB (spike+crash combined)
        crisis_bb = spike_bb + crash_bb
        crisis_barrels_count = spike_barrels + crash_barrels
        crisis_bob = round(crisis_barrels_count / crisis_bb * 100, 1) if crisis_bb > 0 else 0
        
        # Badges
        badges = []
        if oil_boost >= 1.5 and regime_stats["expensive"]["bb"] >= 3:
            badges.append("Petro-Barrel King")
        if crisis_bob >= 15 and crisis_bb >= 3:
            badges.append("Recession Raker")
        if regime_stats["cheap"]["bob_pct"] > regime_stats["expensive"]["bob_pct"] and regime_stats["cheap"]["bb"] >= 3:
            badges.append("Barrel Embargo")
        if oil_boost >= 2.0 and regime_stats["expensive"]["bb"] >= 3:
            badges.append("OPEC\'s Finest")
        
        # Determine dominant regime
        dom_regime = max(regime_stats, key=lambda r: regime_stats[r]["bb"])
        
        profiles.append({
            "batter_id": pid,
            "full_name": b.get("full_name", ""),
            "team": b.get("team", ""),
            "headshot_url": b.get("headshot_url", ""),
            "total_batted_balls": total_bb,
            "total_barrels": total_barrels,
            "career_bob_pct": career_bob_pct,
            "regime_stats": regime_stats,
            "oil_boost_index": oil_boost,
            "crisis_bob_pct": crisis_bob,
            "spike_barrels": spike_barrels, "spike_bb": spike_bb,
            "crash_barrels": crash_barrels, "crash_bb": crash_bb,
            "dominant_regime": dom_regime,
            "badges": badges,
        })
    
    # Sort by oil_boost_index for leaderboard
    profiles.sort(key=lambda x: x["oil_boost_index"], reverse=True)
    
    # Write outputs
    # Oil price widget data
    write_json("oil-price.json", {
        "latest": latest_oil,
        "history": [{"date": p["date"], "price": p["price"], "regime": p["regime"]} 
                    for p in sorted_prices[-30:]],  # Last 30 days
        "season_avg": round(sum(p["price"] for p in sorted_prices[-30:]) / max(len(sorted_prices[-30:]), 1), 2),
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    })
    
    # Leaderboard: Petro-Barrel Kings (top 50 with enough BBs)
    qualified = [p for p in profiles if p["total_batted_balls"] >= 5]
    write_json("leaderboards/crude-barrels.json", qualified[:50])
    
    # Per-player oil profiles (add to individual summary files)
    for p in profiles:
        pid = p["batter_id"]
        summary_path = os.path.join(OUT_DIR, f"batters/{pid}/summary.json")
        if os.path.exists(summary_path):
            try:
                with open(summary_path) as f:
                    existing = json.load(f)
                existing["oil_profile"] = {
                    "career_bob_pct": p["career_bob_pct"],
                    "regime_stats": p["regime_stats"],
                    "oil_boost_index": p["oil_boost_index"],
                    "crisis_bob_pct": p["crisis_bob_pct"],
                    "badges": p["badges"],
                    "dominant_regime": p["dominant_regime"],
                }
                write_json(f"batters/{pid}/summary.json", existing)
            except:
                pass
    
    print(f"  Computed Crude Barrel profiles for {len(profiles)} batters")
    print(f"  Qualified for leaderboard (5+ BBE): {len(qualified)}")
    if qualified:
        top = qualified[0]
        print(f"  Top Petro-Barrel: {top['full_name']} ({top['team']}) - BOB%: {top['career_bob_pct']}%, Boost: {top['oil_boost_index']}x")
    return latest_oil

def generate_crude_barrels(batters):
    """Main orchestrator for the Crude Barrels system. Returns barrel_events for reuse."""
    print(f"\n{'=' * 40}")
    print("CRUDE BARRELS SYSTEM")
    print(f"{'=' * 40}")
    
    oil_prices = fetch_oil_prices()
    if not oil_prices:
        print("  Skipping Crude Barrels (no oil data)")
        return None, []
    
    barrel_events = fetch_barrel_events()
    if not barrel_events:
        print("  Skipping Crude Barrels (no barrel events)")
        return None, []
    
    latest_oil = compute_crude_barrels(batters, oil_prices, barrel_events)
    return latest_oil, barrel_events

# ============================================================
# 10. MERCURY RETROGRADE BATTING STATS
# ============================================================
# 2025-2026 Mercury Retrograde periods (fixed astronomical dates)
RETROGRADE_PERIODS = [
    # 2025
    ("2025-03-14", "2025-04-07"),
    ("2025-07-17", "2025-08-11"),
    ("2025-11-09", "2025-11-29"),
    # 2026
    ("2026-02-26", "2026-03-20"),
    ("2026-06-29", "2026-07-23"),
    ("2026-10-24", "2026-11-13"),
    # 2027 (for late-season coverage)
    ("2027-02-09", "2027-03-03"),
]

def is_retrograde(date_str):
    """Check if a date falls within a Mercury retrograde period."""
    for start, end in RETROGRADE_PERIODS:
        if start <= date_str <= end:
            return True
    return False

def get_retrograde_status():
    """Get current retrograde status and next retrograde info."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    currently_retro = is_retrograde(today)
    
    # Find current or next retrograde
    current_period = None
    next_period = None
    for start, end in RETROGRADE_PERIODS:
        if start <= today <= end:
            current_period = {"start": start, "end": end}
        elif start > today and next_period is None:
            next_period = {"start": start, "end": end}
    
    if currently_retro and current_period:
        from datetime import timedelta
        end_date = datetime.strptime(current_period["end"], "%Y-%m-%d")
        days_left = (end_date - datetime.strptime(today, "%Y-%m-%d")).days
        return {
            "is_retrograde": True,
            "status": "RETROGRADE",
            "days_remaining": days_left,
            "period_start": current_period["start"],
            "period_end": current_period["end"],
            "next_start": None,
            "next_days_away": None,
        }
    else:
        days_away = None
        if next_period:
            next_start = datetime.strptime(next_period["start"], "%Y-%m-%d")
            days_away = (next_start - datetime.strptime(today, "%Y-%m-%d")).days
        return {
            "is_retrograde": False,
            "status": "DIRECT",
            "days_remaining": None,
            "period_start": None,
            "period_end": None,
            "next_start": next_period["start"] if next_period else None,
            "next_days_away": days_away,
        }

def compute_retrograde_stats(batters, barrel_events):
    """Compute batting stats split by Mercury retrograde vs direct."""
    print("\n[10] Computing Mercury Retrograde stats...")
    
    # Group barrel_events by batter with hit/ab info
    # barrel_events has: batter_id, game_date, is_barrel, ev, la
    # We need actual hit/AB data per game. Use the Savant data which has 'events' column.
    # For simplicity, use barrel_events and compute barrel-rate splits.
    # Also fetch game-level stats from the stats API.
    
    # Group by batter and retrograde status
    batter_splits = {}
    for e in barrel_events:
        pid = e["batter_id"]
        if pid not in batter_splits:
            batter_splits[pid] = {
                "retro": {"bb": 0, "barrels": 0},
                "direct": {"bb": 0, "barrels": 0}
            }
        retro = is_retrograde(e["game_date"])
        key = "retro" if retro else "direct"
        batter_splits[pid][key]["bb"] += 1
        if e["is_barrel"]:
            batter_splits[pid][key]["barrels"] += 1
    
    # Also get per-game hitting stats for BA splits
    # Use the batters dict which has season BA — we'll compute retro/direct barrel rate
    # since we have batted-ball-level data
    
    status = get_retrograde_status()
    profiles = []
    
    for pid, splits in batter_splits.items():
        if pid not in batters:
            continue
        b = batters[pid]
        
        retro_bb = splits["retro"]["bb"]
        retro_barrels = splits["retro"]["barrels"]
        direct_bb = splits["direct"]["bb"]
        direct_barrels = splits["direct"]["barrels"]
        
        retro_brl = round(retro_barrels / retro_bb * 100, 1) if retro_bb > 0 else None
        direct_brl = round(direct_barrels / direct_bb * 100, 1) if direct_bb > 0 else None
        
        # Retrograde diff (positive = better in retrograde)
        if retro_brl is not None and direct_brl is not None:
            retro_diff = round(retro_brl - direct_brl, 1)
        else:
            retro_diff = None
        
        # Badges
        badges = []
        if retro_brl is not None and direct_brl is not None and retro_bb >= 3 and direct_bb >= 3:
            if retro_diff is not None and retro_diff >= 5:
                badges.append("Star-Crossed Slugger")
            elif retro_diff is not None and retro_diff <= -5:
                badges.append("Cosmically Cursed")
            elif retro_diff is not None and abs(retro_diff) <= 1:
                badges.append("Retrograde Proof")
        
        profiles.append({
            "batter_id": pid,
            "full_name": b.get("full_name", ""),
            "team": b.get("team", ""),
            "headshot_url": b.get("headshot_url", ""),
            "retro_barrel_rate": retro_brl,
            "direct_barrel_rate": direct_brl,
            "retro_diff": retro_diff,
            "retro_batted_balls": retro_bb,
            "direct_batted_balls": direct_bb,
            "retro_barrels": retro_barrels,
            "direct_barrels": direct_barrels,
            "badges": badges,
            "season_ba": b.get("ba", 0),
            "season_ops": b.get("ops", 0),
        })
    
    # Sort by retrograde barrel rate (descending) for leaderboard
    profiles.sort(key=lambda x: (x["retro_barrel_rate"] or -1), reverse=True)
    
    # Write outputs
    write_json("mercury-status.json", status)
    
    qualified = [p for p in profiles if (p["retro_batted_balls"] + p["direct_batted_balls"]) >= 5]
    write_json("leaderboards/mercury-retrograde.json", qualified[:50])
    
    # Add to player summaries
    for p in profiles:
        pid = p["batter_id"]
        summary_path = os.path.join(OUT_DIR, f"batters/{pid}/summary.json")
        if os.path.exists(summary_path):
            try:
                with open(summary_path) as f:
                    existing = json.load(f)
                existing["mercury_profile"] = {
                    "retro_barrel_rate": p["retro_barrel_rate"],
                    "direct_barrel_rate": p["direct_barrel_rate"],
                    "retro_diff": p["retro_diff"],
                    "retro_bb": p["retro_batted_balls"],
                    "direct_bb": p["direct_batted_balls"],
                    "badges": p["badges"],
                }
                write_json(f"batters/{pid}/summary.json", existing)
            except:
                pass
    
    # League averages for the banner
    all_retro_bb = sum(p["retro_batted_balls"] for p in profiles)
    all_retro_barrels = sum(p["retro_barrels"] for p in profiles)
    all_direct_bb = sum(p["direct_batted_balls"] for p in profiles)
    all_direct_barrels = sum(p["direct_barrels"] for p in profiles)
    league_retro_brl = round(all_retro_barrels / all_retro_bb * 100, 1) if all_retro_bb > 0 else 0
    league_direct_brl = round(all_direct_barrels / all_direct_bb * 100, 1) if all_direct_bb > 0 else 0
    
    status["league_retro_barrel_rate"] = league_retro_brl
    status["league_direct_barrel_rate"] = league_direct_brl
    write_json("mercury-status.json", status)
    
    print(f"  Retrograde status: {status['status']}")
    if status["is_retrograde"]:
        print(f"  Days remaining: {status['days_remaining']}")
    else:
        print(f"  Next retrograde: {status.get('next_start', '?')} ({status.get('next_days_away', '?')} days away)")
    print(f"  Computed profiles for {len(profiles)} batters ({len(qualified)} qualified)")
    print(f"  League BRL%: Retrograde {league_retro_brl}% vs Direct {league_direct_brl}%")

def generate_mercury_retrograde(batters, barrel_events):
    """Main orchestrator for Mercury Retrograde system."""
    print(f"\n{'=' * 40}")
    print("MERCURY RETROGRADE SYSTEM")
    print(f"{'=' * 40}")
    if not barrel_events:
        print("  No barrel events, skipping")
        return
    compute_retrograde_stats(batters, barrel_events)

# ============================================================
# MAIN
# ============================================================
def main():
    print(f"=" * 60)
    print(f"UpperDecky Data Refresh — {SEASON} Season")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Output directory: {OUT_DIR}")
    print(f"=" * 60)

    teams = fetch_teams()
    if not teams:
        print("ERROR: Could not fetch teams. Aborting.")
        sys.exit(1)

    batters = fetch_batters(teams)
    if not batters:
        print("ERROR: Could not fetch batters. Aborting.")
        sys.exit(1)

    batters = fetch_statcast(batters)
    compute_derived(batters)
    total = generate_outputs(batters, teams)
    generate_hos_picks(batters)
    generate_pressbox_picks(batters)
    generate_dugout_prompt(batters, teams)
    _oil_result, barrel_events = generate_crude_barrels(batters)
    generate_mercury_retrograde(batters, barrel_events)

    print(f"\n{'=' * 60}")
    print(f"DONE — {total} batters, {len(teams)} teams")
    print(f"Finished: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
