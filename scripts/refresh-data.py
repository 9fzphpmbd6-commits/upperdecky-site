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
        teams.append({
            "team_id": t["id"],
            "abbreviation": t.get("abbreviation", ""),
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

    print(f"\n{'=' * 60}")
    print(f"DONE — {total} batters, {len(teams)} teams")
    print(f"Finished: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
