"""
StatsBomb Open Data — Women's Football Scraper
===============================================
WHY STATSBOMB INSTEAD OF FBREF:
  - 100% free, no Cloudflare, no CAPTCHA, no bot detection
  - Data lives on GitHub as plain JSON — just download and parse
  - Includes Women's World Cup, NWSL, FA WSL, and more
  - Event-level data: every pass, shot, tackle, dribble per match
  - Easily produces 50,000–200,000+ rows

WOMEN'S COMPETITIONS IN STATSBOMB OPEN DATA:
  ID  72  — FIFA Women's World Cup (multiple seasons)
  ID  37  — FA Women's Super League
  ID  49  — NWSL
  ID 183  — FIFA U20 Women's World Cup
  ID 106  — UEFA Women's Championship
  ID  53  — Women's Super League (older seasons)

SETUP:
    pip install statsbombpy pandas

RUN:
    python statsbomb_womens_scraper.py

OUTPUT:
    ./statsbomb_data/events_master.csv      (every on-ball event, 100k+ rows)
    ./statsbomb_data/matches_master.csv     (match results + metadata)
    ./statsbomb_data/lineups_master.csv     (player lineups per match)
    ./statsbomb_data/player_summary.csv     (aggregated player stats)
"""

import pandas as pd
import os
import json
from datetime import datetime

# Install check
try:
    from statsbombpy import sb
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "statsbombpy", "-q"])
    from statsbombpy import sb

OUTPUT_DIR = "./statsbomb_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Step 1: See all available women's competitions ───────────────────────────
print("=" * 60)
print("  StatsBomb Open Data — Women's Football")
print("=" * 60)

print("\n📋 Fetching all available competitions...")
all_comps = sb.competitions()

# Filter women's only
womens = all_comps[all_comps["competition_gender"] == "female"].copy()
womens = womens.sort_values(["competition_name", "season_name"])

print(f"\n✅ Found {len(womens)} women's competition-seasons:\n")
print(womens[["competition_id", "season_id", "competition_name",
              "season_name", "country_name"]].to_string(index=False))

# Save competition list
womens.to_csv(f"{OUTPUT_DIR}/womens_competitions.csv", index=False)


# ── Step 2: Choose which competitions to download ───────────────────────────
# Grab ALL women's competitions — change this filter if you want specific ones
TARGET_COMPS = womens[["competition_id", "season_id",
                        "competition_name", "season_name"]].values.tolist()

print(f"\n\n🎯 Will download data for {len(TARGET_COMPS)} competition-seasons")


# ── Step 3: Download matches for each competition ────────────────────────────
print("\n\n── STEP 1: Downloading match lists ─────────────────────────")

all_matches = []
for comp_id, season_id, comp_name, season_name in TARGET_COMPS:
    try:
        matches = sb.matches(competition_id=comp_id, season_id=season_id)
        matches["competition_name"] = comp_name
        matches["season_name"]      = season_name
        all_matches.append(matches)
        print(f"  ✅ {comp_name} {season_name}: {len(matches)} matches")
    except Exception as e:
        print(f"  ⚠️  {comp_name} {season_name}: {e}")

matches_df = pd.concat(all_matches, ignore_index=True)
matches_path = f"{OUTPUT_DIR}/matches_master.csv"
matches_df.to_csv(matches_path, index=False)
print(f"\n  Saved {len(matches_df)} total matches → {matches_path}")


# ── Step 4: Download events for every match ───────────────────────────────────
print("\n\n── STEP 2: Downloading match events (this takes a while) ───")
print("  Each match = ~3,000–4,000 event rows\n")

all_events   = []
all_lineups  = []
match_ids    = matches_df["match_id"].unique()
total        = len(match_ids)

for i, match_id in enumerate(match_ids, 1):
    try:
        # Events
        events = sb.events(match_id=match_id)
        # Add match context
        row = matches_df[matches_df["match_id"] == match_id].iloc[0]
        events["match_id"]         = match_id
        events["competition_name"] = row["competition_name"]
        events["season_name"]      = row["season_name"]
        events["home_team"]        = row["home_team"]
        events["away_team"]        = row["away_team"]
        events["match_date"]       = row["match_date"]

        all_events.append(events)

        # Lineups
        lineups = sb.lineups(match_id=match_id)
        for team_name, lineup_df in lineups.items():
            lineup_df["match_id"]   = match_id
            lineup_df["team_name"]  = team_name
            lineup_df["comp_name"]  = row["competition_name"]
            lineup_df["season"]     = row["season_name"]
            all_lineups.append(lineup_df)

        print(f"  [{i}/{total}] match {match_id}: {len(events)} events", flush=True)

        # Save checkpoint every 50 matches
        if i % 50 == 0:
            checkpoint = pd.concat(all_events, ignore_index=True)
            checkpoint.to_csv(f"{OUTPUT_DIR}/_events_checkpoint_{i}.csv", index=False)
            print(f"    💾 Checkpoint saved at match {i}")

    except Exception as e:
        print(f"  ❌ match {match_id}: {e}")

# ── Step 5: Save master files ─────────────────────────────────────────────────
print("\n\n── STEP 3: Saving master files ─────────────────────────────")

events_df = pd.concat(all_events, ignore_index=True)
events_path = f"{OUTPUT_DIR}/events_master.csv"
events_df.to_csv(events_path, index=False)
print(f"  ✅ Events   : {len(events_df):,} rows → {events_path}")

lineups_df = pd.concat(all_lineups, ignore_index=True)
lineups_path = f"{OUTPUT_DIR}/lineups_master.csv"
lineups_df.to_csv(lineups_path, index=False)
print(f"  ✅ Lineups  : {len(lineups_df):,} rows → {lineups_path}")

# ── Step 6: Build aggregated player summary ───────────────────────────────────
print("\n\n── STEP 4: Building player summary stats ───────────────────")

# Count event types per player
player_summary = (
    events_df.groupby(["player", "team", "competition_name", "season_name", "type"])
    .size()
    .reset_index(name="count")
    .pivot_table(
        index=["player", "team", "competition_name", "season_name"],
        columns="type",
        values="count",
        fill_value=0
    )
    .reset_index()
)
player_summary.columns.name = None

summary_path = f"{OUTPUT_DIR}/player_summary.csv"
player_summary.to_csv(summary_path, index=False)
print(f"  ✅ Player summary: {len(player_summary):,} rows → {summary_path}")

# ── Final report ──────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  ✅  ALL DONE!")
print(f"  📊  Events    : {len(events_df):,} rows")
print(f"  📊  Matches   : {len(matches_df):,} rows")
print(f"  📊  Lineups   : {len(lineups_df):,} rows")
print(f"  📊  Player summary: {len(player_summary):,} rows")
print(f"  📁  Output    : {OUTPUT_DIR}/")
print("=" * 60)

print("\n📈 Events by competition:\n")
summary = (
    events_df.groupby("competition_name")
    .size()
    .reset_index(name="event_rows")
    .sort_values("event_rows", ascending=False)
)
print(summary.to_string(index=False))