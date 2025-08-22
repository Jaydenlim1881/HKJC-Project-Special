# -----------------------------
# IMPORTS + UTILS
# -----------------------------
import sys

import time
import re
from datetime import datetime
from utils import (
    log, sanitize_text, clean_placing, convert_finish_time,
    get_distance_group, get_turn_count, get_draw_group,
    get_jump_type, get_distance_group_from_row, get_season_code
)


import sqlite3

from collections import defaultdict
import pandas as pd
from bs4 import BeautifulSoup, UnicodeDammit
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import requests

# ===== DEBUGGING CONTROL =====
DEBUG_LEVEL = "INFO"  # "OFF", "INFO", "DEBUG", "TRACE"

CHROME_DRIVER_PATH = './chromedriver'

from _horse_dynamic_stats_cleaned import (
    sanitize_text,
    build_exact_distance_pref,
    convert_finish_time,
    upsert_running_position,
    create_running_position_table,
    build_course_pref,
    build_bwr_distance_perf,
    upsert_distance_pref,
    upsert_going_pref,
    upsert_course_pref,
    upsert_bwr_distance_perf,
    create_bwr_distance_perf_table,
    ensure_column_exists,
    upsert_horse_jockey_combo,
    create_trainer_combo_table,
    upsert_trainer_combo,
    upsert_jockey_trainer_combo,
    create_jockey_trainer_combo_table,
    get_distance_group,
    get_draw_group,
    build_draw_pref,
    upsert_draw_pref,
    create_draw_pref_table,
    build_weight_pref_from_dict,
    create_weight_pref_table,
    upsert_weight_pref,
    clean_placing,
    build_hwtr_per_class,
    upsert_hwtr_trend,
    build_class_jump_pref,
    upsert_class_jump_pref,
    create_class_jump_pref_table,
    rebuild_running_style_pref,
    create_horse_rating_table,
    upsert_horse_rating
)
from _horse_dynamic_stats_cleaned import create_running_style_pref_table, migrate_turncount_to_real, create_race_field_size_table

# -----------------------------
# DYNAMIC STATS UPSERT (LOCAL)
# -----------------------------
def get_race_field_size(race_date_str, race_no, race_course):
    """Derive field size for a race.

    Attempts to look up the value from the ``race_field_size`` cache table
    first.  If not present, it will scrape the HKJC race result page to count
    the number of runners and cache the result for future use.
    """

    # Ensure the cache table exists
    create_race_field_size_table()

    # 1) Try the local cache
    try:
        conn = sqlite3.connect("hkjc_horses_dynamic.db")
        cur = conn.cursor()
        cur.execute(
            "SELECT FieldSize FROM race_field_size WHERE RaceDate=? AND RaceNo=? AND RaceCourse=?",
            (race_date_str, str(race_no), race_course),
        )
        row = cur.fetchone()
        conn.close()
        if row and row[0]:
            return int(row[0])
    except Exception as e:
        log("DEBUG", f"Field size DB lookup failed: {e}")

    # 2) Fallback to scraping the race result page
    try:
        url = (
            "https://racing.hkjc.com/racing/information/English/racing/"
            f"LocalResults.aspx?RaceDate={race_date_str}&Racecourse={race_course}&RaceNo={race_no}"
        )
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        dammit = UnicodeDammit(resp.content, ["utf-8", "big5", "latin-1"])
        soup = BeautifulSoup(dammit.unicode_markup, "html.parser")

        table = soup.find("table", class_="bigborder")
        if not table:
            for t in soup.find_all("table"):
                header = t.find("tr")
                if header and "Horse" in header.get_text():
                    table = t
                    break

        if table:
            rows = [r for r in table.find_all("tr") if r.find_all("td")]
            field_size = len(rows) - 1  # exclude header
            if field_size > 0:
                try:
                    conn = sqlite3.connect("hkjc_horses_dynamic.db")
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT OR REPLACE INTO race_field_size (RaceDate, RaceNo, RaceCourse, FieldSize) VALUES (?, ?, ?, ?)",
                        (race_date_str, str(race_no), race_course, field_size),
                    )
                    conn.commit()
                    conn.close()
                except Exception as e:
                    log("DEBUG", f"Failed to cache field size: {e}")
                return field_size
    except Exception as e:
        log("DEBUG", f"Field size scrape failed: {e}")

    return None

def create_going_pref_table():
    conn = sqlite3.connect("hkjc_horses_dynamic.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS horse_going_pref (
            HorseID TEXT,
            Season TEXT,
            GoingType TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            PRIMARY KEY (HorseID, Season, GoingType)
        );
    """)
    conn.commit()
    conn.close()


def upsert_dynamic_stats(
    horse_id,
    recent_form,
    days_since_last_run,
    fitness,
    distance_pref,
    going_pref,
    course_pref,
    running_style
):
    conn = sqlite3.connect('hkjc_horses_dynamic.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS horse_dynamic_stats (
            HorseID TEXT PRIMARY KEY,
            RecentForm1 INTEGER,
            RecentForm2 INTEGER,
            RecentForm3 INTEGER,
            RecentForm4 INTEGER,
            RecentForm5 INTEGER,
            DaysSinceLastRun INTEGER,
            FitnessIndicator INTEGER,
            NumRecentRuns INTEGER,
            LastUpdate TEXT
        )
    ''')

    num_recent_runs = len(recent_form)
    recent_form = (recent_form + [None] * 5)[:5]
    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

    cursor.execute('''
        INSERT OR REPLACE INTO horse_dynamic_stats (
            HorseID,
            RecentForm1, RecentForm2, RecentForm3, RecentForm4, RecentForm5,
            DaysSinceLastRun,
            FitnessIndicator,
            NumRecentRuns,
            LastUpdate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        horse_id,
        recent_form[0], recent_form[1], recent_form[2],
        recent_form[3], recent_form[4],
        days_since_last_run,
        int(fitness) if str(fitness).isdigit() else None,
        num_recent_runs,
        last_update
    ))

    conn.commit()
    conn.close()

def build_trainer_combo(rows):
    combo = defaultdict(lambda: defaultdict(lambda: {"Top3Count": 0, "TotalRuns": 0}))

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 8:
            continue

        placing_text = sanitize_text(cols[1].get_text())
        placing_text_clean = re.sub(r'[^\d]', '', placing_text)
        placing = int(placing_text_clean) if placing_text_clean.isdigit() else None
        date_str = sanitize_text(cols[2].get_text())
        trainer = sanitize_text(cols[9].get_text()) if len(cols) > 9 else None

        try:
            race_date = datetime.strptime(date_str, "%d/%m/%y")
        except:
            continue

        if placing is None or not trainer:
            continue

        # Build season code
        if race_date.month >= 9:
            season_code = f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}"
        else:
            season_code = f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"

        combo[season_code][trainer]["TotalRuns"] += 1
        if placing <= 3:
            combo[season_code][trainer]["Top3Count"] += 1

    return combo

# -----------------------------
# Parse course key from HTML (e.g., "ST / Turf / \"A\"")
# -----------------------------
def parse_course_key(course_key_raw):
    if not course_key_raw:
        return "UNKNOWN", "UNKNOWN"
    parts = [p.strip().replace('"', '').replace('+', '').replace('-', '') for p in course_key_raw.split("/")]
    if len(parts) == 3:
        return parts[0], parts[2]  # ST / Turf / A
    elif len(parts) == 2 and "AWT" in parts[1].upper():
        return parts[0], "AWT"
    return parts[0], "UNKNOWN"

# -----------------------------
# SCRAPER / PROCESSOR
# -----------------------------
def extract_dynamic_stats(horse_url):
    service = Service(CHROME_DRIVER_PATH)
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(horse_url)
        time.sleep(3)

        page_source = driver.execute_script("return document.documentElement.outerHTML")
        page_source = sanitize_text(page_source)
        soup = BeautifulSoup(page_source, "html.parser")

        table = soup.find("table", class_="f_tac f_fs12 js_race_tab") or \
                soup.find("table", class_="f_tac f_fs12") or \
                soup.find("table", class_="bigborder")
        if not table:
            raise ValueError("Could not find race history table on page")

        rows = table.find_all("tr")[1:]
        if not rows:
            raise ValueError("No race history data found in table")

        # 🧠 Step: Sort race rows by date (latest first)
        dated_rows = []
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 3:
                continue
            date_str = cols[2].text.strip()
            try:
                race_date = datetime.strptime(date_str, "%d/%m/%y")
                dated_rows.append((race_date, row))
            except:
                continue

        if not dated_rows:
            raise ValueError("No valid race dates found")

        # Sort descending (latest race first)
        dated_rows.sort(key=lambda x: x[0], reverse=True)
        sorted_rows = [r[1] for r in dated_rows]
        hwtr_rows = list(reversed(sorted_rows))  # oldest to newest (for HWTR only)

        # Process each row
        processed_rows = []
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 8:
                continue  # Skip incomplete rows
                
            # Extract race details from link
            race_link_tag = cols[0].find("a")
            if race_link_tag and race_link_tag.has_attr("href"):
                race_url = race_link_tag['href']
                match = re.search(r'RaceDate=([\d/]+)&Racecourse=([A-Z]+)&RaceNo=(\d+)', race_url)
                if match:
                    race_date_str = match.group(1)
                    race_course_code = match.group(2)
                    race_no = int(match.group(3))
                else:
                    race_date_str = None
                    race_course_code = None
                    race_no = None
            else:
                race_date_str = None
                race_course_code = None
                race_no = None

            # Do not set a placeholder here; let DB keep existing or fill later from CSV
            processed_rows.append(row)

        log("DEBUG", f"Running weight preference analysis for {horse_url}")

        # 🏋️ Extract weight preference data
        horse_id = horse_url.split("HorseId=")[-1]
        
        # Analyze processed rows
        recent_form = []
        race_dates = []
        fitness_count = 0
        distance_stats = defaultdict(lambda: {"total": 0, "wins": 0, "placing_sum": 0})
        going_stats = defaultdict(lambda: {"total": 0, "wins": 0, "placing_sum": 0})
        going_stats_seasonal = defaultdict(lambda: defaultdict(lambda: {"total": 0, "top3": 0}))
        course_stats = defaultdict(lambda: {"total": 0, "wins": 0, "placing_sum": 0})
        class_stats = defaultdict(lambda: {"total": 0, "placing_sum": 0})

        today = datetime.today()

        for row in processed_rows:
            cols = row.find_all("td")
            if len(cols) < 8:
                continue

            class_val = sanitize_text(cols[7].get_text())
            placing_text = sanitize_text(cols[1].get_text())
            placing_text_clean = re.sub(r'[^\d]', '', placing_text)
            placing = int(placing_text_clean) if placing_text_clean.isdigit() else None

            date_str = sanitize_text(cols[2].get_text())
            course_str = sanitize_text(cols[3].get_text())
            distance_str = sanitize_text(cols[4].get_text())
            going_str = sanitize_text(cols[5].get_text())

            if not date_str or placing is None:
                continue

            try:
                race_date = datetime.strptime(date_str, "%d/%m/%y")
            except ValueError:
                continue

            race_dates.append(race_date)

            # Build season code
            if race_date.month >= 9:
                season_code = f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}"
            else:
                season_code = f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"

            # Recent form 1-5
            if len(recent_form) < 5:
                recent_form.append(placing)

            # Fitness count
            if 0 <= (today - race_date).days <= 90:
                fitness_count += 1

            # Distance stats
            if distance_str.isdigit():
                d = int(distance_str)
                distance_stats[d]["total"] += 1
                distance_stats[d]["placing_sum"] += placing
                if placing == 1:
                    distance_stats[d]["wins"] += 1

            # Going stats
            if going_str:
                going_stats_seasonal[season_code][going_str]["total"] += 1
                if placing in [1, 2, 3]:
                    going_stats_seasonal[season_code][going_str]["top3"] += 1

                going_stats[going_str]["total"] += 1
                going_stats[going_str]["placing_sum"] += placing
                if placing == 1:
                    going_stats[going_str]["wins"] += 1

            # Course stats
            course_stats[course_str]["total"] += 1
            course_stats[course_str]["placing_sum"] += placing
            if placing == 1:
                course_stats[course_str]["wins"] += 1

            # Class stats
            if class_val.isdigit():
                c = int(class_val)
                class_stats[c]["total"] += 1
                class_stats[c]["placing_sum"] += placing

        race_dates.sort(reverse=True)
        days_since_last_run = (today - race_dates[0]).days if race_dates else None

        def get_best(stats_dict, type="win"):
            best_key, best_value = None, -1
            for key, stats in stats_dict.items():
                if stats["total"] == 0:
                    continue
                if type == "win":
                    win_rate = stats["wins"] / stats["total"]
                    if win_rate > best_value:
                        best_key = key
                        best_value = win_rate
                elif type == "avg":
                    avg = stats["placing_sum"] / stats["total"]
                    if best_value == -1 or avg < best_value:
                        best_key = key
                        best_value = avg
            return best_key, round(best_value, 2) if best_value != -1 else None

        # Get best performance metrics
        best_distance, best_distance_win_rate = get_best(distance_stats, type="win")
        best_going, best_going_win_rate = get_best(going_stats, type="win")
        best_course, best_course_win_rate = get_best(course_stats, type="win")
        best_class, best_class_avg = get_best(class_stats, type="avg")

        # Detailed preferences
        detailed_distance_pref = build_exact_distance_pref(processed_rows)
        detailed_course_pref = build_course_pref(processed_rows)

        # ✅ Insert Running Position data per race
        horse_id = horse_url.split("HorseId=")[-1]

        for row in processed_rows:
            cols = row.find_all("td")
            if len(cols) < 18:
                continue

            try:
                # -- Extract race link details --
                race_link_tag = cols[0].find("a")
                if race_link_tag and race_link_tag.has_attr("href"):
                    # Extract basic race info
                    match = re.search(r'RaceDate=([\d/]+)&Racecourse=([A-Z]+)&RaceNo=(\d+)', race_link_tag['href'])
                    if match:
                        race_date_str = match.group(1)
                        race_course = match.group(2)
                        race_no = match.group(3)
                    else:
                        continue

                    # PROPER RaceID EXTRACTION - THIS IS THE KEY FIX
                    race_id = sanitize_text(race_link_tag.get_text(strip=True))
                    
                    # Clean the extracted RaceID - remove all non-numeric characters
                    race_id = ''.join(c for c in race_id if c.isdigit())
                    
                    # VALIDATION - Only use if we got a proper numeric ID
                    if race_id and len(race_id) >= 3:  # Real RaceIDs are at least 3 digits
                        log("DEBUG", f"Using extracted RaceID: {race_id}")
                    else:
                        # Only construct ID as last resort
                        race_date_obj = datetime.strptime(race_date_str, "%Y/%m/%d")
                        constructed_id = f"{race_date_obj.strftime('%Y%m%d')}_{race_course}_{int(race_no):02d}"
                        race_id = constructed_id
                        log("WARNING", f"Using constructed RaceID: {constructed_id} (Original: {race_link_tag.get_text()})")

                # -- Extract Distance & Course Info
                course_key_raw = sanitize_text(cols[3].get_text())
                race_course, course_type = parse_course_key(course_key_raw)
                distance_str = sanitize_text(cols[4].get_text())
                finish_time_str = sanitize_text(cols[15].get_text())
                running_position_str = sanitize_text(cols[14].get_text())
                placing_str = sanitize_text(cols[1].get_text())

                if not (distance_str.isdigit() and running_position_str):
                    continue

                distance = int(distance_str)
                placing_clean = re.sub(r'[^\d]', '', placing_str)
                placing = int(placing_clean) if placing_clean else None
                finish_time = convert_finish_time(finish_time_str)

                # -- Extract running position sequence
                positions = [int(p) for p in running_position_str.split() if p.isdigit()]
                if len(positions) < 2:
                    continue

                early_pos = positions[0]
                mid_pos = round(sum(positions[1:-1]) / len(positions[1:-1]), 2) if len(positions) > 2 else None
                final_pos = positions[-1]

                # -- Distance Group & Turn Count
                dist_group = get_distance_group(race_course, course_type, distance)
                surface_norm = "AWT" if (str(course_type).strip().upper() == "AWT") else "TURF"
                turn_count = get_turn_count(race_course, surface_norm, distance) or 0.0

                # -- Season
                race_date = datetime.strptime(race_date_str, "%Y/%m/%d")
                season = f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}" if race_date.month >= 9 else f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"
                
                # Derive field size for this race
                field_size = get_race_field_size(race_date_str, race_no, race_course)

                # -- Build data dict
                race_date_obj = datetime.strptime(race_date_str, "%Y/%m/%d")
                rp_data = {
                    "HorseID": horse_id,
                    "RaceDate": race_date_obj.strftime("%Y-%m-%d"),
                    "RaceID": race_id,
                    "RaceNo": race_no,
                    "Season": season,
                    "RaceCourse": race_course,
                    "CourseType": course_type,
                    "DistanceGroup": dist_group,
                    "TurnCount": turn_count,
                    "EarlyPos": early_pos,
                    "MidPos": mid_pos,
                    "FinalPos": final_pos,
                    "FinishTime": finish_time,
                    "Placing": placing if placing is not None else final_pos,
                    "FieldSize": field_size,
                    # Separate display string if needed by consumers
                    "RaceDateDisplay": race_date_obj.strftime("%d/%m/%y"),
                }


                upsert_running_position(rp_data)

            except Exception as err:
                log("WARNING", f"Skipped row for {horse_id} due to: {err}")

        return {
            "HorseID": horse_url.split("HorseId=")[-1],
            "RecentForm": recent_form,
            "NumRecentRuns": len(recent_form),
            "DaysSinceLastRun": days_since_last_run,
            "FitnessIndicator": fitness_count,
            "BestDistance": best_distance,
            "BestDistanceWinRate": best_distance_win_rate,
            "BestGoing": best_going,
            "BestGoingWinRate": best_going_win_rate,
            "BestCourse": best_course,
            "BestCourseWinRate": best_course_win_rate,
            "BestClass": best_class,
            "BestClassAvgPlacing": best_class_avg,
            "DistancePrefDetailed": detailed_distance_pref,
            "GoingPrefSeasonal": going_stats_seasonal,
            "CoursePrefDetailed": detailed_course_pref,
            "RawRows": processed_rows
        }

    except Exception as e:
        log("ERROR", f"Failed to process {horse_url}: {str(e)}")
        return None
    finally:
        driver.quit()

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("\n[INFO] This module provides helper functions for processing HKJC horse data.")
    print("       It's designed to be imported, not run directly.")

    create_running_position_table()
    create_running_style_pref_table()
    migrate_turncount_to_real()

    # Optional one-off backfill for ALL horses
    try:
        upserts, groups = rebuild_running_style_pref(horse_id=None)
        print(f"[BACKFILL] running_style_pref rebuilt. Groups={groups}, rows upserted={upserts}")
    except Exception as e:
        print(f"[ERROR] Backfill failed: {e}")
    
    # 1. First create all basic tables
    create_going_pref_table()
    create_trainer_combo_table()

    # 2. Handle jockey-trainer table with migration
    from _horse_dynamic_stats_cleaned import migrate_jockey_trainer_table
    migrate_jockey_trainer_table()  # First migrate existing tables
    create_jockey_trainer_combo_table()  # Then ensure proper table structure

    # 3. Handle draw preferences
    create_draw_pref_table()  # ✅ Ensure table exists
    ensure_column_exists("hkjc_horses_dynamic.db", "horse_draw_pref", "ID", "INTEGER")
    ensure_column_exists("hkjc_horses_dynamic.db", "horse_draw_pref", "RaceCourse", "TEXT")
    ensure_column_exists("hkjc_horses_dynamic.db", "horse_draw_pref", "LastUpdate", "TIMESTAMP")

    # 4. Create remaining tables
    create_running_position_table()  # ← Important: Keep this single call
    create_bwr_distance_perf_table()  # For BWR processing
    create_weight_pref_table()  # For weight preferences
    create_horse_rating_table()  # ensure horse_rating exists (with LastUpdate)

    # 5. Load and process horses
    horse_id_df = pd.read_csv("horse_ids_to_update.csv")
    horse_id_df = horse_id_df[horse_id_df['HorseID'].notna()]
    horse_ids = horse_id_df['HorseID'].astype(str).str.strip().unique()

    log("INFO", f"\nStarting batch update at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("INFO", "Database tables initialized with LastRaceDate support")

    success = 0
    failure = 0

    for horse_id in horse_ids:
        if not isinstance(horse_id, str) or not horse_id.startswith("HK_") or "_" not in horse_id:
            log("WARNING", f"Skipping invalid HorseID: {horse_id}")
            failure += 1
            continue

        horse_url = f"https://racing.hkjc.com/racing/information/English/Horse/Horse.aspx?HorseId={horse_id.strip()}"
        try:
            log("INFO", f"\nProcessing: {horse_id}")
            horse_data = extract_dynamic_stats(horse_url)

            if horse_data:
                # 1) Dynamic stat row
                upsert_dynamic_stats(
                    horse_id=horse_data["HorseID"],
                    recent_form=horse_data["RecentForm"],
                    days_since_last_run=horse_data["DaysSinceLastRun"],
                    fitness=str(horse_data["FitnessIndicator"]),
                    distance_pref=horse_data["DistancePrefDetailed"],
                    going_pref=horse_data["GoingPrefSeasonal"],
                    course_pref=horse_data["CoursePrefDetailed"],
                    running_style=None
                )

                # --- HWTR Build and Insert ---
                try:
                    season = None
                    for r in horse_data["RawRows"]:
                        cols = r.find_all("td")
                        if len(cols) >= 3:
                            try:
                                date_str = cols[2].get_text().strip()
                                date = datetime.strptime(date_str, "%d/%m/%y")
                                season = f"{date.year%100:02d}/{(date.year+1)%100:02d}" if date.month >= 9 else f"{(date.year-1)%100:02d}/{date.year%100:02d}"
                                break
                            except:
                                continue

                    if season:
                        hwtr_data = build_hwtr_per_class(horse_data["RawRows"], horse_data["HorseID"])
                        upsert_hwtr_trend(hwtr_data)
                        log("DEBUG", f"HWTR data generated: {len(hwtr_data)} rows")

                        # --- Horse Rating snapshot upsert (minimal) ---
                        try:
                            rows = [r.find_all("td") for r in horse_data["RawRows"]]
                            rows = [c for c in rows if len(c) > 8 and c[2].get_text(strip=True)]

                            def _parse_iso(dmy):
                                from datetime import datetime
                                s = dmy.strip()
                                for fmt in ("%d/%m/%y", "%d/%m/%Y"):
                                    try:
                                        return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
                                    except:
                                        pass
                                return None

                            parsed = []
                            for c in rows:
                                date_txt = c[2].get_text(strip=True)
                                iso = _parse_iso(date_txt)
                                rating_txt = c[8].get_text(strip=True) if len(c) > 8 else ""
                                try:
                                    rating_val = float(rating_txt) if rating_txt else None
                                except:
                                    rating_val = None
                                if iso and rating_val is not None:
                                    parsed.append((iso, rating_val))

                            if parsed:
                                parsed.sort(key=lambda x: x[0])  # ascending by date
                                rating_start_career = parsed[0][1]

                                from datetime import datetime
                                def _season_code(iso):
                                    dt = datetime.strptime(iso, "%Y-%m-%d")
                                    return f"{dt.year%100:02d}/{(dt.year+1)%100:02d}" if dt.month >= 9 else f"{(dt.year-1)%100:02d}/{dt.year%100:02d}"

                                season_start = next((rv for iso, rv in parsed if _season_code(iso) == season), parsed[0][1])
                                rating_start_season = season_start

                                as_of_date, official_rating = parsed[-1]

                                upsert_horse_rating(
                                    horse_id=horse_data["HorseID"],
                                    season=season,
                                    as_of_date=as_of_date,
                                    official_rating=official_rating,
                                    rating_start_season=rating_start_season,
                                    rating_start_career=rating_start_career
                                )
                        except Exception as e:
                            log("ERROR", f"Failed to upsert horse_rating for {horse_data.get('HorseID')}: {e}")

                        # ✅ INSERT DISTANCE PREF HERE
                        upsert_distance_pref(
                            horse_id=horse_data["HorseID"],
                            season=season,
                            distance_pref=horse_data["DistancePrefDetailed"]
                        )

                except Exception as e:
                    log("ERROR", f"Failed to insert HWTR for {horse_id}: {e}")

                # 2) Preferences tables
                upsert_distance_pref(
                    horse_id=horse_data["HorseID"],
                    season=season,
                    distance_pref=horse_data["DistancePrefDetailed"]
                )

                upsert_going_pref(
                    horse_id=horse_data["HorseID"],
                    going_pref_dict=horse_data["GoingPrefSeasonal"]
                )

                upsert_course_pref(
                    horse_id=horse_data["HorseID"],
                    course_pref=horse_data["CoursePrefDetailed"]
                )

                upsert_horse_jockey_combo(
                    horse_id=horse_data["HorseID"],
                    rows=horse_data["RawRows"]
                )

                # Class Jump Preference
                try:
                    class_jump_stats = build_class_jump_pref(horse_data["RawRows"])
                    upsert_class_jump_pref(horse_data["HorseID"], class_jump_stats)
                except Exception as e:
                    log("ERROR", f"Failed to update Class Jump Pref for {horse_data['HorseID']}: {e}")

                # Debug/verify: display newest → oldest seasons for Class Jump (no schema change)
                if DEBUG_LEVEL in ("DEBUG", "TRACE"):
                    try:
                        from _horse_dynamic_stats_cleaned import fetch_class_jump_pref_ordered
                        ordered = fetch_class_jump_pref_ordered(horse_data["HorseID"])
                        log("DEBUG", f"ClassJump (newest→oldest) for {horse_data['HorseID']}: {ordered}")
                    except Exception as qerr:
                        log("DEBUG", f"ClassJump verify query failed: {qerr}")

                trainer_combo = build_trainer_combo(horse_data["RawRows"])
                upsert_trainer_combo(
                    horse_id=horse_data["HorseID"],
                    trainer_combo_dict=trainer_combo
                )

                # ✅ Weight Preference
                weight_race_history = []
                log("TRACE", f"Total races in RawRows: {len(horse_data['RawRows'])}")

                for row in horse_data["RawRows"]:
                    cols = row.find_all("td")
                    if len(cols) < 14:
                        log("DEBUG", f"Skipping incomplete row: only {len(cols)} columns")
                        continue

                    placing = clean_placing(cols[1].get_text())
                    date_str = sanitize_text(cols[2].get_text())
                    actual_wt_str = sanitize_text(cols[13].get_text())
                    try:
                        actual_wt = float(actual_wt_str) if actual_wt_str else None
                    except ValueError:
                        log("WARNING", f"Invalid weight value: {actual_wt_str}")
                        continue
                    distance_str = sanitize_text(cols[4].get_text())
                    course_info = sanitize_text(cols[3].get_text())
            
                    if not actual_wt or not distance_str.isdigit():
                        log("DEBUG", f"Skipping - missing ActualWT or distance at date {date_str}")
                        continue

                    if placing is None:
                        log("DEBUG", f"Skipping -  invalid placing '{cols[1].get_text()}' at date {date_str}")
                        continue

                    try:
                        race_date = datetime.strptime(date_str, "%d/%m/%y")
                        if race_date.month >= 9:
                            season_code = f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}"
                        else:
                            season_code = f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"
                        
                        # Parse course info to get race course and type
                        if "AWT" in course_info:
                            race_course = "ST"
                            course_type = "AWT"
                        else:
                            parts = course_info.split("/")
                            race_course = parts[0].strip() if len(parts) > 0 else "Unknown"
                            course_type = parts[2].strip() if len(parts) > 2 else "Turf"
                    
                        distance = int(distance_str)
                        distance_group = get_distance_group(race_course, course_type, distance)
                        if distance_group == "Unknown":
                            log("WARNING", f"Unknown distance group for {race_course}/{course_type} {distance}m")
                        
                        weight_race_history.append({
                            "season": season_code,
                            "finish": placing,
                            "actual_wt": float(actual_wt),
                            "distance_group": distance_group,
                            "race_course": race_course,
                            "course_type": course_type,
                            "distance": distance
                        })
                    except Exception as e:
                        log("WARNING", f"Failed to parse race data: {e}")

                # INSERT DEBUG CODE RIGHT HERE (after the loop ends)
                log("DEBUG", f"\nCollected {len(weight_race_history)} weight records")
                if weight_race_history:
                    log("TRACE", "First 3 weight records:")
                    for i, record in enumerate(weight_race_history[:3]):
                        log("TRACE", f"Record {i+1}:")
                        log("TRACE", f"  Season: {record['season']}")
                        log("TRACE", f"  Finish: {record['finish']}")
                        log("TRACE", f"  ActualWT: {record['actual_wt']} (Type: {type(record['actual_wt'])})")
                        log("TRACE", f"  DistanceGroup: {record['distance_group']}")
                        log("TRACE", f"  Course: {record['race_course']}/{record['course_type']}")
                        log("TRACE", f"  Distance: {record['distance']}m")
                
                # ✅ Sort RawRows by race date descending (latest first)
                sorted_raw_rows = sorted(
                    horse_data["RawRows"],
                    key=lambda row: datetime.strptime(
                    row.find_all("td")[2].get_text(strip=True), 
                    "%d/%m/%y"
                    ),
                    reverse=True  # Newest first
                )
                # Then use this sorted list for BWR processing
                try:
                    bwr_perf = build_bwr_distance_perf(sorted_raw_rows)
                    upsert_bwr_distance_perf(
                        horse_id=horse_data["HorseID"], 
                        bwr_perf_list=bwr_perf
                    )              
                except Exception as e:
                    log("ERROR", f"Failed to update BWR Distance Pref for {horse_id}: {e}")    
                    try:
                        if sorted_raw_rows:
                            newest_date = sorted_raw_rows[0].find_all("td")[2].get_text(strip=True)
                            oldest_date = sorted_raw_rows[-1].find_all("td")[2].get_text(strip=True)
                            log("DEBUG", f"Processing {len(sorted_raw_rows)} races")
                            log("DEBUG", f"Date range: {newest_date} (newest) to {oldest_date} (oldest)")
                    except Exception as debug_e:
                        log("DEBUG", f"Couldn't get debug info: {debug_e}")

                # Optional: assign for weight functions if used elsewhere
                weight_race_history = sorted(
                    horse_data["RawRows"],
                    key=lambda row: datetime.strptime(row.find_all("td")[2].get_text(strip=True), "%d/%m/%y"),
                    reverse=True  # This ensures newest races come first
                )
                
                # Build & Upsert
                weight_pref = build_weight_pref_from_dict(weight_race_history, horse_data["HorseID"])
                log("DEBUG", f"\nSending {len(weight_race_history)} races to build_weight_pref_from_dict")

                # Ensure data consistency (optional safety check)
                for row in weight_pref:
                    row["HorseID"] = horse_data["HorseID"]  # Already set by build_weight_pref_from_dict, but kept for safety
                    row["Season"] = str(row.get("Season", "Unknown"))  # Force string type

                upsert_weight_pref(horse_id=horse_data["HorseID"], weight_pref_list=weight_pref)
                
                # ✅ BWR × Distance Preference
                try:
                    # ✅ Sort RawRows by race date descending
                    bwr_perf = build_bwr_distance_perf(sorted_raw_rows)
                    upsert_bwr_distance_perf(horse_id=horse_data["HorseID"], bwr_perf_list=bwr_perf)              
                except Exception as e:
                    log("ERROR", f"Failed to update BWR Distance Pref for {horse_id}: {e}")

                # Draw preference
                try:
                    draw_pref_dict = build_draw_pref(horse_data["RawRows"])
                    upsert_draw_pref(horse_data["HorseID"], draw_pref_dict)
                    if DEBUG_LEVEL in ("DEBUG", "TRACE"):
                        from _horse_dynamic_stats_cleaned import fetch_draw_pref_ordered
                        ordered = fetch_draw_pref_ordered(horse_data["HorseID"])
                        log("DEBUG", f"DrawPref (newest first) for {horse_data['HorseID']}: {ordered[:3]}")
                except Exception as e:
                    log("ERROR", f"Failed to update draw pref for {horse_data['HorseID']}: {e}")

                # Running Style Preference (aggregated from horse_running_position)
                try:
                    upserts, groups = rebuild_running_style_pref(horse_id)
                    if DEBUG_LEVEL in ("DEBUG", "TRACE"):
                        log("DEBUG", f"RunningStylePref updated for {horse_id}: {upserts} rows across {groups} groups")
                        try:
                            from _horse_dynamic_stats_cleaned import fetch_running_style_pref_ordered
                            ordered = fetch_running_style_pref_ordered(horse_id)
                            # Display seasons in proper order
                            seasons = sorted(set(row[1] for row in ordered), 
                                        key=lambda s: int(s[:2]), 
                                        reverse=True)
                            log("DEBUG", f"RunningStylePref seasons (newest→oldest): {seasons}")
                            log("DEBUG", f"Sample style data: {ordered[0] if ordered else 'None'}")    
                        except Exception as qerr:
                            log("DEBUG", f"RunningStylePref verify query failed: {qerr}")
                except Exception as e:
                    log("ERROR", f"Failed to update running_style_pref for {horse_id}: {e}")

                # Jockey-Trainer combo
                jt_combo_map = defaultdict(lambda: {"top3": 0, "total": 0, "last_date": None})

                for row in horse_data["RawRows"]:
                    cols = row.find_all("td")
                    if len(cols) < 11:
                        continue

                    place_text = sanitize_text(cols[1].get_text())
                    place_clean = re.sub(r'[^\d]', '', place_text)
                    placing = int(place_clean) if place_clean.isdigit() else None

                    date_str = sanitize_text(cols[2].get_text())
                    trainer = sanitize_text(cols[9].get_text()) if len(cols) > 9 else None
                    jockey = sanitize_text(cols[10].get_text()) if len(cols) > 10 else None
                    
                    if placing is None or not jockey or not trainer:
                        continue

                    try:
                        race_date = datetime.strptime(date_str, "%d/%m/%y")
                        if race_date.month >= 9:
                            season_code = f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}"
                        else:
                            season_code = f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"
                    except:
                        continue

                    key = (season_code, jockey, trainer)
                    jt_combo_map[key]["total"] += 1
                    if placing in [1, 2, 3]:
                        jt_combo_map[key]["top3"] += 1

                    current_last = jt_combo_map[key]["last_date"]
                    if current_last is None or race_date > current_last:
                        jt_combo_map[key]["last_date"] = race_date

                for (season, jockey, trainer), result in jt_combo_map.items():
                    top3 = result["top3"]
                    total = result["total"]
                    # Store ISO for DB; keep display string separate if needed
                    last_date_iso = result["last_date"].strftime("%Y-%m-%d") if result["last_date"] else None
                    _last_date_display = result["last_date"].strftime("%d/%m/%y") if result["last_date"] else None

                    upsert_jockey_trainer_combo(
                        horse_id=horse_data["HorseID"],
                        season=season,
                        jockey=jockey,
                        trainer=trainer,
                        top3_count=top3,
                        total_runs=total,
                        last_race_date=last_date_iso,
                    )

                log("INFO", f"Processed: {horse_id}")
                success += 1
            else:
                log("WARNING", f"No data: {horse_id}")
                failure += 1

        except Exception as e:
            import traceback
            log("ERROR", traceback.format_exc())
            log("ERROR", f"Critical error processing {horse_id}: {e}")
            failure += 1

    log("INFO", f"\nSummary: {success} succeeded, {failure} failed out of {len(horse_ids)}")
    log("INFO", f"Batch completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
