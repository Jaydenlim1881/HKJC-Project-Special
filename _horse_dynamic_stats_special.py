# -----------------------------
# IMPORTS + UTILS_SPECIAL
# -----------------------------
import sys
# sys.path.append('/Users/calvinlim13/miniforge3/lib/python3.12/site-packages')

from special.utils_special import (
    log, sanitize_text, clean_placing, convert_finish_time,
    safe_int, safe_float, parse_weight, parse_lbw,
    get_distance_group, get_turn_count, get_draw_group,
    get_jump_type, get_distance_group_from_row, get_season_code,
    DB_PATH,
)


import sqlite3

from collections import defaultdict
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup, UnicodeDammit
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import ftfy
from typing import List, Dict, Union

# ===== DEBUGGING CONTROL =====
DEBUG_LEVEL = "INFO"  # "OFF", "INFO", "DEBUG", "TRACE"

def get_distance_group_simple(distance: int) -> str:
    if distance < 1000:
        return "Sprint"
    elif distance <= 1400:
        return "Short"
    elif distance < 1800:
        return "Mid"
    elif distance < 2200:
        return "Long"
    else:
        return "Endurance"

def _compute_style_bucket(early_pos: int, field_size: int) -> str | None:
    """Map early position to a style bucket using % of field.
    Uses (pos-1)/(field_size-1) so leader=0.0, last=1.0."""
    if not early_pos or not field_size:
        return None
    try:
        early_pos = int(early_pos)
        field_size = int(field_size)
    except:
        return None
    denom = max(field_size - 1, 1)
    early_pct = (early_pos - 1) / denom

    if early_pct <= 0.15:
        return "Leader"
    elif early_pct <= 0.35:
        return "On-pace"
    elif early_pct <= 0.65:
        return "Stalker"
    else:
        return "Closer"

CHROME_DRIVER_PATH = './chromedriver'

def clean_course_type_text(raw_text):
    """
    Normalize course type text for consistency.
    E.g., "B+2" → "B2", "C+3" → "C3", '"A"' → A
    """
    return raw_text.replace('"', '').replace('+', '').replace('-', '').strip()

def convert_time_to_seconds(time_str):
    """
    Convert time string like '1.09.23' or '58.44' to float seconds.
    """
    if not time_str:
        return None
    parts = time_str.strip().split(".")
    try:
        if len(parts) == 3:
            mins, secs, hundredths = parts
            total_seconds = int(mins) * 60 + int(secs) + int(hundredths) / 100
            return round(total_seconds, 2)
        elif len(parts) == 2:
            secs, hundredths = parts
            total_seconds = int(secs) + int(hundredths) / 100
            return round(total_seconds, 2)
    except Exception as e:
        log("DEBUG", f"Failed to convert finish time '{time_str}': {e}")
    return None

# For distance preferences (simple version)
def get_distance_group_special(race_course: str, course_type: str, distance: int) -> str:
    """Determine a horse's distance preference group for special races."""
    if race_course == "ST":
        if course_type == "AWT":
            if distance <= 1200:
                return "Short"
            if distance <= 1400:
                return "Short"
            elif distance <= 1650:
                return "Mid"
            elif distance <= 2000:
                return "Long"
            else:
                return "Endurance"
        else:  # Turf
            if distance <= 1000:
                return "Sprint"
            elif distance <= 1400:
                return "Short"
            elif distance <= 1800:
                return "Mid"
            elif distance <= 2200:
                return "Long"
            else:
                return "Endurance"

    elif race_course == "HV":  # Happy Valley (always Turf)
        if distance <= 1000:
            return "Sprint"
        elif distance <= 1200:
            return "Short"
        elif distance <= 1800:
            return "Mid"
        elif distance <= 2200:
            return "Long"
        else:
            return "Endurance"

    return "Unknown"

def ensure_column_exists(db_path, table, column, col_type):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cursor.fetchall()]
    if column not in columns:
        log("INFO", f"Adding missing column '{column}' to table '{table}'")
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        conn.commit()
    conn.close()

def build_weight_pref_from_dict(race_history_records, horse_id):
    """
    Build performance stats by DistanceGroup and WeightGroup
    """
    from collections import defaultdict
    
    from bs4 import BeautifulSoup
    from bs4.element import Tag

    # ====== HELPER FUNCTIONS ======
    def get_season_from_row(date_str):
        try:
            date = datetime.strptime(sanitize_text(date_str), "%d/%m/%y")
            return f"{date.year%100:02d}/{(date.year+1)%100:02d}" if date.month >= 9 else f"{(date.year-1)%100:02d}/{date.year%100:02d}"
        except:
            return "Unknown"

    def get_course_from_row(course_info):
        try:
            course_info = sanitize_text(course_info)
            if "AWT" in course_info:
                return "ST"
            parts = course_info.split("/")
            return parts[0].strip() if len(parts) > 0 else "Unknown"
        except:
            return "Unknown"

    def get_course_type_from_row(course_info):
        try:
            course_info = sanitize_text(course_info)
            if "AWT" in course_info:
                return "AWT"
            parts = course_info.split("/")
            return parts[2].strip() if len(parts) > 2 else "Turf"
        except:
            return "Unknown"

    def _compute_style_bucket(early_pos: int, field_size: int) -> str | None:
        """
        Map early position to a style bucket using % of field.
        Uses (pos-1)/(field_size-1) so leader=0.0, last=1.0.
        """
        if not early_pos or not field_size:
            return None
        try:
            early_pos = int(early_pos)
            field_size = int(field_size)
        except:
            return None

        denom = max(field_size - 1, 1)
        early_pct = (early_pos - 1) / denom

        if early_pct <= 0.15:
            return "Leader"
        elif early_pct <= 0.35:
            return "On-pace"
        elif early_pct <= 0.65:
            return "Stalker"
        else:
            return "Closer"

    def get_season_from_row(date_str):
        try:
            date = datetime.strptime(sanitize_text(date_str), "%d/%m/%y")
            return f"{date.year%100:02d}/{(date.year+1)%100:02d}" if date.month >= 9 else f"{(date.year-1)%100:02d}/{date.year%100:02d}"
        except:
            return "Unknown"

    def get_course_from_row(course_info):
        try:
            course_info = sanitize_text(course_info)
            if "AWT" in course_info:
                return "ST"
            parts = course_info.split("/")
            return parts[0].strip() if len(parts) > 0 else "Unknown"
        except:
            return "Unknown"
    
    # ====== MAIN FUNCTION ======
    log("DEBUG", f"\n[WEIGHT_BUILD] Starting for {horse_id}")
    log("TRACE", f"Initial input type: {type(race_history_records)}")
    
    # Conversion for BeautifulSoup rows
    if race_history_records and isinstance(race_history_records[0], Tag):
        log("DEBUG", "Detected BeautifulSoup rows - converting to dicts")
        converted_records = []
        for row in race_history_records:
            try:
                cols = row.find_all("td")
                if len(cols) < 14:
                    continue
                    
                record = {
                    'season': get_season_from_row(cols[2].get_text()),
                    'finish': clean_placing(cols[1].get_text()),
                    'actual_wt': float(sanitize_text(cols[13].get_text())),
                    'distance_group': get_distance_group_from_row(cols[3].get_text(), cols[4].get_text()),
                    'race_course': get_course_from_row(cols[3].get_text()),
                    'course_type': get_course_type_from_row(cols[3].get_text()),
                    'distance': int(cols[4].get_text())
                }
                converted_records.append(record)
            except Exception as e:
                log("DEBUG", f"Conversion error: {str(e)}")
                continue
                
        race_history_records = converted_records
        log("DEBUG", "Converted {len(converted_records)}/{len(race_history_records)} rows")
        if converted_records:
            log("TRACE", "Sample converted record:", converted_records[0])

    # Rest of the processing logic...
    def get_weight_group(weight):
        group = ("Light" if weight < 110 else
                "Low-Mid" if weight <= 116 else
                "Mid" if weight <= 123 else
                "High-Mid" if weight <= 130 else
                "Heavy")
        log("TRACE", f"Weight {weight} → {group}")
        return group

    weight_stats = defaultdict(lambda: defaultdict(lambda: {
        "Top3Count": 0,
        "TotalRuns": 0,
        "WeightSum": 0.0
    }))

    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")
    processed = 0
    skipped = 0

    for race in race_history_records:
        try:
            placing = int(race.get("finish", 0))
            if placing <= 0:
                skipped += 1
                continue
                
            carried_weight = float(race.get("actual_wt", 0))
            if carried_weight > 150:
                log("WARNING", f"Suspicious weight: {carried_weight}lbs")
                skipped += 1
                continue

            season = race.get("season", "Unknown")
            distance_group = race.get("distance_group", "Unknown")
            weight_group = get_weight_group(carried_weight)
            
            stats = weight_stats[season][(distance_group, weight_group)]
            stats["TotalRuns"] += 1
            stats["WeightSum"] += carried_weight
            if placing <= 3:
                stats["Top3Count"] += 1
            processed += 1
        except Exception as e:
            skipped += 1
            log("DEBUG", f"Process error for race record: {str(e)}")
            continue

    results = []
    for season, entries in weight_stats.items():
        for (dist_group, weight_group), stats in entries.items():
            top3 = stats["Top3Count"]
            total = stats["TotalRuns"]
            avg_weight = stats["WeightSum"] / total if total > 0 else 0
            
            rate = round(top3 / total, 3) if total >= 3 else round((top3 / total) * 0.5, 3)
            
            results.append({
                "HorseID": horse_id,
                "Season": season,
                "DistanceGroup": dist_group,
                "WeightGroup": weight_group,
                "CarriedWeight": round(avg_weight, 1),
                "Top3Rate": rate,
                "Top3Count": top3,
                "TotalRuns": total,
                "LastUpdate": last_update
            })

    log("INFO", f"Processed {processed} races, skipped {skipped}")
    log("INFO", f"Generated {len(results)} preference records")
    if results:
        log("DEBUG", f"Sample output record: {results[0]}")
        log("TRACE", f"Weight groups generated: { {r['WeightGroup'] for r in results} }")

    return results

def build_bwr_distance_perf(rows):
    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

    def parse_date(date_str):
        clean_date = sanitize_text(date_str)
        try:
            return datetime.strptime(clean_date, "%d/%m/%y")
        except ValueError:
            return None

    def get_bwr_group(bwr):
        if bwr <= 0.90:
            return "Very Low"
        elif bwr <= 0.98:
            return "Low"
        elif bwr <= 1.04:
            return "Medium Low"
        elif bwr <= 1.10:
            return "Medium"
        elif bwr <= 1.18:
            return "Medium High"
        elif bwr <= 1.34:
            return "High"
        else:
            return "Very High"

    bwr_perf = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"Top3Count": 0, "TotalRuns": 0})))
    today = datetime.today()

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 11:
            continue

        placing = clean_placing(cols[1].get_text())
        date_str = sanitize_text(cols[2].get_text())
        distance_str = sanitize_text(cols[4].get_text())
        actual_wt_str = sanitize_text(cols[13].get_text())      # Act. Wt.
        declared_wt_str = sanitize_text(cols[16].get_text())    # Declar. Horse Wt.

        # Validate all inputs
        if (
            placing is None or not date_str or not distance_str.isdigit() or
            not actual_wt_str.isdigit() or not declared_wt_str.isdigit()
        ):
            continue

        race_date = parse_date(date_str)
        if not race_date:
            continue

        # Determine season from race date
        if race_date.month >= 9:
            season_code = f"{race_date.year % 100:02d}/{(race_date.year + 1) % 100:02d}"
        else:
            season_code = f"{(race_date.year - 1) % 100:02d}/{race_date.year % 100:02d}"

        distance = int(distance_str)
        actual_wt = int(actual_wt_str)
        declared_wt = int(declared_wt_str)

        if declared_wt == 0:
            continue

        bwr = round((actual_wt / declared_wt) * 10, 3)
        bwr_group = get_bwr_group(bwr)

        log("DEBUG", f"BWR = {bwr} → Group = {bwr_group} | ActWt = {actual_wt}, DeclWt = {declared_wt}")

        stats = bwr_perf[season_code][distance][bwr_group]
        stats["TotalRuns"] += 1
        if placing in [1, 2, 3]:
            stats["Top3Count"] += 1

    # Final result
    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")
    result = []

    for season, dist_data in bwr_perf.items():
        for dist, bwr_groups in dist_data.items():
            for bwr_group, stats in bwr_groups.items():
                total = stats["TotalRuns"]
                top3 = stats["Top3Count"]
                rate = round(top3 / total, 3) if total > 0 else 0.0
                result.append({
                    "HorseID": None,  # <-- To be filled in later
                    "Season": season,
                    "Distance": dist,
                    "BWRGroup": bwr_group,
                    "Top3Rate": rate,
                    "Top3Count": top3,
                    "TotalRuns": total,
                    "LastUpdate": last_update
                })

    return result

def upsert_hwtr_trend(hwtr_data):
    """Insert or update HWTR performance into horse_hwtr_trend table"""
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS horse_hwtr_trend (
            HorseID TEXT,
            Season TEXT,
            Class TEXT,
            HWTRGroup TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, Class, HWTRGroup)
        );
    """)

    for row in hwtr_data:
        cursor.execute("""
            REPLACE INTO horse_hwtr_trend (
                HorseID, Season, Class, HWTRGroup, Top3Rate, Top3Count, TotalRuns, LastUpdate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["HorseID"], row["Season"], row["Class"], row["HWTRGroup"],
            row["Top3Rate"], row["Top3Count"], row["TotalRuns"], row["LastUpdate"]
        ))

        log("DEBUG", f"UPSERT HWTR → {row['HorseID']} | {row['Season']} | Class={row['Class']} | Group={row['HWTRGroup']}")

    conn.commit()
    conn.close()

# --- Utility: Group HWTR into buckets for ML ---
def get_hwtr_group(hwtr):
    if hwtr < 0.85:
        return "0.75–0.85"
    elif hwtr < 0.95:
        return "0.85–0.95"
    elif hwtr < 1.05:
        return "0.95–1.05"
    elif hwtr < 1.15:
        return "1.05–1.15"
    elif hwtr < 1.25:
        return "1.15–1.25"
    else:
        return "1.25+"

def build_hwtr_per_class(rows, horse_id):
    """Analyze Historical Weight Trend Ratio (HWTR) by Class per horse and season"""
    from collections import defaultdict
    

    def parse_float(value):
        try:
            return float(value)
        except:
            return 0.0

    hwtr_group = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"top3": 0, "total": 0})))

    for i in range(len(rows) - 1, -1, -1):
        cols = rows[i].find_all("td")
        if len(cols) < 17:
            continue

        try:
            placing_str = cols[1].text.strip()
            placing = int(placing_str) if placing_str.isdigit() else 99
            date = datetime.strptime(cols[2].text.strip(), "%d/%m/%y")
            # Correct HKJC season code logic
            if date.month >= 9:
                season_code = f"{date.year%100:02d}/{(date.year+1)%100:02d}"
            else:
                season_code = f"{(date.year-1)%100:02d}/{date.year%100:02d}"
            cls = sanitize_text(cols[6].text).upper()
            if cls in ("GRIFFIN", "GRF"):
                cls = "6"
            actual_wt = parse_float(cols[13].text.strip())
            declared_wt = parse_float(cols[16].text.strip())
        except:
            continue

        # Skip invalid weights
        if actual_wt <= 0 or declared_wt <= 0:
            continue

        # Look back at up to 3 previous races
        history = []
        for j in range(i - 1, -1, -1):
            prev_cols = rows[j].find_all("td")
            if len(prev_cols) < 17:
                continue
            prev_actual = parse_float(prev_cols[13].text.strip())
            if prev_actual > 0:
                history.append(prev_actual)
            if len(history) >= 3:
                break

        # Fix #1: Relax history requirement to minimum 2 past races
        if len(history) < 2:
            continue

        avg_prev_wt = sum(history) / len(history)
        hwtr = actual_wt / avg_prev_wt if avg_prev_wt > 0 else 0.0

        # Define HWTR buckets
        if hwtr < 0.85:
            group = "<0.85"
        elif hwtr < 0.95:
            group = "0.85–0.95"
        elif hwtr < 1.05:
            group = "0.95–1.05"
        elif hwtr < 1.15:
            group = "1.05–1.15"
        else:
            group = "1.15+"

        hwtr_group[season_code][cls][group]["total"] += 1
        if placing <= 3:
            hwtr_group[season_code][cls][group]["top3"] += 1

    # Format result
    result = []
    for season_code in hwtr_group:
        for cls in hwtr_group[season_code]:
            for group in hwtr_group[season_code][cls]:
                top3 = hwtr_group[season_code][cls][group]["top3"]
                total = hwtr_group[season_code][cls][group]["total"]

            # Fix #2: Apply fallback logic for small sample sizes
            if total < 3 and top3 > 0:
                top3rate = (top3 / total) * 0.5
            else:
                top3rate = top3 / total if total > 0 else 0.0

            result.append({
                "HorseID": horse_id,
                "Season": season_code,
                "Class": cls,
                "HWTRGroup": group,
                "Top3Rate": round(top3rate, 3),
                "Top3Count": top3,
                "TotalRuns": total,
                "LastUpdate": datetime.now().strftime("%Y/%m/%d %H:%M")
            })

    # Fix #3: Debug output
    for r in result:
        log("DEBUG", f"HWTR {r['HorseID']} | Class={r['Class']} | Group={r['HWTRGroup']} | Top3Rate={r['Top3Rate']:.2f}")
    
    return result

def upsert_running_position(data_dict):
    """Insert or update a single race's running position entry"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

    race_date = data_dict.get("RaceDate")
    if race_date:
        # Normalize to ISO format (YYYY-MM-DD) for consistent storage
        for fmt in ("%Y-%m-%d", "%d/%m/%y", "%Y/%m/%d"):
            try:
                race_date = datetime.strptime(race_date, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        else:
            race_date = None
    
    cursor.execute("""
        INSERT INTO horse_running_position (
            HorseID, RaceDate, RaceID, RaceNo, Season,
            RaceCourse, CourseType,
            DistanceGroup, TurnCount,
            EarlyPos, MidPos, FinalPos, FinishTime,
            Placing, FieldSize, LastUpdate
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(HorseID, RaceID) DO UPDATE SET
        RaceDate     = excluded.RaceDate,
        RaceNo       = excluded.RaceNo,
        Season       = excluded.Season,
        RaceCourse   = excluded.RaceCourse,
        CourseType   = excluded.CourseType,
        DistanceGroup= excluded.DistanceGroup,
        TurnCount    = excluded.TurnCount,
        EarlyPos     = excluded.EarlyPos,
        MidPos       = excluded.MidPos,
        FinalPos     = excluded.FinalPos,
        FinishTime   = excluded.FinishTime,
        Placing      = excluded.Placing,
        LastUpdate   = excluded.LastUpdate,
        -- Only change FieldSize if the current value is NULL or 0
        FieldSize    = CASE
                         WHEN horse_running_position.FieldSize IS NULL
                              OR horse_running_position.FieldSize = 0
                         THEN excluded.FieldSize
                         ELSE horse_running_position.FieldSize
                       END
""", (
    data_dict.get("HorseID"),
    race_date,  # (normalized above)
    data_dict.get("RaceID"),
    data_dict.get("RaceNo"),
    data_dict.get("Season"),
    data_dict.get("RaceCourse"),
    data_dict.get("CourseType"),
    data_dict.get("DistanceGroup"),
    data_dict.get("TurnCount"),
    data_dict.get("EarlyPos"),
    data_dict.get("MidPos"),
    data_dict.get("FinalPos"),
    data_dict.get("FinishTime"),
    data_dict.get("Placing"),
    data_dict.get("FieldSize"),  # may be None -> preserved
    last_update
    ))

    conn.commit()
    conn.close()

def build_exact_distance_pref(rows):
    def parse_date(date_str):
        clean_date = sanitize_text(date_str)
        try:
            return datetime.strptime(clean_date, "%d/%m/%y")
        except ValueError:
            return None

    distance_pref = defaultdict(lambda: defaultdict(lambda: {"top3": 0, "runs": 0}))
    race_info_list = []
    today = datetime.today()

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue

        placing = clean_placing(cols[1].get_text())
        date_str = sanitize_text(cols[2].get_text())
        distance_str = sanitize_text(cols[4].get_text())
        going_str = sanitize_text(cols[5].get_text())

        if placing is None or not date_str or not distance_str.isdigit():
            continue

        distance = int(distance_str)
        race_date = parse_date(date_str)
        if not race_date:
            continue

        if race_date.month >= 9:
            season_code = f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}"
        else:
            season_code = f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"

        race_info_list.append({
            "season": season_code,
            "Going": going_str,
            "FinishPosition": placing
        })

        # CHANGED: Only use simple distance grouping for distance preferences
        distance_group = get_distance_group_simple(distance)  # Changed from get_distance_group()
        
        stats = distance_pref[season_code][distance_group]
        stats["runs"] += 1
        if placing in [1, 2, 3]:
            stats["top3"] += 1

    final_result = defaultdict(dict)
    for season, dists in distance_pref.items():
        for group, stats in dists.items():
            runs = stats["runs"]
            top3 = stats["top3"]
            rate = (top3 / runs) if runs > 0 else 0.0
            if runs < 3:
                rate /= 2
            final_result[season][group] = {
                "Top3Rate": round(rate, 3),
                "Top3Count": top3,
                "TotalRuns": runs
            }

    final_result["_races"] = race_info_list
    return final_result

def upsert_distance_pref(horse_id, season, distance_pref):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS horse_distance_pref (
            HorseID TEXT,
            Season TEXT,
            DistanceGroup TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, DistanceGroup)
        )
    ''')

    # ✅ Add these to ensure missing columns are added safely
    ensure_column_exists(DB_PATH, "horse_distance_pref", "Top3Count", "INTEGER")
    ensure_column_exists(DB_PATH, "horse_distance_pref", "TotalRuns", "INTEGER")
    ensure_column_exists(DB_PATH, "horse_distance_pref", "LastUpdate", "TEXT")


    for season, dists in distance_pref.items():
        last_update = datetime.now().strftime("%Y/%m/%d %H:%M")
        if season == "_races":
            continue
        for dist, values in dists.items():
            cursor.execute('''
                INSERT OR REPLACE INTO horse_distance_pref (
                    HorseID, Season, DistanceGroup,
                    Top3Rate, Top3Count, TotalRuns, LastUpdate
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                horse_id, season, dist,
                values["Top3Rate"], values["Top3Count"],
                values["TotalRuns"],
                last_update
            ))

    conn.commit()
    conn.close()

def create_horse_jockey_combo_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS horse_jockey_combo (
            HorseID TEXT,
            Season TEXT,
            Jockey TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, Jockey)
        );
    """)
    conn.commit()
    conn.close()

def upsert_horse_jockey_combo(horse_id, rows):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

    # Ensure table and columns exist
    create_horse_jockey_combo_table()
    ensure_column_exists(DB_PATH, "horse_jockey_combo", "LastUpdate", "TEXT")
    ensure_column_exists(DB_PATH, "horse_jockey_combo", "LastRaceDate", "TEXT")
    ensure_column_exists(DB_PATH, "horse_jockey_combo", "LastUpdate", "TEXT")

    # Helper: parse date
    def parse_date(date_str):
        clean_date = sanitize_text(date_str)
        try:
            return datetime.strptime(clean_date, "%d/%m/%y")
        except ValueError:
            return None
    
    try:
        sorted_rows = sorted(
            rows,
            key=lambda row: parse_date(row.find_all("td")[2].get_text(strip=True)) or datetime.min,
            reverse=True
        )
    except Exception as e:
        log("DEBUG", f"Sort error for {horse_id}: {e}")
        sorted_rows = rows

    if sorted_rows:
        try:
            newest = sorted_rows[0].find_all("td")[2].get_text(strip=True)
            oldest = sorted_rows[-1].find_all("td")[2].get_text(strip=True)
            log("DEBUG", f"Processing {len(sorted_rows)} races ({newest} to {oldest})")
        except Exception as debug_e:
            log("DEBUG", f"Debug error: {debug_e}")

    ## Process statistics
    stats_dict = defaultdict(lambda: defaultdict(lambda: {
        "Top3Count": 0,
        "TotalRuns": 0,
        "LastRaceDate": "",
        "LastRaceDateDisplay": "",
        "LatestDateObj": None  # Corrected line - no extra parenthesis
    }))

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 11:
            continue

        try:
            placing = clean_placing(cols[1].get_text())
            date_str = sanitize_text(cols[2].get_text())
            race_date = parse_date(date_str)
            jockey_tag = cols[10].find("a")
            jockey_name = sanitize_text(jockey_tag.get_text()) if jockey_tag else None

            if placing is None or not race_date or not jockey_name:
                continue

            # Convert to season format
            if race_date.month >= 9:
                season_code = f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}"
            else:
                season_code = f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"

            stats = stats_dict[season_code][jockey_name]
            stats["TotalRuns"] += 1
            if placing in [1, 2, 3]:
                stats["Top3Count"] += 1
            
            # Track most recent date
            if (stats["LatestDateObj"] is None or
                race_date > stats["LatestDateObj"]):
                stats["LatestDateObj"] = race_date
                # Store ISO for DB, keep original for display if needed
                stats["LastRaceDate"] = race_date.strftime("%Y-%m-%d")
                stats["LastRaceDateDisplay"] = date_str

        except Exception as e:
            log("DEBUG", f"Row processing error: {e}")
            continue

    for season, jockeys in stats_dict.items():
        for jockey, values in jockeys.items():
            runs = values["TotalRuns"]
            top3 = values["Top3Count"]
            top3_rate = (top3 / runs) if runs > 0 else 0.0
            if runs < 3:
                top3_rate /= 2
            top3_rate = round(top3_rate, 4)

            cursor.execute('''
                INSERT OR REPLACE INTO horse_jockey_combo (
                    HorseID, Season, Jockey,
                    Top3Rate, Top3Count, TotalRuns,
                    LastRaceDate, LastUpdate
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                horse_id, season, jockey,
                top3_rate, top3, runs,
                values["LastRaceDate"],  # Now properly included
                last_update
            ))

    conn.commit()
    conn.close()

def create_bwr_distance_perf_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS horse_bwr_distance_pref (
            HorseID TEXT,
            Season TEXT,
            Distance INTEGER,
            BWRGroup TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, Distance, BWRGroup)
        )
    ''')
    conn.commit()
    conn.close()

def create_running_style_pref_table():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS horse_running_style_pref (
            HorseID TEXT,
            Season TEXT,
            RaceCourse TEXT,            -- ST / HV
            CourseType TEXT,            -- A, B, C, C+3, AWT (Turf unless AWT)
            DistanceGroup TEXT,
            TurnCount REAL,
            StyleBucket TEXT,           -- Leader / On-pace / Stalker / Closer
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (
                HorseID, Season, RaceCourse, CourseType,
                DistanceGroup, TurnCount, StyleBucket)
        );
    """)
    conn.commit()
    conn.close()

def migrate_turncount_to_real(db_path=DB_PATH):
    import sqlite3
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # --- horse_running_position ---
    cur.execute("PRAGMA table_info(horse_running_position)")
    cols = [r[1:] for r in cur.fetchall()]  # (cid,name,type,notnull,default,pk)
    # If TurnCount already REAL, skip
    tc_type = next((r[1] for r in cols if r[0] == "TurnCount"), None)
    if tc_type and tc_type.upper() != "REAL":
        cur.execute("ALTER TABLE horse_running_position RENAME TO horse_running_position_old")
        cur.execute("""
            CREATE TABLE horse_running_position (
                HorseID TEXT,
                RaceDate TEXT,
                RaceID TEXT,
                RaceNo TEXT,
                Season TEXT,
                RaceCourse TEXT,
                CourseType TEXT,
                DistanceGroup TEXT,
                TurnCount REAL,
                EarlyPos INTEGER,
                MidPos REAL,
                FinalPos INTEGER,
                FinishTime REAL,
                Placing INTEGER,
                FieldSize INTEGER,
                LastUpdate TEXT,
                PRIMARY KEY (HorseID, RaceID)
            );
        """)
        cur.execute("""
            INSERT INTO horse_running_position
            (HorseID, RaceDate, RaceID, RaceNo, Season, RaceCourse, CourseType, DistanceGroup,
             TurnCount, EarlyPos, MidPos, FinalPos, FinishTime, Placing, FieldSize, LastUpdate)
            SELECT HorseID, RaceDate, NULL AS RaceID, RaceNo, Season, RaceCourse, CourseType, DistanceGroup,
                   TurnCount, EarlyPos, MidPos, FinalPos, FinishTime, Placing, FieldSize, LastUpdate
            FROM horse_running_position_old
        """)
        cur.execute("DROP TABLE horse_running_position_old")

    # --- horse_running_style_pref ---
    cur.execute("PRAGMA table_info(horse_running_style_pref)")
    cols = [r[1:] for r in cur.fetchall()]
    tc_type = next((r[1] for r in cols if r[0] == "TurnCount"), None)
    if tc_type and tc_type.upper() != "REAL":
        cur.execute("ALTER TABLE horse_running_style_pref RENAME TO horse_running_style_pref_old")
        cur.execute("""
            CREATE TABLE horse_running_style_pref (
                HorseID TEXT,
                Season TEXT,
                RaceCourse TEXT,
                CourseType TEXT,
                DistanceGroup TEXT,
                TurnCount REAL,
                StyleBucket TEXT,
                Top3Rate REAL,
                Top3Count INTEGER,
                TotalRuns INTEGER,
                LastUpdate TEXT,
                PRIMARY KEY (HorseID, Season, RaceCourse, CourseType, DistanceGroup, TurnCount, StyleBucket)
            );
        """)
        cur.execute("""
            INSERT INTO horse_running_style_pref
            (HorseID, Season, RaceCourse, CourseType, DistanceGroup, TurnCount,
             StyleBucket, Top3Rate, Top3Count, TotalRuns, LastUpdate)
            SELECT HorseID, Season, RaceCourse, CourseType, DistanceGroup, TurnCount,
                   StyleBucket, Top3Rate, Top3Count, TotalRuns, LastUpdate
            FROM horse_running_style_pref_old
        """)
        cur.execute("DROP TABLE horse_running_style_pref_old")

    # Clear existing rows so they can be rebuilt with precise TurnCount values
    cur.execute("DELETE FROM horse_running_style_pref")

    conn.commit()
    conn.close()

    # Rebuild style preferences to retain original fractional TurnCount values
    try:
        rebuild_running_style_pref()
    except Exception as e:
        log("WARNING", f"Failed to rebuild running_style_pref: {e}")

def create_running_position_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS horse_running_position (
            HorseID TEXT,
            RaceDate TEXT,
            RaceID TEXT NOT NULL,
            RaceNo TEXT,
            Season TEXT,
            RaceCourse TEXT,
            CourseType TEXT,
            DistanceGroup TEXT,
            TurnCount REAL,
            EarlyPos INTEGER,
            MidPos REAL,
            FinalPos INTEGER,
            FinishTime REAL,
            Placing INTEGER,
            FieldSize INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, RaceID)
        );
    """)
    conn.commit()
    conn.close()
    ensure_column_exists(DB_PATH, "horse_running_position", "Placing", "INTEGER")

def upsert_bwr_distance_perf(horse_id, bwr_perf_list):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    create_bwr_distance_perf_table()  # Ensure table exists

    for row in bwr_perf_list:
        last_update = datetime.now().strftime("%Y/%m/%d %H:%M")
        row["HorseID"] = horse_id  # Inject the HorseID into each row

        cursor.execute("""
            INSERT INTO horse_bwr_distance_pref (
                HorseID, Season, Distance, BWRGroup,
                Top3Rate, Top3Count, TotalRuns, LastUpdate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(HorseID, Season, Distance, BWRGroup)
            DO UPDATE SET
                Top3Rate = excluded.Top3Rate,
                Top3Count = excluded.Top3Count,
                TotalRuns = excluded.TotalRuns,
                LastUpdate = excluded.LastUpdate
        """, (
            row["HorseID"], row["Season"], row["Distance"], row["BWRGroup"],
            row["Top3Rate"], row["Top3Count"], row["TotalRuns"],
            last_update
        ))

    conn.commit()
    conn.close()

def create_trainer_combo_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS horse_trainer_combo (
            HorseID TEXT,
            Season TEXT,
            Trainer TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, Trainer)
        );
    """)
    conn.commit()
    conn.close()

def create_race_field_size_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS race_field_size (
            RaceDate TEXT,
            RaceNo TEXT,
            RaceCourse TEXT,
            FieldSize INTEGER,
            PRIMARY KEY (RaceDate, RaceNo, RaceCourse)
        )
    ''')
    conn.commit()
    conn.close()

def migrate_jockey_trainer_table():
    """Ensures LastRaceDate column exists"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(horse_jockey_trainer_combo)")
        if not any(col[1] == 'LastRaceDate' for col in cursor.fetchall()):
            log("INFO", "Adding LastRaceDate column")
            cursor.execute("ALTER TABLE horse_jockey_trainer_combo ADD COLUMN LastRaceDate TEXT")
            conn.commit()
    except Exception as e:
        log("ERROR", f"[MIGRATION ERROR] {e}")
    finally:
        conn.close()

def create_jockey_trainer_combo_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS horse_jockey_trainer_combo (
            HorseID TEXT,
            Season TEXT,
            Jockey TEXT,
            Trainer TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastRaceDate TEXT,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, Jockey, Trainer)
        )
    """)
    conn.commit()
    conn.close()

def create_draw_pref_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS horse_draw_pref (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            HorseID TEXT,
            Season TEXT,
            RaceCourse TEXT,
            DistanceGroup TEXT,
            DrawGroup TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (HorseID, Season, RaceCourse, DistanceGroup, DrawGroup)
        )
    ''')
    conn.commit()
    conn.close()

def create_going_pref_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS horse_going_pref (
            HorseID TEXT,
            Season TEXT,
            GoingType TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, GoingType)
        );
    """)
    conn.commit()
    conn.close()

def create_weight_pref_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='horse_weight_pref'")
    table_exists = cursor.fetchone()

    if not table_exists:
        cursor.execute("""
            CREATE TABLE horse_weight_pref (
                HorseID TEXT,
                Season TEXT,
                DistanceGroup TEXT,
                WeightGroup TEXT,
                CarriedWeight REAL,  -- ✅ store actual carried weight (avg or rep)
                Top3Rate REAL,
                Top3Count INTEGER,
                TotalRuns INTEGER,
                LastUpdate TEXT,
                PRIMARY KEY (HorseID, Season, DistanceGroup, WeightGroup)
            )
        """)
        conn.commit()
        conn.close()
        return

    cursor.execute("PRAGMA table_info(horse_weight_pref)")
    pk_columns = {row[1] for row in cursor.fetchall() if row[5] > 0}
    required_pk = {"HorseID", "Season", "DistanceGroup", "WeightGroup"}

    if pk_columns != required_pk:
        log("INFO", "Migrating horse_weight_pref table to add primary key and remove duplicates")
        cursor.execute("""
            CREATE TABLE horse_weight_pref_new (
                HorseID TEXT,
                Season TEXT,
                DistanceGroup TEXT,
                WeightGroup TEXT,
                CarriedWeight REAL,
                Top3Rate REAL,
                Top3Count INTEGER,
                TotalRuns INTEGER,
                LastUpdate TEXT,
                PRIMARY KEY (HorseID, Season, DistanceGroup, WeightGroup)
            )
        """)
        cursor.execute("""
            INSERT OR IGNORE INTO horse_weight_pref_new (
                HorseID, Season, DistanceGroup, WeightGroup, CarriedWeight,
                Top3Rate, Top3Count, TotalRuns, LastUpdate
            )
            SELECT
                HorseID, Season, DistanceGroup, WeightGroup, CarriedWeight,
                Top3Rate, Top3Count, TotalRuns, LastUpdate
            FROM horse_weight_pref
        """)
        cursor.execute("DROP TABLE horse_weight_pref")
        cursor.execute("ALTER TABLE horse_weight_pref_new RENAME TO horse_weight_pref")
        conn.commit()

    conn.close()

def create_class_jump_pref_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS horse_class_jump_pref (
            HorseID    TEXT,
            Season     TEXT,
            JumpType   TEXT,   -- 'Up', 'Down', 'Same'
            Top3Rate   REAL,
            Top3Count  INTEGER,
            TotalRuns  INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, JumpType)
        );
    """)
    conn.commit()
    conn.close()

def create_horse_rating_table(db_path=DB_PATH):
    import sqlite3
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Create with LastUpdate as the LAST column
    cur.execute("""
        CREATE TABLE IF NOT EXISTS horse_rating (
            HorseID TEXT,
            Season TEXT,
            AsOfDate TEXT,
            OfficialRating REAL,
            RatingStartSeason REAL,
            RatingStartCareer REAL,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, AsOfDate)
        );
    """)
    # If table existed from an earlier version, ensure LastUpdate exists
    cur.execute("PRAGMA table_info(horse_rating)")
    cols = [c[1] for c in cur.fetchall()]
    if "LastUpdate" not in cols:
        cur.execute("ALTER TABLE horse_rating ADD COLUMN LastUpdate TEXT")
    conn.commit()
    conn.close()

def upsert_horse_rating(
    horse_id: str,
    season: str,
    as_of_date: str,           # 'YYYY-MM-DD'
    official_rating: float,
    rating_start_season: float,
    rating_start_career: float,
    db_path=DB_PATH
):
    import sqlite3
    from datetime import datetime
    last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO horse_rating (
            HorseID, Season, AsOfDate, OfficialRating, RatingStartSeason, RatingStartCareer, LastUpdate
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(HorseID, Season, AsOfDate) DO UPDATE SET
          OfficialRating      = excluded.OfficialRating,
          RatingStartSeason   = excluded.RatingStartSeason,
          RatingStartCareer   = excluded.RatingStartCareer,
          LastUpdate          = excluded.LastUpdate;
    """, (horse_id, season, as_of_date, official_rating, rating_start_season, rating_start_career, last_update))
    conn.commit()
    conn.close()

def upsert_weight_pref(horse_id, weight_pref_list):
    import sqlite3
    

    # ====== 1. INITIAL DEBUG ======
    log("INFO", f"\nStarting upsert for {horse_id}")
    log("INFO", f"Received {len(weight_pref_list)} weight preference records")
    if weight_pref_list:
        log("DEBUG", f"Sample record to upsert: {weight_pref_list[0]}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ====== 2. TABLE VERIFICATION ======
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='horse_weight_pref'")
    if not cursor.fetchone():
        log("ERROR", "horse_weight_pref table doesn't exist!")
        create_weight_pref_table()
        log("INFO", "Recreated horse_weight_pref table")

    # ====== 3. TRANSACTION COUNTER ======
    success_count = 0
    error_count = 0

    for i, row in enumerate(weight_pref_list):
        try:
            # ====== 4. ROW VALIDATION ======
            if not all(key in row for key in ['Season', 'DistanceGroup', 'WeightGroup']):
                log("WARNING", f"Malformed row (missing keys): {row}")
                error_count += 1
                continue

            # ====== 5. DATA PROCESSING ======
            top3 = int(row.get("Top3Count", 0))
            total = int(row.get("TotalRuns", 0))
            
            # Calculate rate with small sample adjustment
            rate = round((top3 / total) if total >= 3 else (top3 / total) * 0.5, 4) if total > 0 else 0.0
            last_update = row.get("LastUpdate") or datetime.now().strftime("%Y/%m/%d %H:%M")

            # ====== 6. DEBUG BEFORE INSERT ======
            log("DEBUG", f"\nRecord {i+1}:")
            log("DEBUG", f"  Season: {row['Season']}")
            log("DEBUG", f"  DistGroup: {row['DistanceGroup']}")
            log("DEBUG", f"  WeightGroup: {row['WeightGroup']}")
            log("DEBUG", f"  CarriedWeight: {row.get('CarriedWeight', 'None')}")
            log("DEBUG", f"  Stats: {top3}/{total} (Rate: {rate})")

            # ====== 7. EXECUTE UPSERT ======
            cursor.execute("""
                INSERT INTO horse_weight_pref (
                    HorseID, Season, DistanceGroup, WeightGroup, CarriedWeight,
                    Top3Rate, Top3Count, TotalRuns, LastUpdate
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(HorseID, Season, DistanceGroup, WeightGroup) DO UPDATE SET
                    CarriedWeight=excluded.CarriedWeight,
                    Top3Rate=excluded.Top3Rate,
                    Top3Count=excluded.Top3Count,
                    TotalRuns=excluded.TotalRuns,
                    LastUpdate=excluded.LastUpdate
            """, (
                horse_id,
                str(row['Season']),
                str(row['DistanceGroup']),
                str(row['WeightGroup']),
                float(row.get('CarriedWeight', 0)) if row.get('CarriedWeight') else None,
                rate,
                top3,
                total,
                last_update
            ))
            success_count += 1

        except Exception as e:
            error_count += 1
            log("ERROR", f"Failed to upsert record {i+1}: {str(e)}")  # Main error message
            log("DEBUG", f"Problematic row: {row}")  # Detailed debug info
            continue

    # ====== 8. FINAL COMMIT AND REPORT ======
    try:
        conn.commit()
        log("INFO", f"\n[WEIGHT_UPSERT] Completed - {success_count} successful, {error_count} failed")
        
        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM horse_weight_pref WHERE HorseID=?", (horse_id,))
        count = cursor.fetchone()[0]
        print(f"Total records for {horse_id} in DB: {count}")
        
    except Exception as e:
        print(f"[CRITICAL] Commit failed: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

def upsert_trainer_combo(horse_id, trainer_combo_dict):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for season, trainer_dict in trainer_combo_dict.items():
        for trainer, stats in trainer_dict.items():
            top3 = stats['Top3Count']
            total = stats['TotalRuns']
            rate = round(top3 / total, 3) if total > 0 else 0.0
            if total < 3:
                rate /= 2
            rate = round(rate, 4)

            last_update = datetime.now().strftime("%Y/%m/%d %H:%M")
            cursor.execute("""
                INSERT INTO horse_trainer_combo 
                (HorseID, Season, Trainer, Top3Rate, Top3Count, TotalRuns, LastUpdate)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(HorseID, Season, Trainer)
                DO UPDATE SET
                    Top3Rate=excluded.Top3Rate,
                    Top3Count=excluded.Top3Count,
                    TotalRuns=excluded.TotalRuns,
                    LastUpdate=excluded.LastUpdate
            """, (horse_id, season, trainer, rate, top3, total, last_update))

    conn.commit()
    conn.close()

def upsert_going_pref(horse_id, going_pref_dict):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS horse_going_pref (
            HorseID TEXT,
            Season TEXT,
            GoingType TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, GoingType)
        );
    ''')

    ensure_column_exists(DB_PATH, "horse_going_pref", "Top3Count", "INTEGER")
    ensure_column_exists(DB_PATH, "horse_going_pref", "TotalRuns", "INTEGER")
    ensure_column_exists(DB_PATH, "horse_going_pref", "LastUpdate", "TEXT")

    for season, goings in going_pref_dict.items():
        for going_type, stats in goings.items():
            last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

            total = stats["total"]
            top3 = stats["top3"]
            if total > 0:
                rate = top3 / total
                if total < 3:
                    rate /= 2
                top3_rate = round(rate, 4)

                cursor.execute('''
                    INSERT OR REPLACE INTO horse_going_pref (
                        HorseID, Season, GoingType,
                        Top3Rate, Top3Count, TotalRuns, LastUpdate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    horse_id, season, going_type,
                    top3_rate, top3, total, last_update
                ))

    conn.commit()
    conn.close()

def upsert_draw_pref(horse_id, draw_pref_dict):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

    for season, combos in draw_pref_dict.items():
        for (race_course, distance_group, draw_group), values in combos.items():
            top3 = values['Top3Count']
            total = values['TotalRuns']
            rate = round(top3 / total, 3) if total > 0 else 0.0
            if total < 3:
                rate /= 2

            cursor.execute(
                """
                INSERT INTO horse_draw_pref (
                    HorseID, Season, RaceCourse, DistanceGroup, DrawGroup,
                    Top3Rate, Top3Count, TotalRuns, LastUpdate
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    horse_id,
                    season,
                    race_course,
                    distance_group,
                    draw_group,
                    rate,
                    top3,
                    total,
                    last_update,
                ),
            )

    conn.commit()
    conn.close()

def upsert_jockey_trainer_combo(horse_id, season, jockey, trainer, top3_count, total_runs, last_race_date=None):
    """
    Final fixed version that:
    1. Properly initializes stats
    2. Fixes SQL syntax errors
    3. Provides clean error handling
    """
    # Initialize stats at function start
    stats = {
        'attempted': 1,  # We're attempting this operation
        'successful': 0,
        'warnings': 0
    }

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Calculate success rate with validation
        try:
            rate = round((top3_count / total_runs), 4) if total_runs > 0 else 0.0
            if total_runs < 3:
                rate = round(rate * 0.5, 4)
                log("DEBUG", f"Small sample adjustment for {horse_id}")
        except ZeroDivisionError:
            rate = 0.0
            stats['warnings'] += 1
            log("WARNING", f"Zero division for {horse_id}")

        # Validate date format (expect ISO)
        try:
            validated_date = (
                datetime.strptime(last_race_date, "%Y-%m-%d").strftime("%Y-%m-%d")
                if last_race_date
                else datetime.now().strftime("%Y-%m-%d")
            )
        except ValueError:
            validated_date = datetime.now().strftime("%Y-%m-%d")
            stats['warnings'] += 1
            log("WARNING", f"Invalid date format for {horse_id}, using current date")

        # Fixed SQL query - removed ellipsis and added complete column list
        query = """
            INSERT INTO horse_jockey_trainer_combo (
                HorseID, Season, Jockey, Trainer,
                Top3Rate, Top3Count, TotalRuns,
                LastRaceDate, LastUpdate
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(HorseID, Season, Jockey, Trainer)
            DO UPDATE SET
                Top3Rate = excluded.Top3Rate,
                Top3Count = excluded.Top3Count,
                TotalRuns = excluded.TotalRuns,
                LastRaceDate = excluded.LastRaceDate,
                LastUpdate = excluded.LastUpdate
        """

        params = (
            horse_id, season, jockey, trainer,
            rate, top3_count, total_runs,
            validated_date,
            datetime.now().strftime("%Y/%m/%d %H:%M")
        )

        cursor.execute(query, params)
        conn.commit()
        stats['successful'] = 1

    except sqlite3.Error as e:
        stats['warnings'] += 1
        print(f"[DB ERROR] {horse_id}: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        stats['warnings'] += 1
        print(f"[ERROR] {horse_id}: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

        # Single clean stats output
        log("DEBUG", f"\n{horse_id} Result:")
        log("DEBUG", f"  Combo: {jockey}/{trainer}")
        log("DEBUG", f"  Status: {'SUCCESS' if stats['successful'] else 'FAILED'}")
        log("DEBUG", f"  Warnings: {stats['warnings']}")

def build_course_pref(rows):
    def parse_date(date_str):
        clean_date = sanitize_text(date_str)
        try:
            return datetime.strptime(clean_date, "%d/%m/%y")
        except ValueError:
            return None

    course_pref = defaultdict(lambda: defaultdict(lambda: {"top3": 0, "runs": 0}))

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue

        placing = clean_placing(cols[1].get_text())
        date_str = sanitize_text(cols[2].get_text())
        raw_course = sanitize_text(cols[3].get_text())
        race_date = parse_date(date_str)

        if placing is None or not race_date or not raw_course:
            continue

        if race_date.month >= 9:
            season_code = f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}"
        else:
            season_code = f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"

        if "AWT" in raw_course:
            race_course = "ST"
            course_type = "AWT"
        else:
            parts = raw_course.split("/")
            if len(parts) >= 3:
                race_course = parts[0].strip()
                course_type = parts[2].replace('"', '').strip()
            else:
                continue

        stats = course_pref[season_code][(race_course, course_type)]
        stats["runs"] += 1
        if placing in [1, 2, 3]:
            stats["top3"] += 1

    result = defaultdict(dict)
    for season, courses in course_pref.items():
        for (race_course, course_type), stats in courses.items():
            runs = stats["runs"]
            top3 = stats["top3"]
            rate = (top3 / runs) if runs > 0 else 0.0
            if runs < 3:
                rate /= 2
            result[season][(race_course, course_type)] = {
                "Top3Rate": round(rate, 3),
                "Top3Count": top3,
                "TotalRuns": runs
            }

    return result

def build_class_jump_pref(rows):
    """
    Build per-season stats for class movement between consecutive races.
    JumpType:
      - 'Up'   : current class number < previous class number (e.g., C4 -> C3)
      - 'Down' : current class number > previous class number (e.g., C3 -> C4)
      - 'Same' : same class number
    Ignores group classes (G1, G2, G3).
    Griffin races are treated as class 6 rather than ignored.
    """
    import re

    def parse_date(ds):
        try:
            return datetime.strptime(sanitize_text(ds), "%d/%m/%y")
        except Exception:
            return None

    def season_from_date(d: datetime) -> str:
        # HK season starts in September
        return (f"{d.year%100:02d}/{(d.year+1)%100:02d}" if d.month >= 9
                else f"{(d.year-1)%100:02d}/{d.year%100:02d}")

    def class_to_int(txt):
        t = sanitize_text(txt).upper()
        if not t:
            return None
        if any(k in t for k in ["G1", "G2", "G3"]):
            return None
        if "GRIFFIN" in t or "GRF" in t:
            return 6
        m = re.search(r"(\d+)", t)
        return int(m.group(1)) if m else None

    # Build chronological list: (date_dt, placing, class_int)
    races = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 8:
            continue

        placing = clean_placing(cols[1].get_text())
        date_str = sanitize_text(cols[2].get_text())
        d = parse_date(date_str)
        if placing is None or not d:
            continue

        # Class can appear at col 6 or 7 depending on table variant
        cls_txt = ""
        try:
            cls_txt = sanitize_text(cols[6].get_text())
        except Exception:
            pass
        # fallback if empty or clearly not class
        if not cls_txt or not re.search(r"\d", cls_txt):
            if len(cols) > 7:
                cls_txt = sanitize_text(cols[7].get_text())

        races.append((d, placing, class_to_int(cls_txt)))

    # Sort oldest -> newest so we can compare with previous race
    races.sort(key=lambda x: x[0])

    stats = defaultdict(lambda: defaultdict(lambda: {"Top3Count": 0, "TotalRuns": 0}))
    prev_cls = None
    prev_valid = False

    for d, placing, curr_cls in races:
        season_code = season_from_date(d)

        jump = None
        if prev_valid and curr_cls is not None:
            try:
                jt = get_jump_type(prev_cls, curr_cls)  # uses your utils_special if available
                jump = jt if jt in ("Up", "Down", "Same") else None
            except Exception:
                jump = None
            if jump is None:
                jump = "Up" if curr_cls < prev_cls else ("Down" if curr_cls > prev_cls else "Same")

        if curr_cls is not None:
            prev_cls = curr_cls
            prev_valid = True

        if not jump:
            continue

        s = stats[season_code][jump]
        s["TotalRuns"] += 1
        if placing in (1, 2, 3):
            s["Top3Count"] += 1

    # Sort seasons in descending order (newest first)
    sorted_stats = {}
    for season in sorted(stats.keys(), key=lambda s: int(s[:2]), reverse=True):
        sorted_stats[season] = stats[season]

    return sorted_stats

def build_draw_pref(rows):
    def parse_date(date_str):
        clean_date = sanitize_text(date_str)
        try:
            return datetime.strptime(clean_date, "%d/%m/%y")
        except ValueError:
            return None

    draw_pref = defaultdict(lambda: defaultdict(lambda: {"Top3Count": 0, "TotalRuns": 0}))

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 8:
            continue

        placing = clean_placing(cols[1].get_text())
        date_str = sanitize_text(cols[2].get_text())
        raw_course = sanitize_text(cols[3].get_text())
        distance_str = sanitize_text(cols[4].get_text())
        draw_str = sanitize_text(cols[7].get_text())

        if placing is None or not date_str or not distance_str.isdigit() or not draw_str.isdigit():
            continue

        race_date = parse_date(date_str)
        if not race_date:
            continue

        distance = int(distance_str)
        draw = int(draw_str)

        # Calculate season code
        if race_date.month >= 9:
            season_code = f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}"
        else:
            season_code = f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"

        distance = int(distance_str)
        draw = int(draw_str)

        # Parse RaceCourse and CourseType
        if "AWT" in raw_course:
            race_course = "ST"
            course_type = "AWT"
        else:
            parts = raw_course.split("/")
            race_course = parts[0].strip() if len(parts) > 0 else "Unknown"
            course_type = parts[2].strip() if len(parts) > 2 else "Turf"

        if not race_course:
            continue

        # Get field_size from row attributes or use default
        field_size = row.attrs.get("field_size", 12)
        try:
            field_size = int(field_size)
        except (ValueError, TypeError):
            field_size = 12  # Default value if conversion fails

        distance_group = get_distance_group(race_course, course_type, distance)
        draw_group = get_draw_group(draw, field_size)

        key = (race_course, distance_group, draw_group)
        stats = draw_pref[season_code][key]
        stats["TotalRuns"] += 1
        if placing in [1, 2, 3]:
            stats["Top3Count"] += 1

    return draw_pref

def upsert_course_pref(horse_id, course_pref):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS horse_course_pref (
            HorseID TEXT,
            Season TEXT,
            RaceCourse TEXT,
            CourseType TEXT,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, RaceCourse, CourseType)
        )
    ''')

    # ✅ Add these 3 lines right here to patch older DBs
    ensure_column_exists("hkjc_horses_dynamic_special.db", "horse_course_pref", "Top3Count", "INTEGER")
    ensure_column_exists("hkjc_horses_dynamic_special.db", "horse_course_pref", "TotalRuns", "INTEGER")
    ensure_column_exists("hkjc_horses_dynamic_special.db", "horse_course_pref", "LastUpdate", "TEXT")

    for season, courses in course_pref.items():
        for (race_course, course_type), values in courses.items():
            course_type = clean_course_type_text(course_type)
            last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

            cursor.execute('''
                INSERT OR REPLACE INTO horse_course_pref (
                    HorseID, Season, RaceCourse, CourseType,
                    Top3Rate, Top3Count, TotalRuns, LastUpdate
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                horse_id, season, race_course, course_type,
                values["Top3Rate"], values["Top3Count"],
                values["TotalRuns"], last_update
            ))

    conn.commit()
    conn.close()

def upsert_class_jump_pref(horse_id, jump_stats):
    """
    Upsert per-season class jump stats into horse_class_jump_pref.
    Applies small-sample adjustment: if TotalRuns < 3 and Top3Count > 0, Top3Rate *= 0.5
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Ensure table exists
    create_class_jump_pref_table()

    # Ensure columns exist (for older DBs)
    ensure_column_exists(DB_PATH, "horse_class_jump_pref", "Top3Count", "INTEGER")
    ensure_column_exists(DB_PATH, "horse_class_jump_pref", "TotalRuns", "INTEGER")
    ensure_column_exists(DB_PATH, "horse_class_jump_pref", "LastUpdate", "TEXT")

    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")

    # Process seasons in sorted order (newest first)
    for season in sorted(jump_stats.keys(), key=lambda s: int(s[:2]), reverse=True):
        per_jump = jump_stats[season]
        for jump_type, vals in per_jump.items():
            top3 = int(vals.get("Top3Count", 0))
            total = int(vals.get("TotalRuns", 0))
            if total <= 0:
                rate = 0.0
            else:
                rate = top3 / total
                if total < 3 and top3 > 0:
                    rate *= 0.5
            rate = round(rate, 4)

            cursor.execute("""
                INSERT INTO horse_class_jump_pref (
                    HorseID, Season, JumpType,
                    Top3Rate, Top3Count, TotalRuns, LastUpdate
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(HorseID, Season, JumpType)
                DO UPDATE SET
                    Top3Rate = excluded.Top3Rate,
                    Top3Count = excluded.Top3Count,
                    TotalRuns = excluded.TotalRuns,
                    LastUpdate = excluded.LastUpdate
            """, (horse_id, season, jump_type, rate, top3, total, last_update))

    conn.commit()
    conn.close()

def fetch_class_jump_pref_ordered(horse_id):
    """Fetch class jump pref ordered by season (newest first) with dynamic season handling"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get current season (automatically handles future seasons)
    current_year = datetime.now().year
    current_season = f"{current_year%100:02d}/{(current_year+1)%100:02d}"
    
    cursor.execute(f"""
        SELECT Season, JumpType, Top3Rate, Top3Count, TotalRuns
        FROM horse_class_jump_pref
        WHERE HorseID = ?
        ORDER BY
            CASE
                WHEN Season = ? THEN 0  -- Current season first
                ELSE 99 - CAST(SUBSTR(Season, 1, 2) AS INTEGER  -- Older seasons sorted by recency
            END
    """, (horse_id, current_season))
    
    results = cursor.fetchall()
    conn.close()
    
    return results

def fetch_running_style_pref_ordered(horse_id):
    """
    Return horse_running_style_pref rows for a horse with Season sorted newest→oldest.
    Also applies a sensible ordering for StyleBucket.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Get current season for proper sorting
    current_year = datetime.now().year
    current_season = f"{current_year%100:02d}/{(current_year+1)%100:02d}"
    
    cur.execute("""
        SELECT
            HorseID, Season, RaceCourse, DistanceGroup, TurnCount,
            StyleBucket, Top3Rate, Top3Count, TotalRuns, LastUpdate
        FROM horse_running_style_pref
        WHERE HorseID = ?
        ORDER BY
            CAST(SUBSTR(Season, 1, 2) AS INTEGER) DESC,      -- 24/25 before 23/24
            RaceCourse,
            DistanceGroup,
            TurnCount DESC,
            CASE StyleBucket
                WHEN 'Leader'  THEN 1
                WHEN 'On-pace' THEN 2
                WHEN 'Stalker' THEN 3
                WHEN 'Closer'  THEN 4
                ELSE 99
            END
    """, (horse_id,))
    rows = cur.fetchall()
    conn.close()
    return rows
    
def fetch_draw_pref_ordered(horse_id):
    """Fetch draw preference rows for a horse ordered by most recent update."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT HorseID, Season, RaceCourse, DistanceGroup, DrawGroup,
               Top3Rate, Top3Count, TotalRuns, LastUpdate
        FROM horse_draw_pref
        WHERE HorseID = ?
        ORDER BY datetime(LastUpdate) DESC
        """,
        (horse_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def rebuild_running_style_pref(horse_id: str | None = None) -> tuple[int, int]:
    """
    Aggregate per-race running positions into style preference rows.
    Expects horse_running_position to have:
      HorseID, Season, RaceCourse, CourseType, DistanceGroup, TurnCount, FieldSize, EarlyPos, Placing
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    create_running_style_pref_table()

    params = []
    sql = """
        SELECT
            HorseID,
            Season,
            RaceCourse,
            CourseType,
            DistanceGroup,
            TurnCount,
            FieldSize,
            EarlyPos,
            Placing
        FROM horse_running_position
    """
    if horse_id:
        sql += " WHERE HorseID = ?"
        params.append(horse_id)
    
    # Add ORDER BY to ensure seasons are processed newest to oldest
    sql += " ORDER BY CAST(SUBSTR(Season, 1, 2) AS INTEGER) DESC, RaceDate DESC"

    rows = cur.execute(sql, params).fetchall()

    agg = {}  # key -> {"top3": int, "total": int}

    for (hid, season, rc, ctype, dist_grp, turn_cnt, fs, early_pos, placing) in rows:
        if not fs or not early_pos:
            continue

        bucket = _compute_style_bucket(early_pos, fs)
        if not bucket:
            continue

        tc = 0
        try:
            tc = round(float(turn_cnt), 1)
        except Exception as e:
            log("DEBUG", f"Failed to convert turn count '{turn_cnt}': {e}")

        key = (hid, season, rc or "Unknown", ctype or "Unknown",
               dist_grp or "Unknown", tc, bucket)

        rec = agg.get(key, {"top3": 0, "total": 0})
        rec["total"] += 1

        try:
            is_top3 = int(placing) <= 3
        except:
            is_top3 = False
        if is_top3:
            rec["top3"] += 1

        agg[key] = rec

    last_update = datetime.now().strftime("%Y/%m/%d %H:%M")
    upserts = 0
    for key, rec in agg.items():
        top3 = rec["top3"]
        total = rec["total"]
        rate = 0.0
        if total > 0:
            rate = top3 / total
            if total < 3 and top3 > 0:  # your 50% damping rule
                rate *= 0.5

        cur.execute("""
            INSERT OR REPLACE INTO horse_running_style_pref
            (HorseID, Season, RaceCourse, CourseType, DistanceGroup,
             TurnCount, StyleBucket,
             Top3Rate, Top3Count, TotalRuns, LastUpdate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (*key, rate, top3, total, last_update))
        upserts += 1

    conn.commit()
    conn.close()
    return upserts, len(agg)

if __name__ == "__main__":
    print("\n[INFO] This module provides helper functions for processing HKJC horse data.")
    print("       It's designed to be imported, not run directly.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE horse_running_position
        SET RaceID = (
            substr(RaceDate, 1, 4) || substr(RaceDate, 6, 2) || substr(RaceDate, 9, 2) || 
            '_' || RaceCourse || '_' || printf('%02d', RaceNo)
        )
        WHERE RaceID IS NULL
    """)
    conn.commit()
    conn.close()
    print("[MIGRATION] Fixed NULL RaceIDs in existing data")

    create_running_position_table()