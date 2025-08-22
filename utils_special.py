import re
from datetime import datetime
import ftfy

def log(level, message):
    if level == "ERROR":
        print(f"[ERROR] {message}")
    elif level == "WARNING":
        print(f"[WARNING] {message}")
    elif level == "INFO":
        print(f"[INFO] {message}")
    elif level == "DEBUG":
        print(f"[DEBUG] {message}")
    elif level == "TRACE":
        print(f"[TRACE] {message}")

def sanitize_text(text):
    if not text:
        return ""
    text = str(text)
    text = ftfy.fix_text(text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text

def clean_placing(placing_text):
    if not placing_text:
        return None
    placing_text = sanitize_text(placing_text)
    placing_clean = re.sub(r'[^\d]', '', placing_text)
    try:
        return int(placing_clean) if placing_clean else None
    except ValueError:
        return None

def convert_finish_time(time_str):
    if not time_str:
        return None
    time_str = sanitize_text(time_str)
    parts = time_str.split('.')
    try:
        if len(parts) == 3:
            mins, secs, hundredths = parts
            return int(mins) * 60 + int(secs) + int(hundredths) / 100
        elif len(parts) == 2:
            secs, hundredths = parts
            return int(secs) + int(hundredths) / 100
        else:
            return None
    except ValueError:
        return None

def safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def parse_weight(weight_str):
    if not weight_str:
        return None
    weight_str = sanitize_text(weight_str)
    match = re.search(r'(\d+)', weight_str)
    if match:
        return int(match.group(1))
    return None

def parse_lbw(lbw_str):
    if not lbw_str:
        return 0.0
    lbw_str = sanitize_text(lbw_str)
    if lbw_str.upper() in ['DH', 'DHD', 'DIST', 'NSE']:
        return 0.0
    match = re.search(r'(\d+\.?\d*)', lbw_str)
    if match:
        return float(match.group(1))
    return 0.0

def get_distance_group(race_course, course_type, distance):
    if race_course == "ST":
        if course_type == "AWT":
            if distance <= 1200:
                return "Short"
            elif distance <= 1650:
                return "Mid"
            elif distance <= 2000:
                return "Long"
            else:
                return "Endurance"
        else:
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
    elif race_course == "HV":
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

def get_turn_count(race_course, surface, distance):
    if race_course == "ST":
        if surface == "AWT":
            if distance <= 1200:
                return 0.0
            elif distance <= 1650:
                return 1.0
            elif distance <= 2000:
                return 2.0
        else:
            if distance <= 1000:
                return 0.0
            elif distance <= 1400:
                return 1.0
            elif distance <= 1800:
                return 2.0
            elif distance <= 2200:
                return 3.0
    elif race_course == "HV":
        if distance <= 1000:
            return 1.0
        elif distance <= 1200:
            return 2.0
        elif distance <= 1800:
            return 3.0
    return 0.0

def get_draw_group(draw, field_size):
    if field_size <= 0:
        return "Unknown"
    if draw <= field_size * 0.33:
        return "Low"
    elif draw <= field_size * 0.66:
        return "Middle"
    else:
        return "High"

def get_jump_type(prev_class, curr_class):
    if prev_class is None or curr_class is None:
        return "Same"
    if curr_class < prev_class:
        return "Up"
    elif curr_class > prev_class:
        return "Down"
    else:
        return "Same"

def get_distance_group_from_row(course_info, distance_str):
    course_info = sanitize_text(course_info)
    distance = safe_int(distance_str)
    
    if "AWT" in course_info:
        race_course = "ST"
        course_type = "AWT"
    else:
        parts = course_info.split("/")
        race_course = parts[0].strip() if len(parts) > 0 else "ST"
        course_type = parts[2].strip() if len(parts) > 2 else "Turf"
    
    return get_distance_group(race_course, course_type, distance)

def get_season_code(race_date):
    if not race_date:
        return "Unknown"
    if isinstance(race_date, str):
        try:
            race_date = datetime.strptime(race_date, "%d/%m/%y")
        except ValueError:
            return "Unknown"
    
    if race_date.month >= 9:
        return f"{race_date.year%100:02d}/{(race_date.year+1)%100:02d}"
    else:
        return f"{(race_date.year-1)%100:02d}/{race_date.year%100:02d}"