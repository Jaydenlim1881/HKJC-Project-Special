import re
import unicodedata
from datetime import datetime
import inspect

from pathlib import Path

# Project root = one level up from /special
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Main dynamic DB path (adjust filename if yours is different)
DB_PATH = str(PROJECT_ROOT / "hkjc_horses_dynamic_special.db")

# --- HKJC date parsing helpers ---
import re
from datetime import datetime as _dt

_DATE_PATTERNS = [
    "%d/%m/%Y",  # 28/06/2023
    "%d-%m-%Y",  # 28-06-2023
    "%Y-%m-%d",  # 2023-06-28
    "%d.%m.%Y",  # 28.06.2023
    "%d/%m/%y",  # 28/06/23
]

def parse_hkjc_date(raw):
    """Return a date (datetime.date) or None. Cleans stray unicode and supports multiple formats."""
    if not raw:
        return None
    s = str(raw).strip()
    # strip hidden LTR/RTL marks & non-digits/sep
    s = s.replace("\u200f", "").replace("\u200e", "")
    s = re.sub(r"[^\d/.\-]", "", s)

    for fmt in _DATE_PATTERNS:
        try:
            return _dt.strptime(s, fmt).date()
        except Exception:
            pass

    # final regex fallback: dd sep mm sep yyyy or yy
    m = re.search(r"(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})", s)
    if m:
        d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:  # assume 20xx
            y += 2000
        try:
            return _dt(y, mth, d).date()
        except Exception:
            return None
    return None

def log(level, *args, **kwargs):
    """Simple logging helper with module-level debug control.

    Uses the caller's ``DEBUG_LEVEL`` variable if present (defaults to
    ``"INFO"``). Levels: ``OFF`` < ``INFO`` < ``DEBUG`` < ``TRACE``.
    """
    levels = {"OFF": 0, "INFO": 1, "DEBUG": 2, "TRACE": 3}
    caller_frame = inspect.currentframe().f_back
    debug_level = caller_frame.f_globals.get("DEBUG_LEVEL", "INFO")
    current_level = levels.get(debug_level, 1)
    msg_level = levels.get(level, 0)
    if msg_level <= current_level:
        print(f"[{level}]", *args, **kwargs)

def sanitize_text(text):
    if not text:
        return ""
    try:
        text = str(text)
        text = re.sub(r'[^\x00-\x7F]+', '', text)
        return text.strip()
    except:
        return ""

def clean_placing(placing_text):
    clean_text = sanitize_text(placing_text)
    digits_only = re.sub(r'[^\d]', '', clean_text)
    return int(digits_only) if digits_only.isdigit() and int(digits_only) > 0 else None

def convert_finish_time(time_str):
    if not time_str:
        return None
    try:
        time_str = time_str.strip().replace(":", ".")
        parts = time_str.split(".")
        if len(parts) == 3:
            mins, secs, hundredths = parts
            return round(int(mins) * 60 + int(secs) + int(hundredths) / 100, 2)
        elif len(parts) == 2:
            secs, hundredths = parts
            return round(int(secs) + int(hundredths) / 100, 2)
        else:
            return None
    except:
        return None

def safe_int(value):
    try:
        return int(value)
    except:
        return None

def safe_float(value):
    try:
        return float(value)
    except:
        return None

def parse_weight(weight_str):
    try:
        return int(weight_str.replace("lb", "").strip())
    except:
        return None

def parse_lbw(lbw_str, placing):
    lbw_str = sanitize_text(lbw_str)
    if placing == 1:
        return 0.0
    try:
        return float(lbw_str)
    except:
        return None

def get_season_code(date_obj):
    if date_obj.month >= 9:
        return f"{date_obj.year%100:02d}/{(date_obj.year+1)%100:02d}"
    else:
        return f"{(date_obj.year-1)%100:02d}/{date_obj.year%100:02d}"

def get_distance_group(race_course, course_type, distance):
    course_type = course_type.upper()
    race_course = race_course.upper()

    if race_course == "ST":
        if course_type == "AWT":
            if distance <= 1000:
                return "Sprint"
            if distance <= 1200:
                return "Short"
            elif distance <= 1400:
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

def get_distance_group_from_row(course_info, distance_str):
    try:
        course_info = sanitize_text(course_info)
        if "AWT" in course_info:
            race_course = "ST"
            course_type = "AWT"
        else:
            parts = course_info.split("/")
            race_course = parts[0].strip() if len(parts) > 0 else "Unknown"
            course_type = parts[2].strip() if len(parts) > 2 else "Turf"
        return get_distance_group(race_course, course_type, int(distance_str))
    except:
        return "Unknown"

# --- Turn geometry (CountTurn) helpers ---------------------------------------

def _norm_course(course: str) -> str:
    """Normalize race course to canonical short code: 'ST' or 'HV'."""
    t = (course or "").strip().upper()
    if t in {"ST", "SHA TIN"} or "SHA TIN" in t:
        return "ST"
    if t in {"HV", "HAPPY VALLEY"} or "HAPPY VALLEY" in t:
        return "HV"
    return t  # leave unknowns as-is

def _norm_surface(surface: str) -> str:
    """Normalize surface to canonical: 'TURF' or 'AWT'."""
    t = (surface or "").strip().upper()
    if t in {"TURF", "T"}:
        return "TURF"
    if t in {"AWT", "ALL WEATHER", "ALL-WEATHER", "ALL WEATHER TRACK", "DIRT"}:
        return "AWT"
    return t  # leave unknowns as-is

# Exact mapping per your specification
_TURN_COUNT_MAP = {
    ("ST", "TURF"): {
        1000: 0.0,
        1200: 1.0, 1400: 1.0, 1600: 1.0, 1800: 1.0,
        2000: 2.0, 2200: 2.0, 2400: 2.0,
    },
    ("ST", "AWT"): {
        1200: 1.0,
        1650: 2.0, 1800: 2.0, 2000: 2.0,
        2400: 3.0,
    },
    ("HV", "TURF"): {
        1000: 1.0,
        1200: 1.5,
        1650: 2.5, 1800: 2.5,
        2200: 3.5, 2400: 3.5,
    },
}

def get_turn_count(race_course: str, surface: str, distance: int | str):
    """Return CountTurn as float; None if unmapped."""
    try:
        d = int(str(distance).strip())
    except Exception:
        return None
    c = _norm_course(race_course)
    s = _norm_surface(surface)
    if c == "HV":
        s = "TURF"
    m = _TURN_COUNT_MAP.get((c, s))
    if not m:
        return None
    return m.get(d)

def is_straight(turn_count):
    return turn_count == 0.0

def is_fractional_turn(turn_count):
    return (turn_count is not None) and (float(turn_count) % 1.0 != 0.0)

def is_one_turn_exact(turn_count):
    return turn_count == 1.0

def get_draw_group(draw_number, field_size=None):
    """
    Map barrier draw to fixed groups (field_size ignored intentionally):
      Inside   = 1–3
      InnerMid = 4–6
      OuterMid = 7–9
      Wide     = 10–12
      Outer    = 13+
    Returns one of: "Inside", "InnerMid", "OuterMid", "Wide", "Outer" or None.
    """
    # Accept strings like " 9 " and handle None/"-" etc.
    if draw_number is None:
        return None
    try:
        d = int(str(draw_number).strip())
    except (ValueError, TypeError):
        return None

    if 1 <= d <= 3:
        return "Inside"
    if 4 <= d <= 6:
        return "InnerMid"
    if 7 <= d <= 9:
        return "OuterMid"
    if 10 <= d <= 12:
        return "Wide"
    if d >= 13:
        return "Outer"
    return None

def get_jump_type(previous_class, current_class):
    try:
        prev = int(previous_class)
        curr = int(current_class)
        if curr < prev:
            return "UP"
        elif curr > prev:
            return "DOWN"
        else:
            return "SAME"
    except:
        return "UNKNOWN"

# Explicit exports
__all__ = [
    "DB_PATH",
    "log",
    "sanitize_text",
    "clean_placing",
    "convert_finish_time",
    "safe_int",
    "safe_float",
    "parse_weight",
    "parse_lbw",
    "get_season_code",
    "get_distance_group",
    "get_distance_group_from_row",
    "get_turn_count",
    "is_straight",
    "is_fractional_turn",
    "is_one_turn_exact",
    "get_draw_group",
    "get_jump_type",
    "parse_hkjc_date"
]

if __name__ == "__main__":
    print(parse_hkjc_date("28/06/2023"))